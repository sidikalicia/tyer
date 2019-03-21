use futures::sync::mpsc::{channel, Receiver, Sender};
use graph::data::subgraph::schema::SubgraphDeploymentEntity;
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use super::SubgraphInstance;
use crate::elastic_logger;
use crate::split_logger;
use crate::ElasticDrainConfig;
use crate::ElasticLoggingConfig;

type InstanceShutdownMap = Arc<RwLock<HashMap<SubgraphDeploymentId, CancelGuard>>>;

pub struct SubgraphInstanceManager {
    logger: Logger,
    input: Sender<SubgraphAssignmentProviderEvent>,
}

impl SubgraphInstanceManager {
    /// Creates a new runtime manager.
    pub fn new<B, S, T>(
        logger: &Logger,
        store: Arc<S>,
        host_builder: T,
        block_stream_builder: B,
        elastic_config: Option<ElasticLoggingConfig>,
    ) -> Self
    where
        S: Store + ChainStore,
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder + 'static,
    {
        let logger = logger.new(o!("component" => "SubgraphInstanceManager"));

        // Create channel for receiving subgraph provider events.
        let (subgraph_sender, subgraph_receiver) = channel(100);

        // Handle incoming events from the subgraph provider.
        Self::handle_subgraph_events(
            logger.clone(),
            subgraph_receiver,
            store,
            host_builder,
            block_stream_builder,
            elastic_config,
        );

        SubgraphInstanceManager {
            logger,
            input: subgraph_sender,
        }
    }

    /// Handle incoming events from subgraph providers.
    fn handle_subgraph_events<B, S, T>(
        logger: Logger,
        receiver: Receiver<SubgraphAssignmentProviderEvent>,
        store: Arc<S>,
        host_builder: T,
        block_stream_builder: B,
        elastic_config: Option<ElasticLoggingConfig>,
    ) where
        S: Store + ChainStore,
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder + 'static,
    {
        // Subgraph instance shutdown senders
        let instances: InstanceShutdownMap = Default::default();

        tokio::spawn(receiver.for_each(move |event| {
            use self::SubgraphAssignmentProviderEvent::*;

            match event {
                SubgraphStart(manifest) => {
                    // Write subgraph logs to the terminal and, if enabled, Elasticsearch
                    let term_logger = logger.new(o!("subgraph_id" => manifest.id.to_string()));
                    let logger = elastic_config
                        .clone()
                        .map(|elastic_config| {
                            split_logger(
                                term_logger.clone(),
                                elastic_logger(
                                    ElasticDrainConfig {
                                        general: elastic_config,
                                        index: String::from("subgraph-logs"),
                                        document_type: String::from("log"),
                                        subgraph_id: manifest.id.clone(),
                                        flush_interval: Duration::from_secs(5),
                                    },
                                    term_logger.clone(),
                                ),
                            )
                        })
                        .unwrap_or(term_logger);

                    info!(logger, "Start subgraph");

                    Self::start_subgraph(
                        logger.clone(),
                        instances.clone(),
                        host_builder.clone(),
                        block_stream_builder.clone(),
                        store.clone(),
                        manifest,
                    )
                    .map_err(|err| error!(logger, "Failed to start subgraph: {}", err))
                    .ok();
                }
                SubgraphStop(id) => {
                    info!(logger, "Stopping subgraph"; "subgraph_id" => id.to_string());
                    Self::stop_subgraph(instances.clone(), id);
                }
            };

            Ok(())
        }));
    }

    fn start_subgraph<B, T, S>(
        logger: Logger,
        instances: InstanceShutdownMap,
        host_builder: T,
        block_stream_builder: B,
        store: Arc<S>,
        manifest: SubgraphManifest,
    ) -> Result<(), Error>
    where
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder,
        S: Store + ChainStore,
    {
        let id = manifest.id.clone();
        let id_for_block = manifest.id.clone();
        let id_for_err = manifest.id.clone();
        let store_for_events = store.clone();
        let store_for_errors = store.clone();

        // Create copies of the data source templates; this creates a vector of
        // the form
        // ```
        // vec![
        //   ("DataSource1", "Template1", <template struct>),
        //   ("DataSource2", "Template1", <template struct>),
        //   ("DataSource2", "Template2", <template struct>),
        // ]
        // ```
        // for easy filtering later
        let mut templates: Vec<(String, DataSourceTemplate)> = vec![];
        for data_source in manifest.data_sources.iter() {
            for template in data_source.templates.iter().flatten() {
                templates.push((data_source.name.clone(), template.expensive_clone()));
            }
        }
        let templates = Arc::new(templates);

        // TODO: Restore dynamic data sources
        let dynamic_data_sources = Arc::new(RwLock::new(vec![]));

        // Request a block stream for this subgraph
        let block_stream_canceler = CancelGuard::new();
        let block_stream_cancel_handle = block_stream_canceler.handle();
        let block_stream = block_stream_builder
            .from_subgraph(&manifest, logger.clone())
            .from_err()
            .cancelable(&block_stream_canceler, || CancelableError::Cancel);

        // Load the subgraph
        let instance = Arc::new(SubgraphInstance::from_manifest(
            &logger,
            manifest,
            host_builder,
        )?);

        // Prepare loggers for different parts of the async processing
        let block_logger = logger.clone();
        let error_logger = logger.clone();

        // Clear the 'failed' state of the subgraph. We were told explicitly
        // to start, which implies we assume the subgraph has not failed (yet)
        // If we can't even clear the 'failed' flag, don't try to start
        // the subgraph.
        let status_ops = SubgraphDeploymentEntity::update_failed_operations(&id_for_err, false);
        store_for_errors.apply_entity_operations(status_ops, EventSource::None)?;

        // Forward block stream events to the subgraph for processing
        tokio::spawn(
            block_stream
                .for_each(move |block| {
                    let id = id_for_block.clone();
                    let instance = instance.clone();
                    let store = store_for_events.clone();
                    let block_stream_cancel_handle = block_stream_cancel_handle.clone();
                    let logger = block_logger.new(o!(
                        "block_number" => format!("{:?}", block.block.number.unwrap()),
                        "block_hash" => format!("{:?}", block.block.hash.unwrap())
                    ));
                    let templates = templates.clone();
                    let dynamic_data_sources = dynamic_data_sources.clone();

                    // Extract logs relevant to the subgraph
                    let logs: Vec<_> = block
                        .transaction_receipts
                        .iter()
                        .flat_map(|receipt| {
                            receipt.logs.iter().filter(|log| instance.matches_log(&log))
                        })
                        .cloned()
                        .collect();

                    if logs.len() == 0 {
                        debug!(logger, "No events found in this block for this subgraph");
                    } else if logs.len() == 1 {
                        info!(logger, "1 event found in this block for this subgraph");
                    } else {
                        info!(
                            logger,
                            "{} events found in this block for this subgraph",
                            logs.len()
                        );
                    }

                    // Process events one after the other, passing in entity operations
                    // collected previously to every new event being processed
                    let block_for_process = Arc::new(block);
                    let block_for_transact = block_for_process.clone();
                    let logger_for_process = logger;
                    let logger_for_transact = logger_for_process.clone();
                    let initial_state = ProcessingState::default();
                    stream::iter_ok::<_, CancelableError<Error>>(logs)
                        // Process events from the block stream
                        .fold(initial_state, move |state, log| {
                            let logger = logger_for_process.clone();
                            let instance = instance.clone();
                            let block = block_for_process.clone();

                            let transaction = block
                                .transaction_for_log(&log)
                                .map(Arc::new)
                                .ok_or_else(|| format_err!("Found no transaction for event"));

                            future::result(transaction).and_then(move |transaction| {
                                instance
                                    .process_log(&logger, block, transaction, log, state)
                                    .map_err(|e| format_err!("Failed to process event: {}", e))
                            })
                        })
                        // Create dynamic data sources and process their events as well
                        .and_then(move |state| {
                            // Create data source instances from templates
                            let data_sources = state.created_data_sources.iter().try_fold(
                                vec![],
                                move |mut data_sources, info| {
                                    let template = &templates
                                        .iter()
                                        .find(|(data_source_name, template)| {
                                            data_source_name == &info.data_source
                                                && template.name == info.template
                                        })
                                        .ok_or_else(|| {
                                            format_err!(
                                                "Failed to create data source with name `{}`. \
                                                 No template with this name in parent data \
                                                 source `{}`.",
                                                info.template,
                                                info.data_source
                                            )
                                        })?
                                        .1;

                                    let data_source =
                                        DataSource::try_from_template(&template, &info.params)?;
                                    data_sources.push(data_source);
                                    Ok(data_sources)
                                },
                            );

                            // Fail early if any of the data sources couln't be created (e.g.
                            // due to parameters missing)
                            let data_sources = match data_sources {
                                Ok(data_sources) => data_sources,
                                Err(e) => return future::err(e),
                            };

                            // TODO: Ask the block stream for events for the new data sources
                            // that come from the same block; then process these events

                            // Update the block stream so it includes events from these
                            // data sources going forward

                            // Track the new data sources
                            let mut dynamic_data_sources = dynamic_data_sources.write().unwrap();
                            dynamic_data_sources.extend(data_sources);

                            // TODO
                            // For each new data source:
                            // 2. Request and process events for the current block
                            // 3. Update block stream going forward
                            // 4. Create entity operations for the new data source

                            future::ok(state)
                        })
                        // Apply entity operations and advance the stream
                        .and_then(move |state| {
                            let block = block_for_transact.clone();
                            let logger = logger_for_transact.clone();

                            let block_ptr_now = EthereumBlockPointer::to_parent(&block);
                            let block_ptr_after = EthereumBlockPointer::from(&*block);

                            // Avoid writing to store if block stream has been canceled
                            if block_stream_cancel_handle.is_canceled() {
                                return Err(CancelableError::Cancel);
                            }

                            info!(
                                logger,
                                "Applying {} entity operation(s)",
                                state.entity_operations.len()
                            );

                            // Transact entity operations into the store and update the
                            // subgraph's block stream pointer
                            store
                                .transact_block_operations(
                                    id.clone(),
                                    block_ptr_now,
                                    block_ptr_after,
                                    state.entity_operations,
                                )
                                .map_err(|e| {
                                    format_err!(
                                        "Error while processing block stream for a subgraph: {}",
                                        e
                                    )
                                    .into()
                                })
                        })
                })
                .map_err(move |e| match e {
                    CancelableError::Cancel => {
                        debug!(
                            error_logger,
                            "Subgraph block stream shut down cleanly";
                            "id" => id_for_err.to_string()
                        );
                    }
                    CancelableError::Error(e) => {
                        error!(
                            error_logger,
                            "Subgraph instance failed to run: {}", e;
                            "id" => id_for_err.to_string()
                        );

                        // Set subgraph status to Failed
                        let status_ops =
                            SubgraphDeploymentEntity::update_failed_operations(&id_for_err, true);
                        if let Err(e) =
                            store_for_errors.apply_entity_operations(status_ops, EventSource::None)
                        {
                            error!(
                                error_logger,
                                "Failed to set subgraph status to Failed: {}", e;
                                "id" => id_for_err.to_string()
                            );
                        }
                    }
                }),
        );

        // Keep the cancel guard for shutting down the subgraph instance later
        instances.write().unwrap().insert(id, block_stream_canceler);
        Ok(())
    }

    fn stop_subgraph(instances: InstanceShutdownMap, id: SubgraphDeploymentId) {
        // Drop the cancel guard to shut down the subgraph now
        let mut instances = instances.write().unwrap();
        instances.remove(&id);
    }
}

impl EventConsumer<SubgraphAssignmentProviderEvent> for SubgraphInstanceManager {
    /// Get the wrapped event sink.
    fn event_sink(
        &self,
    ) -> Box<Sink<SinkItem = SubgraphAssignmentProviderEvent, SinkError = ()> + Send> {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "Component was dropped: {}", e);
        }))
    }
}
