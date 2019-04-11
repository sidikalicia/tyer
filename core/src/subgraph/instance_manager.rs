use futures::future::{loop_fn, Loop};
use futures::sync::mpsc::{channel, Receiver, Sender};
use graph::data::subgraph::schema::{
    DynamicEthereumContractDataSourceEntity, SubgraphDeploymentEntity,
};
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use std::time::Duration;
use uuid::Uuid;

use super::SubgraphInstance;
use crate::elastic_logger;
use crate::split_logger;
use crate::ElasticDrainConfig;
use crate::ElasticLoggingConfig;

struct SubgraphState<B, S, T>
where
    B: BlockStreamBuilder,
    S: Store + ChainStore,
    T: RuntimeHostBuilder,
{
    pub deployment_id: SubgraphDeploymentId,
    pub instance: Arc<RwLock<SubgraphInstance<T>>>,
    pub logger: Logger,
    pub templates: Vec<(String, DataSourceTemplate)>,
    pub store: Arc<S>,
    pub stream_builder: B,
    pub restarts: u64,
}

type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<SubgraphDeploymentId, CancelGuard>>>;

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
        let instances: SharedInstanceKeepAliveMap = Default::default();

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

                    info!(
                        logger,
                        "Start subgraph";
                        "data_sources" => manifest.data_sources.len()
                    );

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
        instances: SharedInstanceKeepAliveMap,
        host_builder: T,
        stream_builder: B,
        store: Arc<S>,
        manifest: SubgraphManifest,
    ) -> Result<(), Error>
    where
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder + 'static,
        S: Store + ChainStore,
    {
        // Clear the 'failed' state of the subgraph. We were told explicitly
        // to start, which implies we assume the subgraph has not failed (yet)
        // If we can't even clear the 'failed' flag, don't try to start
        // the subgraph.
        let status_ops = SubgraphDeploymentEntity::update_failed_operations(&manifest.id, false);
        store.apply_entity_operations(status_ops, EventSource::None)?;

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

        // Load the subgraph
        let deployment_id = manifest.id.clone();
        let instance = Arc::new(RwLock::new(SubgraphInstance::from_manifest(
            &logger,
            manifest,
            host_builder,
        )?));

        // Define the initial subgraph state
        let subgraph_state = SubgraphState {
            deployment_id,
            instance,
            templates,
            store,
            stream_builder,
            restarts: 0,
            logger,
        };

        // Keep restarting the subgraph until it terminates. The subgraph
        // will usually only run once, but if there are dynamic data sources
        // involved, it will be restarted after every block to include
        // blocks for the new data sources. We decided that this would be
        // easier than updating the subgraph's block stream while it is
        // already running.
        tokio::spawn(loop_fn(subgraph_state, move |subgraph_state| {
            let instances = instances.clone();

            // FIXME: This long delay here is a hack to not run into "too many clients"
            // issues due to several instances of the subgraph still running and
            // referencing their chain head listener; we need to find a more robust
            // solution
            run_subgraph(
                subgraph_state
                    .logger
                    .new(o!("restarts" => subgraph_state.restarts)),
                subgraph_state,
                instances.clone(),
            )
        }));

        Ok(())
    }

    fn stop_subgraph(instances: SharedInstanceKeepAliveMap, id: SubgraphDeploymentId) {
        // Drop the cancel guard to shut down the sujbgraph now
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

fn run_subgraph<B, S, T>(
    logger: Logger,
    subgraph_state: SubgraphState<B, S, T>,
    instances: SharedInstanceKeepAliveMap,
) -> impl Future<Item = Loop<(), SubgraphState<B, S, T>>, Error = ()>
where
    B: BlockStreamBuilder,
    S: Store + ChainStore,
    T: RuntimeHostBuilder,
{
    debug!(logger, "Starting or restarting subgraph");

    // Clone a few things for different parts of the async processing
    let id_for_err = subgraph_state.deployment_id.clone();
    let store_for_err = subgraph_state.store.clone();
    let logger_for_err = logger.clone();
    let logger_for_data_sources_check = logger.clone();
    let logger_for_stream = logger.clone();

    // Obtain the subgraph instance from the state
    let subgraph_instance = subgraph_state.instance.clone();

    let block_stream_canceler = CancelGuard::new();
    let block_stream_cancel_handle = block_stream_canceler.handle();
    let block_stream = subgraph_state
        .stream_builder
        .build(
            logger.clone(),
            subgraph_state.deployment_id.clone(),
            subgraph_instance.read().unwrap().ethereum_log_filter(),
        )
        .from_err()
        .cancelable(&block_stream_canceler, || CancelableError::Cancel);

    // Keep the cancel guard for shutting down the subgraph instance later
    instances
        .write()
        .unwrap()
        .insert(subgraph_state.deployment_id.clone(), block_stream_canceler);

    let instances = instances.clone();

    // Flag that indicates when to restart the subgraph's block stream
    let restart = Arc::new(AtomicBool::new(false));
    let restart_for_check = restart.clone();
    let restart_for_needs_restart = restart.clone();

    debug!(logger_for_stream, "Starting block stream");

    block_stream
        // Take blocks from the stream as long as no dynamic data sources
        // have been created. Once that has happened, we need to restart
        // the stream to include blocks for these data sources as well.
        .take_while(move |_| {
            if restart_for_check.load(Ordering::SeqCst) {
                debug!(
                    logger_for_data_sources_check,
                    "Have new data sources, need to restart",
                );
                Ok(false)
            } else {
                Ok(true)
            }
        })
        .fold(subgraph_state, move |subgraph_state, block| {
            let restart = restart.clone();
            let id = subgraph_state.deployment_id.clone();
            let id_for_data_sources = id.clone();
            let instance = subgraph_state.instance.clone();
            let store = subgraph_state.store.clone();
            let block_stream_cancel_handle = block_stream_cancel_handle.clone();
            let logger = logger_for_stream.new(o!(
                "block_number" => format!("{:?}", block.block.number.unwrap()),
                "block_hash" => format!("{:?}", block.block.hash.unwrap())
            ));

            // Extract logs relevant to the subgraph
            let logs: Vec<_> = block
                .transaction_receipts
                .iter()
                .flat_map(|receipt| {
                    receipt
                        .logs
                        .iter()
                        .filter(|log| instance.read().unwrap().matches_log(&log))
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
            let logger_for_data_sources = logger_for_process.clone();
            let block_state = ProcessingState::default();
            stream::iter_ok::<_, CancelableError<Error>>(logs)
                // Process events from the block stream
                .fold(block_state, move |block_state, log| {
                    let logger = logger_for_process.clone();
                    let instance = instance.clone();
                    let block = block_for_process.clone();

                    let transaction = block
                        .transaction_for_log(&log)
                        .map(Arc::new)
                        .ok_or_else(|| format_err!("Found no transaction for event"));

                    future::result(transaction).and_then(move |transaction| {
                        instance
                            .read()
                            .unwrap()
                            .process_log(&logger, block, transaction, log, block_state)
                            .map(|block_state| block_state)
                            .map_err(|e| format_err!("Failed to process event: {}", e))
                    })
                })
                // Create dynamic data sources and process their events as well
                .and_then(move |mut block_state| {
                    // Create data source instances from templates
                    let data_sources = block_state.created_data_sources.iter().try_fold(
                        vec![],
                        |mut data_sources, info| {
                            let template = &subgraph_state
                                .templates
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

                    // Fail early if any of the data sources couldn't be created (e.g.
                    // due to parameters missing)
                    let data_sources = match data_sources {
                        Ok(data_sources) => {
                            if data_sources.is_empty() {
                                None
                            } else {
                                Some(data_sources)
                            }
                        }
                        Err(e) => return future::err(e),
                    };

                    if let Some(data_sources) = data_sources {
                        debug!(
                            logger_for_data_sources,
                            "Creating {} dynamic data source(s)",
                            data_sources.len()
                        );

                        // Add entity operations to the block state in order to persist
                        // the dynamic data sources
                        for data_source in data_sources.iter() {
                            let entity = DynamicEthereumContractDataSourceEntity::from((
                                &id_for_data_sources,
                                data_source,
                            ));
                            let id = format!("{}-dynamic", Uuid::new_v4().to_simple());
                            let operations = entity.write_operations(id.as_ref());
                            block_state.entity_operations.extend(operations);
                        }

                        // Try to add the new data sources to the subgraph;
                        // fail if they can't be added
                        match subgraph_state
                            .instance
                            .write()
                            .unwrap()
                            .add_dynamic_data_sources(data_sources)
                        {
                            Ok(_) => {}
                            Err(e) => return future::err(e.into()),
                        }

                        // If the data sources were added correctly, mark the subgraph
                        // for restarting after this block
                        restart.swap(true, Ordering::SeqCst);
                    }

                    // TODO:
                    // 1. Request and process events for the current block

                    future::ok((subgraph_state, block_state))
                })
                // Apply entity operations and advance the stream
                .and_then(move |(subgraph_state, block_state)| {
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
                        block_state.entity_operations.len()
                    );

                    // Transact entity operations into the store and update the
                    // subgraph's block stream pointer
                    store
                        .transact_block_operations(
                            id.clone(),
                            block_ptr_now,
                            block_ptr_after,
                            block_state.entity_operations,
                        )
                        .map(|_| subgraph_state)
                        .map_err(|e| {
                            format_err!("Error while processing block stream for a subgraph: {}", e)
                                .into()
                        })
                })
        })
        .and_then(move |mut subgraph_state| {
            // Reset restart flag
            restart_for_needs_restart.swap(true, Ordering::SeqCst);

            subgraph_state.restarts += 1;

            // Cancel the stream for real
            instances
                .write()
                .unwrap()
                .remove(&subgraph_state.deployment_id);

            // And restart the subgraph
            Ok(Loop::Continue(subgraph_state))
        })
        .map_err(move |e| match e {
            CancelableError::Cancel => {
                debug!(
                    logger_for_err,
                    "Subgraph block stream shut down cleanly";
                    "id" => id_for_err.to_string()
                );
            }
            CancelableError::Error(e) => {
                error!(
                    logger_for_err,
                    "Subgraph instance failed to run: {}", e;
                    "id" => id_for_err.to_string()
                );

                // Set subgraph status to Failed
                let status_ops =
                    SubgraphDeploymentEntity::update_failed_operations(&id_for_err, true);
                if let Err(e) = store_for_err.apply_entity_operations(status_ops, EventSource::None)
                {
                    error!(
                        logger_for_err,
                        "Failed to set subgraph status to Failed: {}", e;
                        "id" => id_for_err.to_string()
                    );
                }
            }
        })
}
