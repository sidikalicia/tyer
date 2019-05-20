use futures::future::{loop_fn, Loop};
use std::sync::Arc;
use std::time::Duration;

use graph::prelude::*;

pub struct NetworkIngestor<S, E>
where
    S: NetworkStore,
    E: EthereumAdapter,
{
    store: Arc<S>,
    eth_adapter: Arc<E>,
    network_name: String,
    logger: Logger,
}

impl<S, E> NetworkIngestor<S, E>
where
    S: NetworkStore,
    E: EthereumAdapter,
{
    pub fn new(
        store: Arc<S>,
        eth_adapter: Arc<E>,
        network_name: String,
        logger: Logger,
        elastic_config: Option<ElasticLoggingConfig>,
    ) -> Self {
        let term_logger = logger.new(o!(
            "component" => "NetworkIngestor",
            "network_name" => network_name.clone()
        ));
        let logger = elastic_config
            .clone()
            .map_or(term_logger.clone(), |elastic_config| {
                split_logger(
                    term_logger.clone(),
                    elastic_logger(
                        ElasticDrainConfig {
                            general: elastic_config,
                            index: String::from("ethereum-network-ingestor-logs"),
                            document_type: String::from("log"),
                            custom_id_key: String::from("network"),
                            custom_id_value: network_name.clone(),
                            flush_interval: Duration::from_secs(5),
                        },
                        term_logger,
                    ),
                )
            });

        Self {
            store,
            eth_adapter,
            network_name,
            logger,
        }
    }

    pub fn into_polling_stream(self) -> impl Future<Item = (), Error = ()> {
        let logger = self.logger.clone();
        let logger_for_schema_error = self.logger.clone();

        info!(logger, "Starting Ethereum network ingestion");

        // Ensure that the database schema and tables for the network exist
        self.ensure_schema()
            .map_err(move |e| {
                crit!(
                    logger_for_schema_error,
                    "Failed to ensure Ethereum network schema";
                    "error" => format!("{}", e),
                );
            })
            .and_then(|_| {
                loop_fn(self, |ingestor| {
                    ingestor
                        .ingest_next_block()
                        .map(|ingestor| Loop::Continue(ingestor))
                })
                .map(|_: Self| ())
                .map_err(move |e| {
                    crit!(
                        logger,
                        "Failed to index Ethereum block explorer data";
                        "error" => format!("{}", e)
                    )
                })
            })
    }

    fn ensure_schema(&self) -> impl Future<Item = (), Error = Error> {
        future::result(self.store.ensure_network_schema(self.network_name.clone()))
    }

    fn ingest_next_block(self) -> impl Future<Item = Self, Error = Error> {
        future::ok(self)
    }
}
