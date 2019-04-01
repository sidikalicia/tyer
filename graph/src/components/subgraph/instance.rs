use crate::prelude::*;
use web3::types::{Log, Transaction};

/// Represents a loaded instance of a subgraph.
pub trait SubgraphInstance<T>: Sized
where
    T: RuntimeHostBuilder,
{
    /// Creates a subgraph instance from a manifest.
    fn from_manifest(
        logger: &Logger,
        manifest: SubgraphManifest,
        host_builder: T,
    ) -> Result<Self, Error>;

    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Process and Ethereum trigger and return the resulting entity operations as a future.
    fn process_trigger(
        &self,
        logger: &Logger,
        block: Arc<EthereumBlock>,
        trigger: EthereumTrigger,
        entity_operations: Vec<EntityOperation>,
    ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error> + Send>;
}
