
use super::*;
use tokio_threadpool::blocking;

/// A tokio-aware wrapper that moves store operations to a blocking thread.
struct TokioStoreWrapper<S> {
    store: Arc<S>
}

/// Common trait for store implementations.
impl<S: Store> Store for TokioStoreWrapper<S> {
    fn block_ptr(&self, subgraph_id: SubgraphDeploymentId) -> Result<EthereumBlockPointer, Error> {
        blocking(|| self.store.block_ptr(subgraph_id))
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        blocking(|| self.store.get(key))
    }

    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        blocking(|| self.store.find(query))
    }

    fn find_one(&self, query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        blocking(|| self.store.find_one(query))
    }

    fn find_ens_name(&self, hash: &str) -> Result<Option<String>, QueryExecutionError> {
        blocking(|| self.store.find_ens_name(hash))
    }

    fn set_block_ptr_with_no_changes(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        blocking(|| self.store.set_block_ptr_with_no_changes(subgraph_id, block_ptr_from, block_ptr_to))
    }

    fn transact_block_operations(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
        operations: Vec<EntityOperation>,
    ) -> Result<(), StoreError> {
        blocking(|| self.store.transact_block_operations(subgraph_id, block_ptr_from, block_ptr_to))
    }

    fn apply_entity_operations(
        &self,
        operations: Vec<EntityOperation>,
        history_event: Option<HistoryEvent>,
    ) -> Result<(), StoreError> {
        blocking(|| self.store.apply_entity_operations(operations, history_event))
    }

    fn build_entity_attribute_indexes(
        &self,
        indexes: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        blocking(|| self.store.apply_entity_operations(indexes))
    }

    fn revert_block_operations(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        blocking(|| self.store.revert_block_operations(subgraph_id, block_ptr_from, block_ptr_to))
    }

    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        blocking(|| self.store.subscribe(entities))
    }

    fn resolve_subgraph_name_to_id(
        &self,
        name: SubgraphName,
    ) -> Result<Option<SubgraphDeploymentId>, Error> {
        blocking(|| self.store.resolve_subgraph_name_to_id(name))
    }

    fn read_subgraph_version_summaries(
        &self,
        deployment_ids: Vec<SubgraphDeploymentId>,
    ) -> Result<(Vec<SubgraphVersionSummary>, Vec<EntityOperation>), Error> {
        blocking(|| self.store.resolve_subgraph_name_to_id(deployment_ids))
    }

    /// Create a new subgraph deployment. The deployment must not exist yet. `ops`
    /// needs to contain all the operations on subgraphs and subgraph deployments to
    /// create the deployment, including any assignments as a current or pending
    /// version
    fn create_subgraph_deployment(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        ops: Vec<EntityOperation>,
    ) -> Result<(), StoreError>;
}

pub trait SubgraphDeploymentStore: Send + Sync + 'static {
    fn subgraph_schema(
        &self,
        subgraph_id: &SubgraphDeploymentId,
    ) -> Box<dyn Future<Item = Arc<Schema>, Error = Error> + Send + Sync>;
}

/// Common trait for blockchain store implementations.
pub trait ChainStore: Send + Sync + 'static {
    type ChainHeadUpdateListener: ChainHeadUpdateListener;

    /// Get a pointer to this blockchain's genesis block.
    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error>;

    /// Insert blocks into the store (or update if they are already present).
    fn upsert_blocks<'a, B, E>(&self, blocks: B) -> Box<Future<Item = (), Error = E> + Send + 'a>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'a,
        E: From<Error> + Send + 'a;

    /// Try to update the head block pointer to the block with the highest block number.
    ///
    /// Only updates pointer if there is a block with a higher block number than the current head
    /// block, and the `ancestor_count` most recent ancestors of that block are in the store.
    /// Note that this means if the Ethereum node returns a different "latest block" with a
    /// different hash but same number, we do not update the chain head pointer.
    /// This situation can happen on e.g. Infura where requests are load balanced across many
    /// Ethereum nodes, in which case it's better not to continuously revert and reapply the latest
    /// blocks.
    ///
    /// If the pointer was updated, returns `Ok(vec![])`, and fires a HeadUpdateEvent.
    ///
    /// If no block has a number higher than the current head block, returns `Ok(vec![])`.
    ///
    /// If the candidate new head block had one or more missing ancestors, returns
    /// `Ok(missing_blocks)`, where `missing_blocks` is a nonexhaustive list of missing blocks.
    fn attempt_chain_head_update(&self, ancestor_count: u64) -> Result<Vec<H256>, Error>;

    /// Subscribe to chain head updates.
    fn chain_head_updates(&self) -> ChainHeadUpdateStream;

    /// Get the current head block pointer for this chain.
    /// Any changes to the head block pointer will be to a block with a larger block number, never
    /// to a block with a smaller or equal block number.
    ///
    /// The head block pointer will be None on initial set up.
    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error>;

    /// Get Some(block) if it is present in the chain store, or None.
    fn block(&self, block_hash: H256) -> Result<Option<EthereumBlock>, Error>;

    /// Get the `offset`th ancestor of `block_hash`, where offset=0 means the block matching
    /// `block_hash` and offset=1 means its parent. Returns None if unable to complete due to
    /// missing blocks in the chain store.
    ///
    /// Returns an error if the offset would reach past the genesis block.
    fn ancestor_block(
        &self,
        block_ptr: EthereumBlockPointer,
        offset: u64,
    ) -> Result<Option<EthereumBlock>, Error>;
}
