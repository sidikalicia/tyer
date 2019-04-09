use failure::Error;
use futures::Stream;

use crate::prelude::*;

pub trait BlockStream:
    Stream<Item = EthereumBlock, Error = Error> + EventConsumer<ChainHeadUpdate>
{
}

pub trait BlockStreamBuilder: Clone + Send + Sync {
    type Stream: BlockStream + Send + 'static;

    fn with_subgraph(self, manifest: &SubgraphManifest) -> Self;
    fn with_logger(self, logger: Logger) -> Self;
    fn build(&self, log_filter: EthereumLogFilter) -> Self::Stream;
}
