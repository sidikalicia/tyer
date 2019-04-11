use crate::prelude::*;

pub trait SubgraphLoader {
    fn load_dynamic_data_sources(
        self: Arc<Self>,
        id: &SubgraphDeploymentId,
    ) -> Box<Future<Item = Vec<DataSource>, Error = Error> + Send>;
}
