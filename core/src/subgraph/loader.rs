use std::collections::HashMap;
use std::iter::FromIterator;

use graph::data::subgraph::schema::*;
use graph::prelude::{GraphQlRunner, SubgraphLoader as SubgraphLoaderTrait, *};
use graph_graphql::graphql_parser::{parse_query, query as q};
use graph::data::subgraph::{UnresolvedDataSource};

pub struct SubgraphLoader<L, Q, S> {
    store: Arc<S>,
    link_resolver: Arc<L>,
    graphql_runner: Arc<Q>,
}

impl<L, Q, S> SubgraphLoader<L, Q, S>
where
    L: LinkResolver,
    S: Store + SubgraphDeploymentStore,
    Q: GraphQlRunner,
{
    pub fn new(store: Arc<S>, link_resolver: Arc<L>, graphql_runner: Arc<Q>) -> Arc<Self> {
        Arc::new(SubgraphLoader {
            store,
            link_resolver,
            graphql_runner,
        })
    }
}

impl<L, Q, S> SubgraphLoaderTrait for SubgraphLoader<L, Q, S>
where
    L: LinkResolver,
    Q: GraphQlRunner,
    S: Store + SubgraphDeploymentStore,
{
    fn load_dynamic_data_sources(
        self: Arc<Self>,
        deployment: &SubgraphDeploymentId,
    ) -> Box<Future<Item = Vec<DataSource>, Error = Error> + Send> {
        let self_for_resolving = self.clone();
        let deployment_for_err1 = deployment.clone();
        let deployment_for_err2 = deployment.clone();

        // Obtain the "subgraphs" schema
        let schema = match self.store.subgraph_schema(&SUBGRAPHS_ID) {
            Ok(schema) => schema,
            Err(e) => return Box::new(future::err(e)),
        };

        let query = Query {
            schema,
            document: parse_query(
                r#"
                query deployment($id: ID!) {
                  subgraphDeployment(id: $id) {
                    dynamicDataSources(orderBy: id) {
                      kind
                      network
                      name
                      source { address abi }
                      mapping {
                        kind
                        apiVersion
                        language
                        file
                        entities
                        abis { name file }
                        eventHandlers { event handler }
                      }
                      templates {
                        kind
                        network
                        name
                        source { abi }
                        mapping {
                          kind
                          apiVersion
                          language
                          file
                          entities
                          abis { name file }
                          eventHandlers { event handler }
                        }
                      }
                    }
                  }
                }
                "#,
            )
            .expect("invalid query for dynamic data sources"),
            variables: Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("id"), q::Value::String(deployment.to_string()))].into_iter(),
            ))),
        };

        Box::new(
            self.graphql_runner
                .run_query(query)
                .map_err(move |e| {
                    format_err!("Failed to load manifest `{}`: {}", deployment_for_err1, e)
                })
                .and_then(move |query_result| {
                    let data = match query_result.data.expect("subgraph deployment not found") {
                        q::Value::Object(obj) => obj,
                        _ => {
                            return future::err(format_err!(
                                "Query result for deployment `{}` is not an on object",
                                deployment_for_err2,
                            ));
                        }
                    };

                    // Extract the deployment from the query result
                    let deployment = match data.get("subgraphDeployment") {
                        Some(q::Value::Object(obj)) => obj,
                        _ => {
                            return future::err(format_err!(
                                "Deployment `{}` is not an object",
                                deployment_for_err2
                            ));
                        }
                    };

                    // Extract the dynamic data sources from the query result
                    let raw_data_sources = match deployment.get("dynamicDataSources") {
                        Some(q::Value::List(objs)) => {
                            if objs.iter().all(|obj| match obj {
                                q::Value::Object(_) => true,
                                _ => false,
                            }) {
                                objs
                            } else {
                                return future::err(format_err!(
                                    "Not all dynamic data sources of deployment `{}` are objects",
                                    deployment_for_err2
                                ));
                            }
                        }
                        _ => {
                            return future::err(format_err!(
                                "Dynamic data sources of deployment `{}` are not a list",
                                deployment_for_err2
                            ));
                        }
                    };

                    // Parse the raw data sources into typed entities
                    let data_source_entities = match raw_data_sources.into_iter().try_fold(
                        vec![],
                        |mut entities, value| {
                            entities.push(
                                EthereumContractDataSourceEntity::try_from_value(
                                    value,
                                )?
                            );
                            Ok(entities)
                        },
                    ) as Result<Vec<_>, Error> {
                        Ok(entities) => entities,
                        Err(e) => {
                            return future::err(format_err!(
                                "Failed to parse dynamic data source entities: {}",
                                e
                            ));
                        }
                    };

                    // Turn the entities into unresolved data sources
                    future::ok(data_source_entities.into_iter().map(Into::into).collect::<Vec<UnresolvedDataSource>>())
                })
                .and_then(move |unresolved_data_sources| {
                    // Resolve the data sources and return them
                    stream::iter_ok(unresolved_data_sources)
                        .fold(vec![], move |mut resolved, data_source| {
                            data_source
                                .resolve(&*self_for_resolving.link_resolver)
                                .and_then(|data_source| {
                                    resolved.push(data_source);
                                    future::ok(resolved)
                                })
                        })
                }),
        )
    }
}
