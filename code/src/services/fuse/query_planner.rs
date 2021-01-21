use std::sync::Arc;

use crate::datasource::{CatalogTable, HCombTable};
use crate::error::{BuzzError, Result};
use crate::models::query::{BuzzStep, BuzzStepType};
use crate::not_impl_err;
use crate::plan_utils;
use crate::services::utils;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{Expr, LogicalPlan};
use futures::future::{BoxFuture, FutureExt};

pub struct QueryPlanner {
    /// This execution context is not meant to run queries but only to plan them.
    execution_context: ExecutionContext,
}

#[derive(Debug)]
pub struct ZonePlan {
    pub hbee: Vec<LogicalPlan>,
    pub hcomb: LogicalPlan,
}

/// The plans to be distributed among hbees and hcombs
/// To transfer them over the wire, these logical plans should be serializable
#[derive(Debug)]
pub struct DistributedPlan {
    /// One hcomb/hbee combination of plan for each zone.
    pub zones: Vec<ZonePlan>,
    pub nb_hbee: usize,
}

impl QueryPlanner {
    pub fn new() -> Self {
        Self {
            execution_context: ExecutionContext::new(),
        }
    }

    pub fn add_catalog(&mut self, name: &str, table: CatalogTable) {
        self.execution_context.register_table(name, Box::new(table));
    }

    pub async fn plan(
        &mut self,
        query_id: String,
        query_steps: Vec<BuzzStep>,
        nb_hcomb: i16,
    ) -> Result<DistributedPlan> {
        // TODO lift the limitation inforced by the following assert:
        assert!(
            query_steps.len() == 2
                && query_steps[0].step_type == BuzzStepType::HBee
                && query_steps[1].step_type == BuzzStepType::HComb,
            "You must have one exactly one HBee step followed by one HComb step for now"
        );

        let bee_df = self.execution_context.sql(&query_steps[0].sql)?;
        let src_bee_plan = self.execution_context.optimize(&bee_df.to_logical_plan())?;
        let bee_output_schema = src_bee_plan.schema().as_ref().clone();
        let bee_plans = self.split(&src_bee_plan, vec![]).await?.0;
        let nb_hbee = bee_plans.len();

        if nb_hbee == 0 {
            return Ok(DistributedPlan {
                zones: vec![],
                nb_hbee,
            });
        }

        // register a handle to the intermediate table on the context
        let result_table = HCombTable::new(query_id, nb_hbee, bee_output_schema.into());
        self.execution_context
            .register_table(&query_steps[0].name, Box::new(result_table));

        // run the hcomb part of the query
        let hcomb_df = self.execution_context.sql(&query_steps[1].sql)?;
        let hcomb_plan = hcomb_df.to_logical_plan();

        utils::find_table::<HCombTable>(&hcomb_plan).map_err(|_| {
            BuzzError::BadRequest(format!(
                "The source table for the {} step is not an HBee",
                query_steps[1].name,
            ))
        })?;

        // If they are less hbees than hcombs, don't use all hcombs
        let used_hcomb = std::cmp::min(nb_hcomb as usize, nb_hbee);

        // init plans for each zone
        let mut zones = (0..used_hcomb)
            .map(|_i| ZonePlan {
                hbee: vec![],
                hcomb: hcomb_plan.clone(),
            })
            .collect::<Vec<_>>();
        // distribute hbee plans between zones
        bee_plans
            .into_iter()
            .enumerate()
            .for_each(|(i, bee_plan)| zones[i % used_hcomb].hbee.push(bee_plan));

        Ok(DistributedPlan {
            zones: zones,
            nb_hbee,
        })
    }

    /// Takes a plan and if the source is a catalog, it distibutes the files accordingly
    /// Each resulting logical plan is a good workload for a given bee
    /// Only works with linear plans (only one datasource)
    fn split<'a>(
        &'a mut self,
        plan: &'a LogicalPlan,
        upper_lvl_filters: Vec<Expr>,
    ) -> BoxFuture<'a, Result<(Vec<LogicalPlan>, Vec<Expr>)>> {
        async move {
            let new_inputs = datafusion::optimizer::utils::inputs(&plan);
            if new_inputs.len() > 1 {
                Err(not_impl_err!(
                    "Operations with more than one inputs are not supported",
                ))
            } else if new_inputs.len() == 1 {
                let mut is_filter = false;
                let mut filter_exprs = vec![];
                if let LogicalPlan::Filter { predicate, .. } = &plan {
                    is_filter = true;
                    plan_utils::split_expr(predicate, &mut filter_exprs);
                }
                let (inputs, remaining_filter_exprs) = self
                    .split(new_inputs[0], filter_exprs.into_iter().cloned().collect())
                    .await?;
                let splitted_plans = inputs
                    .into_iter()
                    .map(|lp| -> Result<LogicalPlan> {
                        // if all filters where managed at the partition level, prune the filter stage
                        if is_filter && remaining_filter_exprs.len() == 0 {
                            Ok(lp)
                        } else if is_filter {
                            Ok(datafusion::optimizer::utils::from_plan(
                                plan,
                                &vec![plan_utils::merge_expr(&remaining_filter_exprs)],
                                &vec![lp],
                            )?)
                        } else {
                            let exprs = datafusion::optimizer::utils::expressions(plan);
                            Ok(datafusion::optimizer::utils::from_plan(
                                plan,
                                &exprs,
                                &vec![lp],
                            )?)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((splitted_plans, upper_lvl_filters))
            } else if let Some(catalog_table) = Self::as_catalog(&plan) {
                let (regular_exprs, partition_exprs) =
                    catalog_table.extract_partition_exprs(upper_lvl_filters)?;
                let splitted_plans = catalog_table
                    .split(&partition_exprs)
                    .await?
                    .into_iter()
                    .map(|item| {
                        Ok(self
                            .execution_context
                            .read_table(Arc::new(item))?
                            .to_logical_plan())
                    })
                    .collect::<Result<Vec<LogicalPlan>>>()?;
                Ok((splitted_plans, regular_exprs))
            } else {
                Ok((vec![plan.clone()], upper_lvl_filters))
            }
        }
        .boxed() // recursion in an `async fn` requires boxing
    }

    fn as_catalog<'a>(plan: &'a LogicalPlan) -> Option<&'a CatalogTable> {
        if let LogicalPlan::TableScan { source: table, .. } = plan {
            table.as_any().downcast_ref::<CatalogTable>()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::CatalogTable;
    use crate::datasource::MockSplittableTable;

    #[tokio::test]
    async fn test_simple_query() {
        let mut planner = QueryPlanner::new();
        let nb_split = 5;
        planner.add_catalog(
            "test",
            CatalogTable::new(Box::new(MockSplittableTable::new(nb_split, 0))),
        );

        let steps = vec![
            BuzzStep {
                sql: "SELECT * FROM test".to_owned(),
                name: "mapper".to_owned(),
                step_type: BuzzStepType::HBee,
            },
            BuzzStep {
                sql: "SELECT * FROM mapper".to_owned(),
                name: "reducer".to_owned(),
                step_type: BuzzStepType::HComb,
            },
        ];

        let plan_res = planner.plan("mock_query_id".to_owned(), steps, 1).await;
        let plan = plan_res.expect("The planner failed on a simple query");
        assert_eq!(plan.zones.len(), 1);
        assert_eq!(plan.zones[0].hbee.len(), nb_split);
    }

    #[tokio::test]
    async fn test_query_unknown_table() {
        let mut planner = QueryPlanner::new();
        let steps = vec![
            BuzzStep {
                sql: "SELECT * FROM test".to_owned(),
                name: "mapper".to_owned(),
                step_type: BuzzStepType::HBee,
            },
            BuzzStep {
                sql: "SELECT * FROM mapper".to_owned(),
                name: "reducer".to_owned(),
                step_type: BuzzStepType::HComb,
            },
        ];

        let plan_res = planner.plan("mock_query_id".to_owned(), steps, 1).await;
        assert!(
            plan_res.is_err(),
            "The planner should have failed as the 'test' table is not defined"
        );
    }

    #[tokio::test]
    async fn test_query_with_condition() {
        let mut planner = QueryPlanner::new();
        let nb_split = 5;
        planner.add_catalog(
            "test",
            CatalogTable::new(Box::new(MockSplittableTable::new(nb_split, 2))),
        );

        let steps = vec![
            BuzzStep {
                sql: "SELECT * FROM test WHERE 
                part_key_2>='part_value_001' AND 
                part_key_2<='part_value_003' AND
                data_col=0"
                    .to_owned(),
                name: "mapper".to_owned(),
                step_type: BuzzStepType::HBee,
            },
            BuzzStep {
                sql: "SELECT * FROM mapper".to_owned(),
                name: "reducer".to_owned(),
                step_type: BuzzStepType::HComb,
            },
        ];

        let plan_res = planner.plan("mock_query_id".to_owned(), steps, 1).await;
        let plan = plan_res.expect("The planner failed on a query with condition");
        assert_eq!(plan.zones.len(), 1);
        assert_eq!(plan.zones[0].hbee.len(), 3);
    }

    #[tokio::test]
    async fn test_query_with_empty_catalog() {
        let mut planner = QueryPlanner::new();
        let nb_split = 5;
        planner.add_catalog(
            "test",
            CatalogTable::new(Box::new(MockSplittableTable::new(nb_split, 1))),
        );

        let steps = vec![
            BuzzStep {
                sql: "SELECT * FROM test WHERE part_key_1='not_in_partition_value'"
                    .to_owned(),
                name: "mapper".to_owned(),
                step_type: BuzzStepType::HBee,
            },
            BuzzStep {
                sql: "SELECT * FROM mapper".to_owned(),
                name: "reducer".to_owned(),
                step_type: BuzzStepType::HComb,
            },
        ];

        let plan_res = planner.plan("mock_query_id".to_owned(), steps, 1).await;
        let plan =
            plan_res.expect("The planner failed on a query with no data in catalog");
        assert_eq!(plan.zones.len(), 0);
    }

    #[tokio::test]
    async fn test_query_with_grouping() {
        let mut planner = QueryPlanner::new();
        let nb_split = 5;
        planner.add_catalog(
            "test",
            CatalogTable::new(Box::new(MockSplittableTable::new(nb_split, 1))),
        );

        let steps = vec![
            BuzzStep {
                sql:
                    "SELECT data_col, count(data_col) as cnt FROM test GROUP BY data_col"
                        .to_owned(),
                name: "mapper".to_owned(),
                step_type: BuzzStepType::HBee,
            },
            BuzzStep {
                sql: "SELECT data_col, count(cnt) FROM mapper GROUP BY data_col"
                    .to_owned(),
                name: "reducer".to_owned(),
                step_type: BuzzStepType::HComb,
            },
        ];

        let plan_res = planner.plan("mock_query_id".to_owned(), steps, 1).await;
        let plan = plan_res.expect("The planner failed on a query with condition");
        assert_eq!(plan.zones.len(), 1);
        assert_eq!(plan.zones[0].hbee.len(), nb_split);
    }

    #[tokio::test]
    async fn test_bad_hcomb_table() {
        let mut planner = QueryPlanner::new();
        let nb_split = 5;
        planner.add_catalog(
            "test",
            CatalogTable::new(Box::new(MockSplittableTable::new(nb_split, 0))),
        );

        let steps = vec![
            BuzzStep {
                sql: "SELECT * FROM test".to_owned(),
                name: "mapper".to_owned(),
                step_type: BuzzStepType::HBee,
            },
            BuzzStep {
                sql: "SELECT * FROM test".to_owned(),
                name: "reducer".to_owned(),
                step_type: BuzzStepType::HComb,
            },
        ];

        let plan_res = planner.plan("mock_query_id".to_owned(), steps, 1).await;
        plan_res.expect_err("The source table for the reducer step is not an HBee");
    }
}
