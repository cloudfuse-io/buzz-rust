use crate::datasource::{CatalogTable, HBeeTableDesc, HCombTable, HCombTableDesc};
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
pub struct HBeePlan {
    pub sql: String,
    pub source: String,
    pub table: HBeeTableDesc,
}

#[derive(Debug)]
pub struct HCombPlan {
    pub sql: String,
    pub source: String,
    pub table: HCombTableDesc,
}

#[derive(Debug)]
pub struct ZonePlan {
    pub hbee: Vec<HBeePlan>,
    pub hcomb: HCombPlan,
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

        let hbee_step = &query_steps[0];
        let hcomb_step = &query_steps[1];

        let bee_df = self.execution_context.sql(&hbee_step.sql)?;
        let src_bee_plan = self.execution_context.optimize(&bee_df.to_logical_plan())?;
        let hbee_actual_src = utils::find_table_name::<CatalogTable>(&src_bee_plan)?;
        let bee_output_schema = src_bee_plan.schema().as_ref().clone();
        let bee_plans = self.split(&src_bee_plan, vec![]).await?;
        let nb_hbee = bee_plans.len();

        if nb_hbee == 0 {
            return Ok(DistributedPlan {
                zones: vec![],
                nb_hbee,
            });
        }

        // register a handle to the intermediate table on the context
        let hcomb_table_desc =
            HCombTableDesc::new(query_id, nb_hbee, bee_output_schema.into());
        let hcomb_expected_src = &hbee_step.name;

        // plan the hcomb part of the query, to check if it is valid
        let hcomb_table = HCombTable::new_empty(hcomb_table_desc.clone());
        self.execution_context
            .register_table(hcomb_expected_src, Box::new(hcomb_table));
        let hcomb_df = self.execution_context.sql(&hcomb_step.sql)?;
        let hcomb_plan = hcomb_df.to_logical_plan();
        let hcomb_actual_src = utils::find_table_name::<HCombTable>(&hcomb_plan)?;
        if hcomb_actual_src != hcomb_expected_src {
            return Err(BuzzError::BadRequest(format!(
                "The source table for the {} step is not an HBee",
                hcomb_step.name,
            )));
        }

        // If they are less hbees than hcombs, don't use all hcombs
        let used_hcomb = std::cmp::min(nb_hcomb as usize, nb_hbee);

        // init plans for each zone
        let mut zones = (0..used_hcomb)
            .map(|_i| ZonePlan {
                hbee: vec![],
                hcomb: HCombPlan {
                    table: hcomb_table_desc.clone(),
                    sql: query_steps[1].sql.clone(),
                    source: hcomb_actual_src.to_owned(),
                },
            })
            .collect::<Vec<_>>();
        // distribute hbee plans between zones
        bee_plans.into_iter().enumerate().for_each(|(i, bee_plan)| {
            zones[i % used_hcomb].hbee.push(HBeePlan {
                table: bee_plan,
                sql: hbee_step.sql.clone(),
                source: hbee_actual_src.to_owned(),
            })
        });

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
    ) -> BoxFuture<'a, Result<Vec<HBeeTableDesc>>> {
        async move {
            let new_inputs = datafusion::optimizer::utils::inputs(&plan);
            if new_inputs.len() > 1 {
                Err(not_impl_err!(
                    "Operations with more than one inputs are not supported",
                ))
            } else if new_inputs.len() == 1 {
                let mut filter_exprs = vec![];
                if let LogicalPlan::Filter { predicate, .. } = &plan {
                    plan_utils::split_expr(predicate, &mut filter_exprs);
                }
                let table_descs = self
                    .split(new_inputs[0], filter_exprs.into_iter().cloned().collect())
                    .await?;
                Ok(table_descs)
            } else if let Some(catalog_table) = Self::as_catalog(&plan) {
                let partition_exprs =
                    catalog_table.extract_partition_exprs(upper_lvl_filters)?;
                let table_descs = catalog_table.split(&partition_exprs).await?;
                Ok(table_descs)
            } else {
                Err(not_impl_err!("Split only works with catalog tables",))
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
