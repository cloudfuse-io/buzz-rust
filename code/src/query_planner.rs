use chrono::Utc;
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::datasource::{ResultTable, StaticCatalogTable};
use crate::error::Result;
use crate::not_impl_err;
use crate::query::{BuzzStep, BuzzStepType};
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use futures::future::{BoxFuture, FutureExt};

pub struct QueryPlanner {
    /// This execution context is not meant to run queries but only to plan them.
    execution_context: ExecutionContext,
}

pub struct ZonePlan {
    pub hbee: Vec<LogicalPlan>,
    pub hcomb: LogicalPlan,
}

/// The plans to be distributed among hbees and hcombs
/// To transfer them over the wire, these logical plans should be serializable
pub struct DistributedPlan {
    /// One hcomb/hbee combination of plan for each zone.
    pub zones: Vec<ZonePlan>,
}

impl QueryPlanner {
    pub fn new() -> Self {
        Self {
            execution_context: ExecutionContext::new(),
        }
    }

    pub fn add_catalog(&mut self, catalog: &dyn Catalog) {
        catalog.fill(&mut self.execution_context);
    }

    pub async fn plan(
        &mut self,
        query_steps: Vec<BuzzStep>,
        nb_hcomb: i16,
    ) -> Result<DistributedPlan> {
        // TODO lift the limitation inforced by the following assert:
        assert!(
            query_steps.len() == 2
                && query_steps[0].step_type == BuzzStepType::HBee
                && query_steps[1].step_type == BuzzStepType::HComb,
            "You must have one exactly one hbee step followed by one hcomb step for now"
        );

        let bee_df = self.execution_context.sql(&query_steps[0].sql)?;
        let bee_plan = bee_df.to_logical_plan();
        let bee_output_schema = bee_plan.schema().as_ref().clone();
        let bee_plans = self.split(&bee_plan).await?;

        // register a handle to the intermediate table on the context
        let result_table = ResultTable::new(
            format!("{}-{}", &query_steps[0].name, Utc::now().to_rfc3339()),
            bee_plans.len(),
            bee_output_schema.into(),
        );
        self.execution_context
            .register_table(&query_steps[0].name, Box::new(result_table));

        // run the hcomb part of the query
        let hcomb_df = self.execution_context.sql(&query_steps[1].sql)?;
        let hcomb_plan = hcomb_df.to_logical_plan();

        // TODO check that the source is a valid hcomb provider

        // If they are less hbees than hcombs, don't use all hcombs
        let used_hcomb = std::cmp::min(nb_hcomb as usize, bee_plans.len());

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

        Ok(DistributedPlan { zones: zones })
    }

    /// takes a plan and if the source is a catalog, it distibutes the files accordingly
    /// each logical is a good workload for a given bee
    fn split<'a>(
        &'a mut self,
        plan: &'a LogicalPlan,
    ) -> BoxFuture<'a, Result<Vec<LogicalPlan>>> {
        async move {
            let new_inputs = datafusion::optimizer::utils::inputs(&plan);
            if new_inputs.len() > 1 {
                Err(not_impl_err!(
                    "Operations with more than one inputs are not supported",
                ))
            } else if new_inputs.len() == 1 {
                self.split(new_inputs[0]).await
            } else if let Some(catalog_table) = Self::as_catalog(&plan) {
                catalog_table
                    .split()
                    .iter()
                    .map(|item| {
                        Ok(self
                            .execution_context
                            .read_table(Arc::clone(item))?
                            .to_logical_plan())
                    })
                    .collect()
            } else {
                Ok(vec![plan.clone()])
            }
        }
        .boxed() // recursion in an `async fn` requires boxing
    }

    fn as_catalog<'a>(plan: &'a LogicalPlan) -> Option<&'a StaticCatalogTable> {
        if let LogicalPlan::TableScan { source: table, .. } = plan {
            table.as_any().downcast_ref::<StaticCatalogTable>()
        } else {
            None
        }
    }
}

// use std::sync::Arc;

// use crate::catalog::{Catalog, SizedFile};
// use crate::dataframe_ops::ClosureDataframeOps;
// use crate::error::Result;
// use crate::hbee_query::HBeeQueryBatch;
// use crate::hcomb_query::HCombQuery;
// use arrow::datatypes::Schema;
// use datafusion::dataframe::DataFrame;
// use datafusion::logical_plan::{col, count, sum};

// pub struct QueryPlanner {
//     catalog: Box<dyn Catalog>,
// }

// impl QueryPlanner {
//     pub fn new(catalog: Box<dyn Catalog>) -> Self {
//         Self { catalog }
//     }

//     fn hcomb(
//         &self,
//         query_id: String,
//         nb_hbees: usize,
//         column_name: String,
//         schema: Arc<Schema>,
//     ) -> Result<HCombQuery> {
//         let query = move |df: Arc<dyn DataFrame>| {
//             // Ok(df)
//             // df.select(vec![col(&column_name), col(&format!("COUNT({})", &column_name))])
//             // df.aggregate(vec![col(&column_name)], vec![sum(col(&format!("COUNT({})", &column_name)))])
//             df.aggregate(
//                 vec![col(&column_name)],
//                 vec![sum(col(&format!("COUNT({})", &column_name)))],
//             )?
//             .sort(vec![
//                 col(&format!("SUM(COUNT({}))", &column_name)).sort(false, false)
//             ])
//         };
//         Ok(HCombQuery {
//             query_id,
//             nb_hbees,
//             schema,
//             ops: Box::new(ClosureDataframeOps {
//                 ops: Arc::new(query),
//             }),
//         })
//     }

//     fn hbee(&self, query_id: String, column_name: String) -> Result<HBeeQueryBatch> {
//         let query = move |df: Arc<dyn DataFrame>| {
//             df.aggregate(vec![col(&column_name)], vec![count(col(&column_name))])?
//                 .sort(vec![
//                     col(&format!("COUNT({})", &column_name)).sort(false, false)
//                 ])
//         };
//         Ok(HBeeQueryBatch {
//             query_id,
//             input_schema: self.catalog.get_schema("nyc-taxi")?,
//             region: "eu-west-1".to_owned(),
//             file_bucket: "cloudfuse-taxi-data".to_owned(),
//             file_distribution: vec![
//                 vec![SizedFile {
//                     key: "raw_small/2009/01/data.parquet".to_owned(),
//                     length: 27301328,
//                 }],
//                 vec![SizedFile {
//                     key: "raw_small/2009/01/data.parquet".to_owned(),
//                     length: 27301328,
//                 }],
//             ],
//             // file_distribution: vec![vec![SizedFile {
//             //     key: "raw_5M/2009/01/data.parquet".to_owned(),
//             //     length: 388070114,
//             // }]],
//             ops: Arc::new(ClosureDataframeOps {
//                 ops: Arc::new(query),
//             }),
//         })
//     }

//     pub fn plan(&self, column_name: String) -> Result<(HCombQuery, HBeeQueryBatch)> {
//         let query_id = "test0";
//         // prepare hbee queries
//         let hbee_query = self.hbee(query_id.to_owned(), column_name.clone())?;
//         // compute schema that will be returned by hbees
//         let intermediate_schema = hbee_query.output_schema()?;
//         // prepare hcomb query
//         let hcomb_query = self.hcomb(
//             query_id.to_owned(),
//             hbee_query.nb_hbees(),
//             column_name.clone(),
//             intermediate_schema,
//         )?;
//         Ok((hcomb_query, hbee_query))
//     }
// }
