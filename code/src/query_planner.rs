use crate::catalog::Catalog;
use crate::query::BuzzStep;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;

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
        &self,
        query_steps: Vec<BuzzStep>,
        nb_hcomb: i16,
    ) -> DistributedPlan {
        DistributedPlan { zones: vec![] }
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
