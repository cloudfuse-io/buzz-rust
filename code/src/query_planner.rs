use std::sync::Arc;

use crate::bee_query::{BeeQueryBatch, SizedFile};
use crate::catalog::{self, Catalog};
use crate::dataframe_ops::ClosureDataframeOps;
use crate::hive_query::HiveQuery;
use arrow::datatypes::Schema;
use datafusion::prelude::*;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error with the catalog"))]
    CatalogNotFound { source: catalog::Error },
    #[snafu(display("Intermediate schema returned by bee could not be estimated"))]
    IntermediateSchema {
        source: datafusion::error::DataFusionError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct QueryPlanner {
    catalog: Box<dyn Catalog>,
}

impl QueryPlanner {
    pub fn new(catalog: Box<dyn Catalog>) -> Self {
        Self { catalog }
    }

    fn hive(
        &self,
        query_id: String,
        nb_bees: usize,
        column_name: String,
        schema: Arc<Schema>,
    ) -> Result<HiveQuery> {
        let query = move |df: Arc<dyn DataFrame>| {
            // Ok(df)
            // df.select(vec![col(&column_name), col(&format!("COUNT({})", &column_name))])
            // df.aggregate(vec![col(&column_name)], vec![sum(col(&format!("COUNT({})", &column_name)))])
            df.aggregate(
                vec![col(&column_name)],
                vec![sum(col(&format!("COUNT({})", &column_name)))],
            )?
            .sort(vec![
                col(&format!("SUM(COUNT({}))", &column_name)).sort(false, false)
            ])
        };
        Ok(HiveQuery {
            query_id,
            nb_bees,
            schema,
            ops: Box::new(ClosureDataframeOps {
                ops: Arc::new(query),
            }),
        })
    }

    fn bee(&self, query_id: String, column_name: String) -> Result<BeeQueryBatch> {
        let query = move |df: Arc<dyn DataFrame>| {
            df.aggregate(vec![col(&column_name)], vec![count(col(&column_name))])?
                .sort(vec![
                    col(&format!("COUNT({})", &column_name)).sort(false, false)
                ])
        };
        Ok(BeeQueryBatch {
            query_id,
            input_schema: self
                .catalog
                .get_schema("nyc-taxi")
                .context(CatalogNotFound)?,
            region: "eu-west-1".to_owned(),
            file_bucket: "cloudfuse-taxi-data".to_owned(),
            file_distribution: vec![
                vec![SizedFile {
                    key: "raw_small/2009/01/data.parquet".to_owned(),
                    length: 27301328,
                }],
                vec![SizedFile {
                    key: "raw_small/2009/01/data.parquet".to_owned(),
                    length: 27301328,
                }],
            ],
            // file_distribution: vec![vec![SizedFile {
            //     key: "raw_5M/2009/01/data.parquet".to_owned(),
            //     length: 388070114,
            // }]],
            ops: Arc::new(ClosureDataframeOps {
                ops: Arc::new(query),
            }),
        })
    }

    pub fn plan(&self, column_name: String) -> Result<(HiveQuery, BeeQueryBatch)> {
        let query_id = "test0";
        // prepare bee queries
        let bee_query = self.bee(query_id.to_owned(), column_name.clone())?;
        // compute schema that will be returned by bees
        let intermediate_schema =
            bee_query.output_schema().context(IntermediateSchema)?;
        // prepare hive query
        let hive_query = self.hive(
            query_id.to_owned(),
            bee_query.nb_bees(),
            column_name.clone(),
            intermediate_schema,
        )?;
        Ok((hive_query, bee_query))
    }
}
