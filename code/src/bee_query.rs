use std::iter::Iterator;
use std::sync::Arc;
use std::time::Instant;

use crate::catalog::SizedFile;
use crate::dataframe_ops::DataframeOperations;
use crate::datasource::{EmptyTable, ParquetTable};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use datafusion::error::Result;
use datafusion::prelude::*;

pub struct BeeQueryBatch {
    pub query_id: String,
    pub region: String,
    pub file_bucket: String,
    pub file_distribution: Vec<Vec<SizedFile>>,
    pub input_schema: Arc<Schema>,
    pub ops: Arc<dyn DataframeOperations>,
}

impl BeeQueryBatch {
    /// The schema that will be returned by the bees after executing they part of the query
    pub fn output_schema(&self) -> Result<Arc<Schema>> {
        let mut ctx = ExecutionContext::with_config(ExecutionConfig::new());
        let empty_table = EmptyTable::new(Arc::clone(&self.input_schema));
        let df = self.ops.apply_to(ctx.read_table(Arc::new(empty_table))?)?;
        let logical_plan = df.to_logical_plan();
        Ok(Arc::clone(logical_plan.schema()))
    }

    pub fn nb_bees(&self) -> usize {
        self.file_distribution.len()
    }

    pub fn queries(&self) -> impl Iterator<Item = BeeQuery> {
        let query_id = self.query_id.clone();
        let region = self.region.clone();
        let file_bucket = self.file_bucket.clone();
        let input_schema = self.input_schema.clone();
        let ops = self.ops.clone();
        self.file_distribution
            .clone()
            .into_iter()
            .map(move |elem| BeeQuery {
                query_id: query_id.clone(),
                region: region.clone(),
                file_bucket: file_bucket.clone(),
                files: elem.clone(),
                input_schema: input_schema.clone(),
                ops: ops.clone(),
            })
    }
}

pub struct BeeQuery {
    pub query_id: String,
    pub region: String,
    pub file_bucket: String,
    pub files: Vec<SizedFile>,
    pub input_schema: Arc<Schema>,
    pub ops: Arc<dyn DataframeOperations>,
}

pub struct BeeQueryRunner {
    concurrency: usize,
    batch_size: usize,
}

impl BeeQueryRunner {
    pub fn new() -> Self {
        Self {
            concurrency: 1,
            batch_size: 2048,
        }
    }

    pub async fn run(&self, query: BeeQuery) -> Result<Vec<RecordBatch>> {
        let debug = true;
        let mut start = Instant::now();
        let config = ExecutionConfig::new()
            .with_concurrency(self.concurrency)
            .with_batch_size(self.batch_size);

        let mut parquet_table = ParquetTable::new(
            query.region,
            query.file_bucket,
            query.files,
            query.input_schema,
        );
        parquet_table.start_download().await;

        let mut ctx = ExecutionContext::with_config(config);
        let df = query
            .ops
            .apply_to(ctx.read_table(Arc::new(parquet_table))?)?;
        let logical_plan = df.to_logical_plan();
        if debug {
            println!("=> Original logical plan:\n{:?}", logical_plan);
        }
        let logical_plan = ctx.optimize(&logical_plan)?;
        if debug {
            println!("=> Optimized logical plan:\n{:?}", logical_plan);
        }
        let physical_plan = ctx.create_physical_plan(&logical_plan).unwrap();
        if debug {
            // println!("=> Physical plan:\n{:?}", physical_plan);
            println!("=> Schema:\n{:?}", physical_plan.schema());
        }
        let setup_duration = start.elapsed().as_millis();
        start = Instant::now();
        let result = ctx.collect(physical_plan).await?;
        if debug {
            pretty::print_batches(&result)?;
            println!("Setup took {} ms", setup_duration);
            println!("Processing took {} ms", start.elapsed().as_millis());
        }
        Ok(result)
    }
}
