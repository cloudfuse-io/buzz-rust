use std::sync::Arc;
use std::time::Instant;

use crate::dataframe_ops::DataframeOperations;
use crate::datasource::ParquetTable;
use crate::s3;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use datafusion::error::Result;
use datafusion::prelude::*;

pub struct BeeQuery {
    pub query_id: String,
    pub region: String,
    pub file_bucket: String,
    pub file_key: String,
    pub file_length: u64,
    pub schema: Arc<Schema>,
    pub ops: Box<dyn DataframeOperations>,
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
        let file = s3::S3FileAsync::new(
            query.file_bucket.clone(),
            query.file_key.clone(),
            query.file_length,
            s3::new_client(&query.region),
        );
        let parquet_table = Arc::new(ParquetTable::new(file.clone(), query.schema)?);
        let mut ctx = ExecutionContext::with_config(config);
        let df = query.ops.apply_to(ctx.read_table(parquet_table.clone())?)?;
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
