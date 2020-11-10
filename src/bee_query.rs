use std::sync::Arc;
use std::time::Instant;

use crate::table_provider::ParquetTable;
use arrow::util::pretty;
use datafusion::error::Result;
use datafusion::prelude::*;

use crate::s3;

pub struct QueryConfig {
    pub concurrency: usize,
    pub batch_size: usize,
    pub region: String,
    pub file_bucket: String,
    pub file_key: String,
    pub file_length: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            concurrency: 1,
            batch_size: 2048,
            region: "eu-west-1".to_owned(),
            file_bucket: String::new(),
            file_key: String::new(),
            file_length: 0,
        }
    }
}

pub async fn run<F>(query_conf: QueryConfig, query: F) -> Result<()>
where
    F: Fn(Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>>,
{
    let debug = true;
    let mut start = Instant::now();

    let config = ExecutionConfig::new()
        .with_concurrency(query_conf.concurrency)
        .with_batch_size(query_conf.batch_size);

    let file = s3::S3FileAsync::new(
        query_conf.file_bucket.clone(),
        query_conf.file_key.clone(),
        query_conf.file_length,
        s3::new_client(&query_conf.region),
    );

    file.download_footer().await;

    let parquet_table = Arc::new(ParquetTable::try_new(file.clone())?);

    let mut ctx = ExecutionContext::with_config(config);

    let df = query(ctx.read_table(parquet_table.clone())?)?;
    // .aggregate(vec![col("device")], vec![count(col("device"))])?
    // .sort(vec![col("COUNT(device)").sort(false, false)])?;

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
        println!("=> Physical plan:\n{:?}", physical_plan);
    }

    let setup_duration = start.elapsed().as_millis();
    start = Instant::now();

    file.download_columns().await;

    let dl_duration = start.elapsed().as_millis();
    start = Instant::now();

    let result = ctx.collect(physical_plan).await?;

    if debug {
        pretty::print_batches(&result)?;
        println!("Setup took {} ms", setup_duration);
        println!("Download took {} ms", dl_duration);
        println!("Processing took {} ms", start.elapsed().as_millis());
    }
    Ok(())
}
