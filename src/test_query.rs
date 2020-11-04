use std::sync::Arc;
use std::time::Instant;

use crate::table_provider::ParquetTable;
use arrow::util::pretty;
use datafusion::error::Result;
use datafusion::prelude::*;

use crate::s3;

pub async fn run() -> Result<()> {
  let debug = true;
  let mut start = Instant::now();

  let config = ExecutionConfig::new()
    .with_concurrency(1)
    .with_batch_size(2048);

  let file = s3::S3FileAsync::new(
    "bb-test-data-dev".to_owned(),
    "bid-large.parquet".to_owned(),
    218890209,
    // "bb-test-data-dev".to_owned(),
    // "bid-small.parquet".to_owned(),
    // 2418624,
    s3::new_client(),
  );

  file.download_footer().await;

  let parquet_table = Arc::new(ParquetTable::try_new(file.clone())?);

  let mut ctx = ExecutionContext::with_config(config);

  let df = ctx
    .read_table(parquet_table.clone())?
    .aggregate(vec![col("device")], vec![count(col("device"))])?
    .sort(vec![col("COUNT(device)").sort(false, false)])?;

  let logical_plan = df.to_logical_plan();
  if debug {
    println!("=> Original logical plan:\n{:?}", logical_plan);
  }

  let logical_plan = ctx.optimize(&logical_plan)?;
  if debug {
    println!("=> Optimized logical plan:\n{:?}", logical_plan);
  }

  let physical_plan = ctx.create_physical_plan(&logical_plan).unwrap();

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
