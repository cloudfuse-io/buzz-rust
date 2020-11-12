use arrow::util::pretty;
use datafusion::error::Result;
use datafusion::prelude::*;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    let debug = true;
    let start = Instant::now();

    let config = ExecutionConfig::new()
        .with_concurrency(1)
        .with_batch_size(2048);

    let mut ctx = ExecutionContext::with_config(config);

    let df = ctx
        .read_parquet("./bid-large.parquet")?
        .select(vec![col("device")])?
        .aggregate(vec![col("device")], vec![count(col("device"))])?
        .sort(vec![col("COUNT(device)").sort(false, false)])?;
    //.select(vec![col("device"), count(col("device"))])?;

    let plan = df.to_logical_plan();
    if debug {
        println!("=> Original logical plan:\n{:?}", plan);
    }
    let plan = ctx.optimize(&plan)?;
    if debug {
        println!("=> Optimized logical plan:\n{:?}", plan);
    }
    let physical_plan = ctx.create_physical_plan(&plan).unwrap();
    let setup_duration = start.elapsed().as_millis();
    let result = ctx.collect(physical_plan).await?;
    if debug {
        pretty::print_batches(&result)?;
        println!("Setup took {} ms", setup_duration);
        println!("Query took {} ms", start.elapsed().as_millis());
    }
    Ok(())
}

// #[tokio::main]
// async fn main() -> Result<()> {
//   let debug = true;
//   let sql =
//     "SELECT device, COUNT(device) as group_count FROM bids GROUP BY device ORDER BY group_count DESC";
//   let start = Instant::now();

//   let config = ExecutionConfig::new()
//     .with_concurrency(1)
//     .with_batch_size(2048);
//   let mut ctx = ExecutionContext::with_config(config);
//   ctx.register_parquet("bids", "./bid-large.parquet")?;
//   let plan = ctx.create_logical_plan(sql)?;
//   let plan = ctx.optimize(&plan)?;
//   if debug {
//     println!("Optimized logical plan:\n{:?}", plan);
//   }
//   let physical_plan = ctx.create_physical_plan(&plan)?;
//   let setup_duration = start.elapsed().as_millis();
//   let result = ctx.collect(physical_plan).await?;
//   if debug {
//     pretty::print_batches(&result)?;
//     println!("Setup took {} ms", setup_duration);
//     println!("Query took {} ms", start.elapsed().as_millis());
//   }
//   Ok(())
// }
