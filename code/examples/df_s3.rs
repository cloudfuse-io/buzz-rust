// use std::iter::Iterator;
// use std::sync::Arc;
// use std::time::Instant;

// use arrow::datatypes::Schema;
// use arrow::record_batch::RecordBatch;
// use arrow::util::pretty;
// use buzz::catalog::SizedFile;
// use buzz::dataframe_ops::DataframeOperations;
// use buzz::datasource::{EmptyTable, ParquetTable};
// use buzz::error::Result;
// use datafusion::prelude::*;

fn main() {
    // let debug = true;
    // let catalog = buzz::catalog::StaticCatalog {};
    // let concurrency = 1;
    // let batch_size = 2048;

    // let mut start = Instant::now();
    // let config = ExecutionConfig::new()
    //     .with_concurrency(concurrency)
    //     .with_batch_size(batch_size);

    // let mut parquet_table = ParquetTable::new(
    //     query.region,
    //     query.file_bucket,
    //     query.files,
    //     query.input_schema,
    // );
    // parquet_table.start_download().await;

    // let mut ctx = ExecutionContext::with_config(config);
    // let df = query
    //     .ops
    //     .apply_to(ctx.read_table(Arc::new(parquet_table))?)?;
    // let logical_plan = df.to_logical_plan();
    // if debug {
    //     println!("=> Original logical plan:\n{:?}", logical_plan);
    // }
    // let logical_plan = ctx.optimize(&logical_plan)?;
    // if debug {
    //     println!("=> Optimized logical plan:\n{:?}", logical_plan);
    // }
    // let physical_plan = ctx.create_physical_plan(&logical_plan).unwrap();
    // if debug {
    //     // println!("=> Physical plan:\n{:?}", physical_plan);
    //     println!("=> Schema:\n{:?}", physical_plan.schema());
    // }
    // let setup_duration = start.elapsed().as_millis();
    // start = Instant::now();
    // let result = ctx.collect(physical_plan).await?;
    // if debug {
    //     pretty::print_batches(&result)?;
    //     println!("Setup took {} ms", setup_duration);
    //     println!("Processing took {} ms", start.elapsed().as_millis());
    // }
}
