use datafusion::prelude::*;
use std::sync::Arc;

mod bee_query;
mod execution_plan;
mod s3;
mod table_provider;

fn main() {
  let conf = bee_query::QueryConfig {
    // file_bucket: "bb-test-data-dev".to_owned(),
    // file_key: "bid-large.parquet".to_owned(),
    // file_length: 218890209,
    file_bucket: "bb-test-data-dev".to_owned(),
    file_key: "bid-small.parquet".to_owned(),
    file_length: 2418624,
    ..Default::default()
  };
  let query = |df: Arc<dyn DataFrame>| {
    df.aggregate(vec![col("device")], vec![count(col("device"))])?
      .sort(vec![col("COUNT(device)").sort(false, false)])
  };
  let result = tokio::runtime::Runtime::new()
    .unwrap()
    .block_on(bee_query::run(conf, query));
  println!("Result:{:?}", result);
}
