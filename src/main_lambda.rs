use datafusion::prelude::*;
use std::error::Error;
use std::sync::Arc;

use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_json::Value;

mod bee_query;
mod execution_plan;
mod s3;
mod table_provider;

fn main() -> Result<(), Box<dyn Error>> {
    lambda!(my_handler);
    Ok(())
}

fn my_handler(event: Value, _: Context) -> Result<Value, HandlerError> {
    println!("Input Event: {:?}", event);
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
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(bee_query::run(conf, query))
        .unwrap();
    Ok(Value::String("Ok!".to_owned()))
}
