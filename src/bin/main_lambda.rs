use datafusion::prelude::*;
use std::error::Error;
use std::sync::Arc;

use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_json::Value;

use buzz::bee_query;

fn main() -> Result<(), Box<dyn Error>> {
    lambda!(my_handler);
    Ok(())
}

fn my_handler(event: Value, _: Context) -> Result<Value, HandlerError> {
    println!("Input Event: {:?}", event);
    let conf = bee_query::QueryConfig {
        file_bucket: "cloudfuse-taxi-data".to_owned(),
        file_key: "raw_small/2009/01/data.parquet".to_owned(),
        file_length: 27301328,
        // file_key: "raw_5M/2009/01/data.parquet".to_owned(),
        // file_length: 388070114,
        ..Default::default()
    };
    let query = |df: Arc<dyn DataFrame>| {
        df.aggregate(vec![col("payment_type")], vec![count(col("payment_type"))])?
            .sort(vec![col("COUNT(payment_type)").sort(false, false)])
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(bee_query::run(conf, query))
        .unwrap();
    Ok(Value::String("Ok!".to_owned()))
}
