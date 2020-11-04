use std::error::Error;

use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_json::Value;

mod execution_plan;
mod s3;
mod table_provider;
mod test_query;

fn main() -> Result<(), Box<dyn Error>> {
  lambda!(my_handler);
  Ok(())
}

fn my_handler(event: Value, _: Context) -> Result<Value, HandlerError> {
  println!("{:?}", event);
  tokio::runtime::Runtime::new()
    .unwrap()
    .block_on(test_query::run())
    .unwrap();
  Ok(Value::String("Ok!".to_owned()))
}
