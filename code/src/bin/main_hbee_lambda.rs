use std::error::Error;

use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_json::Value;

fn main() -> Result<(), Box<dyn Error>> {
    lambda!(my_handler);
    Ok(())
}

fn my_handler(event: Value, _: Context) -> Result<Value, HandlerError> {
    println!("Input Event: {:?}", event);
    Ok(Value::String("Ok!".to_owned()))
}
