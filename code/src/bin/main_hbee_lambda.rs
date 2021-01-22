use std::error::Error;

use buzz::models::HBeeEvent;
use buzz::services::hbee::{HBeeService, HttpCollector};
use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_json::Value;

async fn exec(event: Value) -> Result<(), Box<dyn Error>> {
    let hbee_event: HBeeEvent = serde_json::from_value(event)?;
    let (hbee_table_desc, sql, source) = hbee_event.plan.parse()?;
    let collector = Box::new(HttpCollector {});
    let mut hbee_service = HBeeService::new(collector).await;
    hbee_service
        .execute_query(
            hbee_event.query_id,
            hbee_table_desc,
            sql,
            source,
            hbee_event.hcomb_address,
        )
        .await
        .map_err(|e| Box::new(e).into())
}

fn main() -> Result<(), Box<dyn Error>> {
    lambda!(my_handler);
    Ok(())
}

fn my_handler(event: Value, _: Context) -> Result<Value, HandlerError> {
    println!("Input Event: {:?}", event);
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(exec(event))
        .unwrap();
    Ok(Value::String("Ok!".to_owned()))
}
