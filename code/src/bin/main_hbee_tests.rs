use std::error::Error;

use buzz::example_catalog;
use buzz::services::fuse::{HBeePlan, QueryPlanner};
use buzz::services::hbee::{HBeeService, NoopCollector};
use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_json::Value;

const QUERY_ID: &str = "test_query";

async fn new_plan(event: Value) -> Result<HBeePlan, Box<dyn Error>> {
    let mut qp = QueryPlanner::new();
    qp.add_catalog("nyc_taxi_ursa", example_catalog::nyc_taxi_ursa());
    qp.add_catalog("nyc_taxi_cloudfuse", example_catalog::nyc_taxi_cloudfuse());
    qp.add_catalog(
        "nyc_taxi_cloudfuse_sample",
        example_catalog::nyc_taxi_cloudfuse_sample(),
    );
    let steps = serde_json::from_value(event)?;
    qp.plan(QUERY_ID.to_owned(), steps, 1)
        .await
        .map(|dist_plan| {
            dist_plan
                .zones
                .into_iter()
                .next()
                .unwrap()
                .hbee
                .into_iter()
                .next()
                .unwrap()
        })
        .map_err(|e| Box::new(e).into())
}

async fn exec(event: Value) -> Result<(), Box<dyn Error>> {
    let plan = new_plan(event).await?;
    let collector = Box::new(NoopCollector {});
    let mut hbee_service = HBeeService::new(collector).await;
    hbee_service
        .execute_query(
            QUERY_ID.to_owned(),
            plan.table,
            plan.sql,
            plan.source,
            "http://mock_endpoint".to_owned(),
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
