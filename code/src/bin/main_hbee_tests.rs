use std::error::Error;

use buzz::example_catalog;
use buzz::models::query::{BuzzStep, BuzzStepType};
use buzz::services::fuse::QueryPlanner;
use buzz::services::hbee::{HBeeService, NoopCollector};
use datafusion::logical_plan::LogicalPlan;
use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_json::Value;

const QUERY_ID: &str = "test_query";

async fn new_plan() -> Result<LogicalPlan, Box<dyn Error>> {
    let mut query_planner = QueryPlanner::new();
    query_planner.add_catalog("nyc_taxi", example_catalog::nyc_taxi_ursa_large());
    let steps = vec![
        BuzzStep {
            sql: "SELECT payment_type, COUNT(payment_type) as payment_type_count, SUM(fare_amount) as fare_amount_sum FROM nyc_taxi GROUP BY payment_type".to_owned(),
            name: "nyc_taxi_map".to_owned(),
            step_type: BuzzStepType::HBee,
        },
        BuzzStep {
            sql: "SELECT payment_type, SUM(payment_type_count), SUM(fare_amount_sum) FROM nyc_taxi_map GROUP BY payment_type".to_owned(),
            name: "nyc_taxi_reduce".to_owned(),
            step_type: BuzzStepType::HComb,
        },
    ];
    query_planner
        .plan(QUERY_ID.to_owned(), steps, 1)
        .await
        .map(|dist_plan| dist_plan.zones[0].hbee[0].clone())
        .map_err(|e| Box::new(e).into())
}

async fn exec() -> Result<(), Box<dyn Error>> {
    let plan: LogicalPlan = new_plan().await?;
    let collector = Box::new(NoopCollector {});
    let hbee_service = HBeeService::new(collector).await;
    hbee_service
        .execute_query(QUERY_ID.to_owned(), plan, "http://mock_endpoint".to_owned())
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
        .block_on(exec())
        .unwrap();
    Ok(Value::String("Ok!".to_owned()))
}
