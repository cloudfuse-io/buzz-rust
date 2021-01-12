use std::error::Error;

use buzz::error::Result as BuzzResult;
use buzz::example_catalog;
use buzz::models::query::{BuzzQuery, BuzzStep, BuzzStepType, HCombCapacity};
use buzz::services::fuse::{
    FargateHCombManager, FuseService, HttpHCombScheduler, LambdaHBeeScheduler,
    QueryPlanner,
};
use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_json::Value;

pub async fn start_fuse() -> BuzzResult<()> {
    let hbee_scheduler = LambdaHBeeScheduler::try_new("eu-west-1")?;
    let hcomb_manager = FargateHCombManager::new("eu-west-1");
    let hcomb_scheduler = HttpHCombScheduler {};
    let query_planner = QueryPlanner::new();
    let mut service = FuseService::new(
        Box::new(hbee_scheduler),
        Box::new(hcomb_manager),
        Box::new(hcomb_scheduler),
        query_planner,
    );

    service.add_catalog("nyc_taxi", example_catalog::nyc_taxi_small());

    println!("[fuse] initialized, starting query...");

    service
        .run(BuzzQuery {
            steps: vec![
                BuzzStep {
                    sql: "SELECT payment_type, COUNT(payment_type) as payment_type_count FROM nyc_taxi GROUP BY payment_type".to_owned(),
                    name: "nyc_taxi_map".to_owned(),
                    step_type: BuzzStepType::HBee,
                },
                BuzzStep {
                    sql: "SELECT payment_type, SUM(payment_type_count) FROM nyc_taxi_map GROUP BY payment_type".to_owned(),
                    name: "nyc_taxi_reduce".to_owned(),
                    step_type: BuzzStepType::HComb,
                },
            ],
            capacity: HCombCapacity {
                ram: 1,
                cores: 1,
                zones: 1,
            },
        })
        .await?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    lambda!(my_handler);
    Ok(())
}

fn my_handler(event: Value, _: Context) -> Result<Value, HandlerError> {
    println!("Input Event: {:?}", event);
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(start_fuse())
        .unwrap();
    Ok(Value::String("Ok!".to_owned()))
}
