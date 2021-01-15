use std::error::Error;

use buzz::error::{BuzzError, Result as BuzzResult};
use buzz::example_catalog;
use buzz::services::fuse::{
    FargateHCombManager, FuseService, HttpHCombScheduler, LambdaHBeeScheduler,
    QueryPlanner,
};
use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_json::Value;

pub async fn start_fuse(event: Value) -> BuzzResult<()> {
    let hbee_scheduler = LambdaHBeeScheduler::try_new()?;
    let hcomb_manager = FargateHCombManager::try_new()?;
    let hcomb_scheduler = HttpHCombScheduler {};
    let query_planner = QueryPlanner::new();
    let mut service = FuseService::new(
        Box::new(hbee_scheduler),
        Box::new(hcomb_manager),
        Box::new(hcomb_scheduler),
        query_planner,
    );

    service.add_catalog(
        "nyc_taxi_ursa_small",
        example_catalog::nyc_taxi_ursa_small(),
    );
    service.add_catalog(
        "nyc_taxi_ursa_large",
        example_catalog::nyc_taxi_ursa_large(),
    );
    service.add_catalog(
        "nyc_taxi_cloudfuse_sample",
        example_catalog::nyc_taxi_cloudfuse_sample(),
    );
    service.add_catalog(
        "nyc_taxi_cloudfuse_full",
        example_catalog::nyc_taxi_cloudfuse_full(),
    );

    println!("[fuse] initialized, starting query...");

    let query = serde_json::from_value(event)
        .map_err(|e| BuzzError::BadRequest(format!("{}", e)))?;

    service.run(query).await?;
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
        .block_on(start_fuse(event))
        .unwrap();
    Ok(Value::String("Ok!".to_owned()))
}
