use buzz::services::fuse::{
    FuseService, HttpHCombScheduler, QueryPlanner, TestHBeeScheduler, TestHCombManager,
};
use std::fs::read_to_string;

pub async fn start_fuse(
    hbee_addr: &str,
    hcomb_addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // a fuse is all about the right delay, otherwise everything explodes to your face :)
    tokio::time::sleep(std::time::Duration::new(1, 0)).await;

    let query_string = read_to_string("./examples/query-static-sample.json")?;

    let hbee_scheduler = TestHBeeScheduler {
        domain: hbee_addr.to_owned(),
    };
    let hcomb_manager = TestHCombManager {
        domain: hcomb_addr.to_owned(),
    };
    let hcomb_scheduler = HttpHCombScheduler {};
    let query_planner = QueryPlanner::new();
    let mut service = FuseService::new(
        Box::new(hbee_scheduler),
        Box::new(hcomb_manager),
        Box::new(hcomb_scheduler),
        query_planner,
    );

    let query = serde_json::from_str(&query_string)?;

    service.run(query).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start_fuse("hbee", "hcomb").await
}
