use buzz::example_catalog;
use buzz::models::query::{BuzzQuery, BuzzStep, BuzzStepType, HCombCapacity};
use buzz::services::fuse::{
    FuseService, HttpHCombScheduler, QueryPlanner, TestHBeeScheduler, TestHCombManager,
};

pub async fn start_fuse(
    hbee_addr: &str,
    hcomb_addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // a fuse is all about the right delay, otherwise everything explodes to your face :)
    tokio::time::delay_for(std::time::Duration::new(1, 0)).await;

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

    service.add_catalog("nyc_taxi", example_catalog::nyc_taxi_small());

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start_fuse("hbee", "hcomb").await
}
