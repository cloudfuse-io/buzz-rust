use buzz::catalog::StaticCatalog;
use buzz::fuse_service::FuseService;
use buzz::hbee_scheduler::TestHBeeScheduler;
use buzz::hcomb_manager::TestHCombManager;
use buzz::hcomb_scheduler::HttpHCombScheduler;
use buzz::query::{BuzzQuery, BuzzStep, BuzzStepType, HCombCapacity};
use buzz::query_planner::QueryPlanner;

pub async fn start_fuse() -> Result<(), Box<dyn std::error::Error>> {
    // a fuse is all about the right delay, otherwise everything explodes to your face :)
    tokio::time::delay_for(std::time::Duration::new(1, 0)).await;

    let hbee_scheduler = TestHBeeScheduler {
        domain: "localhost".to_owned(),
    };
    let hcomb_manager = TestHCombManager {
        domain: "localhost".to_owned(),
    };
    let hcomb_scheduler = HttpHCombScheduler {};
    let query_planner = QueryPlanner::new();
    let mut service = FuseService::new(
        Box::new(hbee_scheduler),
        Box::new(hcomb_manager),
        Box::new(hcomb_scheduler),
        query_planner,
    );

    let catalog = StaticCatalog {};
    service.add_catalog(&catalog);

    service
        .run(BuzzQuery {
            steps: vec![
                BuzzStep {
                    sql: "SELECT payment_type, COUNT(payment_type) FROM nyc_taxi GROUP BY payment_type".to_owned(),
                    name: "nyc_taxi_map".to_owned(),
                    step_type: BuzzStepType::HBee,
                },
                BuzzStep {
                    sql: "SELECT payment_type, SUM(COUNT(payment_type)) FROM nyc_taxi_map GROUP BY payment_type".to_owned(),
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
    start_fuse().await
}
