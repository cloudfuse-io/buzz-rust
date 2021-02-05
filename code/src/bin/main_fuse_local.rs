use buzz::example_catalog;
use buzz::services::fuse::{
    FuseService, HttpHCombScheduler, QueryPlanner, TestHBeeScheduler, TestHCombManager,
};

const QUERY: &'static str = r#"
{
    "steps": [
        {
            "sql": "SELECT payment_type, COUNT(payment_type) as payment_type_count FROM nyc_taxi GROUP BY payment_type",
            "name": "nyc_taxi_map",
            "step_type": "HBee",
            "partition_filter": "month='2009/01'"
        },
        {
            "sql": "SELECT payment_type, SUM(payment_type_count) FROM nyc_taxi_map GROUP BY payment_type",
            "name": "nyc_taxi_reduce",
            "step_type": "HComb"
        }
    ],
    "capacity": {
        "zones": 1
    }
}
"#;

pub async fn start_fuse(
    hbee_addr: &str,
    hcomb_addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // a fuse is all about the right delay, otherwise everything explodes to your face :)
    tokio::time::sleep(std::time::Duration::new(1, 0)).await;

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

    service.add_catalog("nyc_taxi", example_catalog::nyc_taxi_cloudfuse_sample());

    let query = serde_json::from_str(QUERY)?;

    service.run(query).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start_fuse("hbee", "hcomb").await
}
