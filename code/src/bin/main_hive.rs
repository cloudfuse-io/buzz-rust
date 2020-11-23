use std::sync::Arc;

use buzz::catalog::StaticCatalog;
use buzz::flight_service::FlightServiceImpl;
use buzz::hive_query::HiveQueryRunner;
use buzz::query_planner::QueryPlanner;
use buzz::results_service::ResultsService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let results_service = Arc::new(ResultsService::new());
    let query_runner = HiveQueryRunner::new(Arc::clone(&results_service));
    let flight_service = FlightServiceImpl::new(Arc::clone(&results_service));
    let server_handle = flight_service.start().await;
    let catalog = StaticCatalog::new();
    let planner = QueryPlanner::new(Box::new(catalog));

    let (hive_query, _) = planner.plan("payment_type".to_owned())?;

    query_runner.run(hive_query).await.unwrap();

    server_handle.await.unwrap();

    Ok(())
}
