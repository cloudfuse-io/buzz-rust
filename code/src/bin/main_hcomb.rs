use std::sync::Arc;

use buzz::catalog::StaticCatalog;
use buzz::flight_service::FlightServiceImpl;
use buzz::hcomb_query::HCombQueryRunner;
use buzz::query_planner::QueryPlanner;
use buzz::results_service::ResultsService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let results_service = Arc::new(ResultsService::new());
    let query_runner = HCombQueryRunner::new(Arc::clone(&results_service));
    let flight_service = FlightServiceImpl::new(Arc::clone(&results_service));
    let server_handle = flight_service.start().await;
    let catalog = StaticCatalog::new();
    let planner = QueryPlanner::new(Box::new(catalog));

    let (hcomb_query, _) = planner.plan("payment_type".to_owned())?;

    query_runner.run(hcomb_query).await.unwrap();

    server_handle.await.unwrap();

    Ok(())
}
