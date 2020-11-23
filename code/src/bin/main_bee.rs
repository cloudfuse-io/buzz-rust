use buzz::bee_query::BeeQueryRunner;
use buzz::catalog::StaticCatalog;
use buzz::flight_client;
use buzz::query_planner::QueryPlanner;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = StaticCatalog::new();
    let planner = QueryPlanner::new(Box::new(catalog));
    let (_, bee_queries) = planner.plan("payment_type".to_owned())?;
    let batch = BeeQueryRunner::new()
        .run(bee_queries.queries().next().unwrap())
        .await?;
    flight_client::call_do_put(batch).await?;
    Ok(())
}
