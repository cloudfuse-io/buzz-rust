use buzz::catalog::StaticCatalog;
use buzz::flight_client;
use buzz::hbee_query::{HBeeQuery, HBeeQueryRunner};
use buzz::query_planner::QueryPlanner;

// things that are happening on the hcomb
fn on_hcomb() -> Result<HBeeQuery, Box<dyn std::error::Error>> {
    let catalog = StaticCatalog::new();
    let planner = QueryPlanner::new(Box::new(catalog));
    let (_, hbee_queries) = planner.plan("payment_type".to_owned())?;
    Ok(hbee_queries.queries().next().unwrap())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hbee_query = on_hcomb()?;
    let batch = HBeeQueryRunner::new().run(hbee_query).await?;
    flight_client::call_do_put(batch).await?;
    Ok(())
}
