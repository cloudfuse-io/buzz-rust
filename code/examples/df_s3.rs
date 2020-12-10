use buzz::catalog::StaticCatalog;
use buzz::hbee_query::HBeeQueryRunner;
use buzz::query_planner::QueryPlanner;

fn main() {
    let catalog = StaticCatalog::new();
    let planner = QueryPlanner::new(Box::new(catalog));
    let (_, hbee_queries) = planner.plan("payment_type".to_owned()).unwrap();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(HBeeQueryRunner::new().run(hbee_queries.queries().next().unwrap()))
        .unwrap();
}
