use buzz::bee_query::BeeQueryRunner;
use buzz::catalog::StaticCatalog;
use buzz::query_planner::QueryPlanner;

fn main() {
    let catalog = StaticCatalog::new();
    let planner = QueryPlanner::new(Box::new(catalog));
    let (_, bee_queries) = planner.plan("payment_type".to_owned()).unwrap();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(BeeQueryRunner::new().run(bee_queries.queries().next().unwrap()))
        .unwrap();
}
