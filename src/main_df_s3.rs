mod execution_plan;
mod s3;
mod table_provider;
mod test_query;

fn main() {
  let result = tokio::runtime::Runtime::new()
    .unwrap()
    .block_on(test_query::run());
  println!("Result:{:?}", result);
}
