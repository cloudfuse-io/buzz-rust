use std::sync::Arc;

use datafusion::prelude::*;

use buzz::flight_service::FlightServiceImpl;
use buzz::hive_query::{self, ResultsService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let result_service = Arc::new(ResultsService::new());
    let flight_service = FlightServiceImpl::new(Arc::clone(&result_service));
    let server_handle = flight_service.start().await;

    let res_stream = result_service.new_query("test0".to_owned(), 2);

    use arrow::datatypes::Schema;
    use arrow::datatypes::{DataType, Field};
    let schema = Arc::new(Schema::new(vec![
        Field::new("payment_type", DataType::Utf8, false),
        Field::new("COUNT(payment_type)", DataType::UInt64, true),
    ]));
    let config = hive_query::QueryConfig {
        concurrency: 1,
        batch_size: 2048,
        stream: res_stream,
        schema: schema,
    };
    let query = |df: Arc<dyn DataFrame>| {
        // Ok(df)
        // df.select(vec![col("payment_type"), col("COUNT(payment_type)")])
        // df.aggregate(vec![col("payment_type")], vec![sum(col("COUNT(payment_type)"))])
        df.aggregate(
            vec![col("payment_type")],
            vec![sum(col("COUNT(payment_type)"))],
        )?
        .sort(vec![col("SUM(COUNT(payment_type))").sort(false, false)])
    };
    hive_query::run(config, query).await.unwrap();

    server_handle.await.unwrap();

    Ok(())
}
