use std::sync::Arc;
use tonic::transport::Server;

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::prelude::*;

use buzz::flight_service;
use buzz::hive_query;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let result_service = Arc::new(hive_query::IntermediateResults::new());

    let addr = "0.0.0.0:50051".parse()?;
    let service = flight_service::FlightServiceImpl {
        result_service: Arc::clone(&result_service),
    };

    let svc = FlightServiceServer::new(service);

    let server_handle = tokio::spawn(async move {
        println!("Listening on {:?}", addr);
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .unwrap();
    });

    let res_stream = result_service.new_query("test0".to_owned(), 2);

    use arrow::datatypes::Schema;
    use arrow::datatypes::{DataType, Field};
    let schema = Arc::new(Schema::new(vec![
        Field::new("device", DataType::Utf8, false),
        Field::new("COUNT(device)", DataType::UInt64, true),
    ]));
    let config = hive_query::QueryConfig {
        concurrency: 1,
        batch_size: 2048,
        stream: res_stream,
        schema: schema,
    };
    let query = |df: Arc<dyn DataFrame>| {
        // Ok(df)
        // df.select(vec![col("device"), col("COUNT(device)")])
        // df.aggregate(vec![col("device")], vec![sum(col("COUNT(device)"))])
        df.aggregate(vec![col("device")], vec![sum(col("COUNT(device)"))])?
            .sort(vec![col("SUM(COUNT(device))").sort(false, false)])
    };
    hive_query::run(config, query).await.unwrap();

    server_handle.await.unwrap();

    Ok(())
}
