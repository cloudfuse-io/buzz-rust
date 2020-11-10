use std::sync::Arc;

use tonic::transport::Server;

use arrow_flight::flight_service_server::FlightServiceServer;

mod flight_service;
mod hive_query;

/// This example shows how to wrap DataFusion with `FlightService` to support looking up schema information for
/// Parquet files and executing SQL queries against them on a remote server.
/// This example is run along-side the example `flight_client`.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let service = flight_service::FlightServiceImpl {
        result_service: Arc::new(hive_query::IntermediateResults::new()),
    };

    let svc = FlightServiceServer::new(service);

    println!("Listening on {:?}", addr);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
