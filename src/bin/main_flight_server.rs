use std::sync::Arc;

use tonic::transport::Server;

use arrow_flight::flight_service_server::FlightServiceServer;

use buzz::flight_service;
use buzz::hive_query;

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
