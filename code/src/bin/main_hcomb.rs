use buzz::services::hcomb::{FlightServiceImpl, HCombService};

pub async fn start_hcomb_server() -> Result<(), Box<dyn std::error::Error>> {
    let hcomb_service = HCombService::new();
    let flight_service = FlightServiceImpl::new(hcomb_service);
    let server_handle = flight_service.start().await;
    server_handle.await.unwrap();

    Ok(())
}

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start_hcomb_server().await
}
