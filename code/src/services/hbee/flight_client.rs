use std::error::Error;

use crate::models::HCombAddress;
use crate::services::flight_utils;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

pub async fn call_do_put(
    query_id: String,
    address: &HCombAddress,
    results: SendableRecordBatchStream,
) -> Result<(), Box<dyn Error>> {
    // Create Flight client after delay, to leave time for the server to boot
    tokio::time::delay_for(std::time::Duration::new(1, 0)).await;

    let input = flight_utils::batches_to_flight(&query_id, results).await?;

    let request = tonic::Request::new(input);

    let mut client = FlightServiceClient::connect(address.clone()).await?;
    // wait for the response to be complete but don't do anything with it
    client
        .do_put(request)
        .await?
        .into_inner()
        .collect::<Vec<_>>()
        .await;

    Ok(())
}
