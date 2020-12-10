use std::vec::IntoIter;

use arrow::record_batch::RecordBatch;
use arrow_flight::flight_descriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor};
use futures::stream::Iter;
use futures::StreamExt;

pub async fn call_do_put(
    results: Vec<RecordBatch>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create Flight client after delay, to leave time for the server to boot
    tokio::time::delay_for(std::time::Duration::new(1, 0)).await;

    // add an initial FlightData message that sends schema
    let schema = results[0].schema();
    let mut flight_schema = FlightData::from(schema.as_ref());
    flight_schema.flight_descriptor = Some(FlightDescriptor {
        r#type: flight_descriptor::DescriptorType::Cmd as i32,
        cmd: "test0".as_bytes().to_owned(),
        path: vec![],
    });
    let mut flights: Vec<FlightData> = vec![flight_schema];

    let mut batches: Vec<FlightData> = results
        .iter()
        .map(|batch| FlightData::from(batch))
        .collect();

    // append batch vector to schema vector, so that the first message sent is the schema
    flights.append(&mut batches);

    let input = futures::stream::iter(flights);

    let request = tonic::Request::new(input);

    do_put(request).await?;

    Ok(())
}

async fn do_put(
    request: tonic::Request<Iter<IntoIter<FlightData>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlightServiceClient::connect("http://hcomb:50051").await?;
    // wait for the response to be complete but don't do anything with it
    client
        .do_put(request)
        .await?
        .into_inner()
        .collect::<Vec<_>>()
        .await;
    Ok(())
}
