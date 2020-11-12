use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use futures::StreamExt;

use arrow_flight::flight_descriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor};

async fn call_do_put(
    results: Vec<RecordBatch>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create Flight client after delay, to leave time for the server to boot
    tokio::time::delay_for(std::time::Duration::new(1, 0)).await;
    let mut client = FlightServiceClient::connect("http://hive:50051").await?;

    // add an initial FlightData message that sends schema
    let schema = results[0].schema();
    let mut flight_schema = FlightData::from(schema.as_ref());
    flight_schema.flight_descriptor = Some(FlightDescriptor {
        r#type: flight_descriptor::DescriptorType::Cmd as i32,
        cmd: "hello".as_bytes().to_owned(),
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

    let put_result = client.do_put(request).await?.into_inner();

    println!("put_result: {:?}", put_result.collect::<Vec<_>>());

    Ok(())
}

fn mock_data() -> Result<RecordBatch, arrow::error::ArrowError> {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    // call_do_get().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));

    // define data.
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let batch = mock_data()?;
    call_do_put(vec![batch]).await?;
    Ok(())
}
