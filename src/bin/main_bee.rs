use arrow::record_batch::RecordBatch;
use arrow_flight::flight_descriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor};
use buzz::bee_query::BeeQueryRunner;
use buzz::catalog::StaticCatalog;
use buzz::query_planner::QueryPlanner;
use futures::StreamExt;

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

    // wait for the response to be complete but don't do anything with it
    client
        .do_put(request)
        .await?
        .into_inner()
        .collect::<Vec<_>>()
        .await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = StaticCatalog::new();
    let planner = QueryPlanner::new(Box::new(catalog));
    let (_, mut bee_queries) = planner.plan("payment_type".to_owned())?;
    let batch = BeeQueryRunner::new()
        .run(bee_queries.pop().unwrap())
        .await?;
    call_do_put(batch).await?;
    Ok(())
}
