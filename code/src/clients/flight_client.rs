use std::convert::TryInto;
use std::error::Error;
use std::pin::Pin;

use crate::flight_utils;
use crate::models::HCombAddress;
use crate::protobuf::LogicalPlanNode;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use datafusion::logical_plan::LogicalPlan;
use futures::Stream;
use futures::StreamExt;
use prost::Message;

/// Calls the hcomb do_get endpoint, expecting the first message to be the schema
pub async fn call_do_get(
    address: &HCombAddress,
    plan: LogicalPlan,
) -> Result<Pin<Box<dyn Stream<Item = ArrowResult<RecordBatch>>>>, Box<dyn Error>> {
    // Create Flight client
    let mut client = FlightServiceClient::connect(address.clone()).await?;

    let proto_plan: LogicalPlanNode = (&plan).try_into()?;

    let mut buf = vec![];
    proto_plan.encode(&mut buf)?;

    // Call do_get to execute a SQL query and receive results
    let request = tonic::Request::new(Ticket { ticket: buf });
    let stream = client.do_get(request).await?.into_inner();
    let (_, record_batch_stream) = flight_utils::flight_to_batches(stream).await?;
    Ok(Box::pin(record_batch_stream))
}

pub async fn call_do_put(
    query_id: String,
    address: &HCombAddress,
    results: Vec<RecordBatch>,
) -> Result<(), Box<dyn Error>> {
    // Create Flight client after delay, to leave time for the server to boot
    tokio::time::delay_for(std::time::Duration::new(1, 0)).await;

    let input = flight_utils::batch_vec_to_flight(&query_id, results).await?;

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

pub async fn call_do_action(
    query_id: String,
    address: &HCombAddress,
    action_type: String,
) -> Result<(), Box<dyn Error>> {
    let action = arrow_flight::Action {
        body: query_id.as_bytes().to_owned(),
        r#type: action_type,
    };
    let request = tonic::Request::new(action);

    let mut client = FlightServiceClient::connect(address.clone()).await?;
    // wait for the response to be complete but don't do anything with it
    client
        .do_action(request)
        .await?
        .into_inner()
        .collect::<Vec<_>>()
        .await;

    Ok(())
}
