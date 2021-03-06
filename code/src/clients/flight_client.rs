use std::error::Error;
use std::pin::Pin;

use crate::datasource::HCombTableDesc;
use crate::flight_utils;
use crate::models::{actions, HCombAddress};
use crate::serde;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::Stream;
use futures::StreamExt;
use prost::Message;

/// Calls the hcomb do_get endpoint, expecting the first message to be the schema
pub async fn call_do_get(
    address: &HCombAddress,
    hcomb_table: &HCombTableDesc,
    sql: String,
    source: String,
) -> Result<Pin<Box<dyn Stream<Item = ArrowResult<RecordBatch>>>>, Box<dyn Error>> {
    // Create Flight client
    let mut client = FlightServiceClient::connect(address.clone()).await?;

    let proto_plan = serde::serialize_hcomb(hcomb_table, sql, source);

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

pub async fn call_fail_action(
    query_id: String,
    address: &HCombAddress,
    reason: String,
) -> Result<(), Box<dyn Error>> {
    let action = arrow_flight::Action {
        body: serde_json::to_vec(&actions::Fail { query_id, reason }).unwrap(),
        r#type: actions::ActionType::Fail.to_string(),
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

pub async fn try_connect(address: &HCombAddress) -> Result<(), Box<dyn Error>> {
    FlightServiceClient::connect(address.clone()).await?;
    Ok(())
}
