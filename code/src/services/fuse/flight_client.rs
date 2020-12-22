use std::convert::TryInto;
use std::error::Error;
use std::pin::Pin;

use crate::models::HCombAddress;
use crate::protobuf::LogicalPlanNode;
use crate::services::flight_utils;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use datafusion::logical_plan::LogicalPlan;
use futures::Stream;
use prost::Message;

/// Calls the hcomb do_get endpoint, expecting the first message to be the schema
pub async fn call_do_get(
    address: &HCombAddress,
    plan: LogicalPlan,
) -> Result<Pin<Box<dyn Stream<Item = RecordBatch>>>, Box<dyn Error>> {
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
