use std::convert::TryInto;
use std::io::Cursor;

use crate::error::Result;
use crate::hcomb_manager::HCombAddress;
use crate::internal_err;
use crate::protobuf::LogicalPlanNode;
use async_trait::async_trait;
use base64;
use datafusion::logical_plan::LogicalPlan;
use hyper::{Body, Client, Request};
use prost::Message;
use serde::{Deserialize, Serialize};
// use tokio::io::{self, AsyncWriteExt as _};

#[async_trait]
pub trait HBeeScheduler {
    async fn schedule(
        &self,
        query_id: String,
        address: &HCombAddress,
        plan: LogicalPlan,
    ) -> Result<()>;
}

pub struct TestHBeeScheduler {
    pub domain: String,
}

#[derive(Serialize, Deserialize)]
/// Binary Base64 encoded representation of a logical plan
/// TODO find serialization with less copies
pub struct LogicalPlanBytes {
    #[serde(rename = "b")]
    bytes: String,
}

impl LogicalPlanBytes {
    /// Serialize and encode the given logical plan
    pub fn try_new(plan: &LogicalPlan) -> Result<Self> {
        let proto_plan: LogicalPlanNode = plan.try_into()?;

        let mut buf = vec![];
        proto_plan
            .encode(&mut buf)
            .map_err(|_| internal_err!("Could convert proto to bytes"))?;

        Ok(Self {
            bytes: base64::encode(&buf),
        })
    }

    pub fn parse(&self) -> Result<LogicalPlan> {
        let buf = base64::decode(&self.bytes)
            .map_err(|_| internal_err!("Could convert parse Base64"))?;
        let proto_plan = LogicalPlanNode::decode(&mut Cursor::new(buf))
            .map_err(|_| internal_err!("Could convert bytes to proto"))?;
        (&proto_plan).try_into()
    }
}

#[derive(Serialize, Deserialize)]
pub struct HBeeEvent {
    #[serde(rename = "id")]
    pub query_id: String,
    #[serde(rename = "a")]
    pub hcomb_address: HCombAddress,
    #[serde(rename = "p")]
    pub plan: LogicalPlanBytes,
}

#[async_trait]
impl HBeeScheduler for TestHBeeScheduler {
    async fn schedule(
        &self,
        query_id: String,
        address: &HCombAddress,
        plan: LogicalPlan,
    ) -> Result<()> {
        let client = Client::new();

        let req_body = serde_json::to_string(&HBeeEvent {
            query_id,
            hcomb_address: address.clone(),
            plan: LogicalPlanBytes::try_new(&plan)?,
        })
        .map_err(|_| internal_err!("failed to serialize to json"))?;

        let req = Request::builder()
            .method("POST")
            .uri(format!("http://{}:3000", self.domain))
            .body(Body::from(req_body))
            .map_err(|_| internal_err!("failed to build hbee request"))?;

        let res = client
            .request(req)
            .await
            .map_err(|e| internal_err!("hbee scheduling failed: {}", e))?;

        println!("Response status from hbee: {}", res.status());

        Ok(())
    }
}

// TODO implementations:
// TODO - for lambd
