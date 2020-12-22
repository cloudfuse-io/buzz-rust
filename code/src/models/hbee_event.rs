use std::convert::TryInto;
use std::io::Cursor;

use crate::error::Result;
use crate::internal_err;
use crate::models::HCombAddress;
use crate::protobuf::LogicalPlanNode;
use base64;
use datafusion::logical_plan::LogicalPlan;
use prost::Message;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
/// Binary Base64 encoded representation of a logical plan
/// TODO find serialization with less copies
pub struct LogicalPlanBytes {
    #[serde(rename = "b")]
    bytes: String,
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
