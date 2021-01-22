use std::io::Cursor;

use crate::datasource::HBeeTableDesc;
use crate::error::Result;
use crate::internal_err;
use crate::models::HCombAddress;
use crate::protobuf;
use crate::serde as proto_serde;
use base64;
use prost::Message;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
/// Binary Base64 encoded representation of a logical plan
/// TODO serialize this as json instead of base64 proto
pub struct HBeePlanBytes {
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
    pub plan: HBeePlanBytes,
}

impl HBeePlanBytes {
    /// Serialize and encode the given logical plan
    pub fn try_new(
        table_desc: &HBeeTableDesc,
        sql: String,
        source: String,
    ) -> Result<Self> {
        let proto_plan = proto_serde::serialize_hbee(table_desc, sql, source);

        let mut buf = vec![];
        proto_plan
            .encode(&mut buf)
            .map_err(|_| internal_err!("Could convert proto to bytes"))?;

        Ok(Self {
            bytes: base64::encode(&buf),
        })
    }

    pub fn parse(&self) -> Result<(HBeeTableDesc, String, String)> {
        let buf = base64::decode(&self.bytes)
            .map_err(|_| internal_err!("Could convert parse Base64"))?;
        let proto_plan = protobuf::HBeeScanNode::decode(&mut Cursor::new(buf))
            .map_err(|_| internal_err!("Could convert bytes to proto"))?;
        proto_serde::deserialize_hbee(proto_plan)
    }
}
