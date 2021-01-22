use std::sync::Arc;

use crate::datasource::{HBeeTableDesc, HCombTableDesc, S3ParquetTable};
use crate::error::Result;
use crate::internal_err;
use crate::models::SizedFile;
use crate::protobuf;
use arrow::ipc::convert;

pub fn deserialize_hbee(
    message: protobuf::HBeeScanNode,
) -> Result<(HBeeTableDesc, String, String)> {
    let schema = convert::schema_from_bytes(&message.schema)?;
    let scan = message
        .scan
        .ok_or(internal_err!("Scan field cannot be empty"))?;
    let provider = match scan {
        protobuf::h_bee_scan_node::Scan::S3Parquet(scan_node) => S3ParquetTable::new(
            scan_node.region.to_owned(),
            scan_node.bucket.to_owned(),
            scan_node
                .files
                .iter()
                .map(|sized_file| SizedFile {
                    key: sized_file.key.to_owned(),
                    length: sized_file.length,
                })
                .collect(),
            Arc::new(schema),
        ),
    };

    Ok((provider, message.sql, message.source))
}

pub fn deserialize_hcomb(
    message: protobuf::HCombScanNode,
) -> Result<(HCombTableDesc, String, String)> {
    let schema = convert::schema_from_bytes(&message.schema)?;
    let provider = HCombTableDesc::new(
        message.query_id.to_owned(),
        message.nb_hbee as usize,
        Arc::new(schema),
    );
    Ok((provider, message.sql, message.source))
}
