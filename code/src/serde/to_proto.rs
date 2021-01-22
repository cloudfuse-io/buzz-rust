use crate::datasource::{HBeeTableDesc, HCombTableDesc};
use crate::protobuf;
use arrow::datatypes::Schema;
use arrow::ipc::{writer, writer::EncodedData, writer::IpcWriteOptions};

fn serialize_schema(schema: &Schema) -> EncodedData {
    let options = IpcWriteOptions::default();
    let data_gen = writer::IpcDataGenerator::default();
    data_gen.schema_to_bytes(schema, &options)
}

pub fn serialize_hbee(
    hbee_table: &HBeeTableDesc,
    sql: String,
    source: String,
) -> protobuf::HBeeScanNode {
    let schema = serialize_schema(&hbee_table.schema());
    let scan = match hbee_table {
        HBeeTableDesc::S3Parquet(table) => Some(
            protobuf::h_bee_scan_node::Scan::S3Parquet(protobuf::S3ParquetScanNode {
                region: table.region().to_owned(),
                bucket: table.bucket().to_owned(),
                files: table
                    .files()
                    .iter()
                    .map(|sized_file| protobuf::SizedFile {
                        key: sized_file.key.to_owned(),
                        length: sized_file.length,
                    })
                    .collect(),
            }),
        ),
    };
    protobuf::HBeeScanNode {
        scan,
        sql,
        schema: schema.ipc_message,
        source,
    }
}

pub fn serialize_hcomb(
    hcomb_table: &HCombTableDesc,
    sql: String,
    source: String,
) -> protobuf::HCombScanNode {
    let schema = serialize_schema(&hcomb_table.schema());
    protobuf::HCombScanNode {
        query_id: hcomb_table.query_id().to_owned(),
        nb_hbee: hcomb_table.nb_hbee() as u32,
        schema: schema.ipc_message,
        sql,
        source,
    }
}
