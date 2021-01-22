//! These serialization / deserialization methods allow the exchange of DataFusion logical plans between services

mod from_proto;
mod to_proto;

pub use from_proto::*;
pub use to_proto::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::datasource::{HCombTableDesc, S3ParquetTable};
    use crate::models::SizedFile;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    #[test]
    fn roundtrip_parquet() {
        let parquet_table = S3ParquetTable::new(
            "south-pole-1".to_owned(),
            "santa".to_owned(),
            vec![SizedFile {
                key: "gift1".to_owned(),
                length: 1,
            }],
            Arc::new(test_schema()),
        );
        let sql = "SELECT * FROM swag";
        let source = "swag";

        let proto =
            to_proto::serialize_hbee(&parquet_table, sql.to_owned(), source.to_owned());

        let (transfered_table, transfered_sql, transfered_source) =
            from_proto::deserialize_hbee(proto).unwrap();

        assert_eq!(sql, transfered_sql);
        assert_eq!(source, transfered_source);
        assert_eq!(
            format!("{:?}", parquet_table),
            format!("{:?}", transfered_table)
        );
    }

    #[test]
    fn roundtrip_hcomb() {
        let hcomb_table =
            HCombTableDesc::new("test_query_id".to_owned(), 16, Arc::new(test_schema()));
        let sql = "SELECT * FROM swag";
        let source = "swag";

        let proto =
            to_proto::serialize_hcomb(&hcomb_table, sql.to_owned(), source.to_owned());

        let (transfered_table, transfered_sql, transfered_source) =
            from_proto::deserialize_hcomb(proto).unwrap();

        assert_eq!(sql, transfered_sql);
        assert_eq!(source, transfered_source);
        assert_eq!(
            format!("{:?}", hcomb_table),
            format!("{:?}", transfered_table)
        );
    }

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Float64, false),
            Field::new(
                "last_login",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ])
    }
}
