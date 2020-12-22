//! These serialization / deserialization methods allow the exchange of DataFusion logical plans between services

pub mod from_proto;
pub mod to_proto;

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use std::sync::Arc;

    use crate::datasource::{HBeeTable, S3ParquetTable};
    use crate::models::SizedFile;
    use crate::protobuf;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::execution::context::ExecutionContext;
    use datafusion::logical_plan::LogicalPlan;
    use datafusion::logical_plan::{col, count};

    #[test]
    fn roundtrip() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let parquet_table = mock_table();

        let source_df = ExecutionContext::new().read_table(Arc::new(parquet_table))?;

        let source_plan = source_df.to_logical_plan();

        let proto: protobuf::LogicalPlanNode = (&source_plan).try_into()?;

        let transfered_plan: LogicalPlan = (&proto).try_into()?;

        assert_eq!(
            format!("{:?}", source_plan),
            format!("{:?}", transfered_plan)
        );

        Ok(())
    }

    #[test]
    fn roundtrip_aggregate() -> Result<(), Box<dyn std::error::Error>> {
        let parquet_table = mock_table();

        let source_df = ExecutionContext::new()
            .read_table(Arc::new(parquet_table))?
            .aggregate(vec![col("state")], vec![count(col("state"))])?;

        let source_plan = source_df.to_logical_plan();

        let proto: protobuf::LogicalPlanNode = (&source_plan).try_into()?;

        let transfered_plan: LogicalPlan = (&proto).try_into()?;

        assert_eq!(
            format!("{:?}", source_plan),
            format!("{:?}", transfered_plan)
        );

        Ok(())
    }

    fn mock_table() -> HBeeTable {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Float64, false),
            Field::new(
                "last_login",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]);

        S3ParquetTable::new(
            "south-pole-1".to_owned(),
            "santa".to_owned(),
            vec![SizedFile {
                key: "gift1".to_owned(),
                length: 1,
            }],
            Arc::new(schema),
        )
    }
}
