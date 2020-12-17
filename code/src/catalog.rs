use std::sync::Arc;

use crate::datasource::StaticCatalogTable;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::execution::context::ExecutionContext;

#[derive(Clone)]
pub struct SizedFile {
    pub key: String,
    pub length: u64,
}

pub trait Catalog {
    fn fill(&self, context: &mut ExecutionContext);
}

pub struct StaticCatalog {}

impl Catalog for StaticCatalog {
    fn fill(&self, context: &mut ExecutionContext) {
        context.register_table(
            "nyc_taxi",
            Box::new(StaticCatalogTable::new(
                Self::nyc_taxi(),
                "eu-west-1".to_owned(),
                "cloudfuse-taxi-data".to_owned(),
                vec![],
            )),
        );
    }
}

impl StaticCatalog {
    fn nyc_taxi() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("vendor_id", DataType::Utf8, true),
            Field::new(
                "pickup_at",
                DataType::Timestamp(TimeUnit::Microsecond, Option::None),
                true,
            ),
            Field::new(
                "dropoff_at",
                DataType::Timestamp(TimeUnit::Microsecond, Option::None),
                true,
            ),
            Field::new("passenger_count", DataType::Int8, true),
            Field::new("trip_distance", DataType::Float32, true),
            Field::new("pickup_longitude", DataType::Float32, true),
            Field::new("pickup_latitude", DataType::Float32, true),
            Field::new("rate_code_id", DataType::Null, true),
            Field::new("store_and_fwd_flag", DataType::Utf8, true),
            Field::new("dropoff_longitude", DataType::Float32, true),
            Field::new("dropoff_latitude", DataType::Float32, true),
            Field::new("payment_type", DataType::Utf8, true),
            Field::new("fare_amount", DataType::Float32, true),
            Field::new("extra", DataType::Float32, true),
            Field::new("mta_tax", DataType::Float32, true),
            Field::new("tip_amount", DataType::Float32, true),
            Field::new("tolls_amount", DataType::Float32, true),
            Field::new("total_amount", DataType::Float32, true),
        ]))
    }
}
