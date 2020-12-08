use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::internal_err;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

#[derive(Clone)]
pub struct SizedFile {
    pub key: String,
    pub length: u64,
}

pub trait Catalog {
    fn get_schema(&self, name: &str) -> Result<Arc<Schema>>;
}

pub struct StaticCatalog {
    data: HashMap<String, Arc<Schema>>,
}

impl StaticCatalog {
    pub fn new() -> Self {
        let mut data = HashMap::new();
        data.insert("nyc-taxi".to_owned(), Self::nyc_taxi());
        Self { data }
    }

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

impl Catalog for StaticCatalog {
    fn get_schema(&self, name: &str) -> Result<Arc<Schema>> {
        Ok(Arc::clone(
            self.data
                .get(name)
                .ok_or(internal_err!("{} not found", name))?,
        ))
    }
}
