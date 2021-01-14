use std::sync::Arc;

use crate::datasource::{CatalogTable, StaticCatalogTable};
use crate::models::SizedFile;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

pub fn nyc_taxi_small() -> CatalogTable {
    StaticCatalogTable::new(
        nyc_taxi_schema(),
        "eu-west-1".to_owned(),
        "cloudfuse-taxi-data".to_owned(),
        vec![
            SizedFile {
                key: "raw_small/2009/01/data.parquet".to_owned(),
                length: 27301328,
            },
            SizedFile {
                key: "raw_small/2009/01/data.parquet".to_owned(),
                length: 27301328,
            },
        ],
    )
}

pub fn nyc_taxi_large() -> CatalogTable {
    StaticCatalogTable::new(
        nyc_taxi_schema(),
        "eu-west-1".to_owned(),
        "cloudfuse-taxi-data".to_owned(),
        vec![
            SizedFile {
                key: "raw_5M/2009/01/data.parquet".to_owned(),
                length: 388070114,
            },
            SizedFile {
                key: "raw_5M/2009/02/data.parquet".to_owned(),
                length: 368127982,
            },
            SizedFile {
                key: "raw_5M/2009/03/data.parquet".to_owned(),
                length: 398600815,
            },
            SizedFile {
                key: "raw_5M/2009/04/data.parquet".to_owned(),
                length: 396353841,
            },
            SizedFile {
                key: "raw_5M/2009/05/data.parquet".to_owned(),
                length: 410283205,
            },
        ],
    )
}

pub fn nyc_taxi_ursa() -> CatalogTable {
    StaticCatalogTable::new(
        nyc_taxi_schema(),
        "us-east-2".to_owned(),
        "ursa-labs-taxi-data".to_owned(),
        vec![
            SizedFile {
                key: "2009/01/data.parquet".to_owned(),
                length: 461966527,
            },
            SizedFile {
                key: "2009/02/data.parquet".to_owned(),
                length: 436405669,
            },
            SizedFile {
                key: "2009/03/data.parquet".to_owned(),
                length: 474795751,
            },
            SizedFile {
                key: "2009/04/data.parquet".to_owned(),
                length: 470914229,
            },
            SizedFile {
                key: "2009/05/data.parquet".to_owned(),
                length: 489248585,
            },
        ],
    )
}

fn nyc_taxi_schema() -> Arc<Schema> {
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
