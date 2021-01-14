use std::sync::Arc;

use crate::datasource::{CatalogTable, StaticCatalogTable};
use crate::models::SizedFile;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// shortened nyc taxi, hosted by cloudfuse
pub fn nyc_taxi_cloudfuse_sample() -> CatalogTable {
    StaticCatalogTable::new(
        nyc_taxi_v1_schema(TimeUnit::Microsecond),
        "us-east-2".to_owned(),
        "cloudfuse-taxi-data".to_owned(),
        vec![SizedFile {
            key: "raw_small/2009/01/data.parquet".to_owned(),
            length: 27301328,
        }],
    )
}

/// complete nyc taxi files with 5M rows per rowgroups, hosted by cloudfuse
pub fn nyc_taxi_cloudfuse_full() -> CatalogTable {
    StaticCatalogTable::new(
        nyc_taxi_v1_schema(TimeUnit::Microsecond),
        "us-east-2".to_owned(),
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

/// A single file of the nyc taxi parquet data hosted by Ursa Labs
/// Note that some nyc parquet files hosted by Ursa Labs have many small row groups which is inefficient
pub fn nyc_taxi_ursa_small() -> CatalogTable {
    StaticCatalogTable::new(
        nyc_taxi_v1_schema(TimeUnit::Nanosecond),
        "us-east-2".to_owned(),
        "ursa-labs-taxi-data".to_owned(),
        vec![SizedFile {
            key: "2009/01/data.parquet".to_owned(),
            length: 461966527,
        }],
    )
}

/// A subset of the nyc taxi parquet files hosted by Ursa Labs
/// Note that some nyc parquet files hosted by Ursa Labs have many small row groups which is inefficient
pub fn nyc_taxi_ursa_large() -> CatalogTable {
    StaticCatalogTable::new(
        nyc_taxi_v1_schema(TimeUnit::Nanosecond),
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
            SizedFile {
                key: "2009/06/data.parquet".to_owned(),
                length: 465578495,
            },
            SizedFile {
                key: "2009/07/data.parquet".to_owned(),
                length: 448227037,
            },
            SizedFile {
                key: "2009/08/data.parquet".to_owned(),
                length: 450774566,
            },
            SizedFile {
                key: "2009/09/data.parquet".to_owned(),
                length: 460835784,
            },
            SizedFile {
                key: "2009/10/data.parquet".to_owned(),
                length: 517609313,
            },
            SizedFile {
                key: "2009/11/data.parquet".to_owned(),
                length: 471148697,
            },
            SizedFile {
                key: "2009/12/data.parquet".to_owned(),
                length: 479899902,
            },
        ],
    )
}

/// schema found in earlier nyc taxi files (e.g 2009)
fn nyc_taxi_v1_schema(time_unit: TimeUnit) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("vendor_id", DataType::Utf8, true),
        Field::new(
            "pickup_at",
            DataType::Timestamp(time_unit.clone(), Option::None),
            true,
        ),
        Field::new(
            "dropoff_at",
            DataType::Timestamp(time_unit.clone(), Option::None),
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

// /// schema found in latest nyc taxi files (e.g. 2019)
// fn nyc_taxi_v2_schema() -> Arc<Schema> {
//     Arc::new(Schema::new(vec![
//         Field::new("vendor_id", DataType::Utf8, true),
//         Field::new(
//             "pickup_at",
//             DataType::Timestamp(TimeUnit::Microsecond, Option::None),
//             true,
//         ),
//         Field::new(
//             "dropoff_at",
//             DataType::Timestamp(TimeUnit::Microsecond, Option::None),
//             true,
//         ),
//         Field::new("passenger_count", DataType::Int8, true),
//         Field::new("trip_distance", DataType::Float32, true),
//         Field::new("rate_code_id", DataType::Utf8, true),
//         Field::new("store_and_fwd_flag", DataType::Utf8, true),
//         Field::new("pickup_location_id", DataType::Int32, true),
//         Field::new("dropoff_location_id", DataType::Int32, true),
//         Field::new("payment_type", DataType::Utf8, true),
//         Field::new("fare_amount", DataType::Float32, true),
//         Field::new("extra", DataType::Float32, true),
//         Field::new("mta_tax", DataType::Float32, true),
//         Field::new("tip_amount", DataType::Float32, true),
//         Field::new("tolls_amount", DataType::Float32, true),
//         Field::new("improvement_surcharge", DataType::Float32, true),
//         Field::new("total_amount", DataType::Float32, true),
//         Field::new("congestion_surcharge", DataType::Float32, true),
//     ]))
// }
