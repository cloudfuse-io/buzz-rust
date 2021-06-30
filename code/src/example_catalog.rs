use std::sync::Arc;

use crate::datasource::{
    CatalogFile, CatalogTable, DeltaCatalogTable, StaticCatalogTable,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// shortened nyc taxi, hosted by cloudfuse
pub fn nyc_taxi_cloudfuse_sample() -> CatalogTable {
    CatalogTable::new(Box::new(StaticCatalogTable::new(
        nyc_taxi_v1_schema(TimeUnit::Microsecond),
        "us-east-2".to_owned(),
        "cloudfuse-taxi-data".to_owned(),
        vec!["month".to_owned()],
        vec![CatalogFile::new(
            "raw_small/2009/01/data.parquet",
            27301328,
            vec!["2009/01".to_owned()],
        )],
    )))
}

/// complete nyc taxi files with 5M rows per rowgroups, hosted by cloudfuse
pub fn nyc_taxi_cloudfuse() -> CatalogTable {
    CatalogTable::new(Box::new(StaticCatalogTable::new(
        nyc_taxi_v1_schema(TimeUnit::Microsecond),
        "us-east-2".to_owned(),
        "cloudfuse-taxi-data".to_owned(),
        vec!["month".to_owned()],
        vec![
            CatalogFile::new(
                "raw_5M/2009/01/data.parquet",
                388070114,
                vec!["2009/01".to_owned()],
            ),
            CatalogFile::new(
                "raw_5M/2009/02/data.parquet",
                368127982,
                vec!["2009/02".to_owned()],
            ),
            CatalogFile::new(
                "raw_5M/2009/03/data.parquet",
                398600815,
                vec!["2009/03".to_owned()],
            ),
            CatalogFile::new(
                "raw_5M/2009/04/data.parquet",
                396353841,
                vec!["2009/04".to_owned()],
            ),
            CatalogFile::new(
                "raw_5M/2009/05/data.parquet",
                410283205,
                vec!["2009/05".to_owned()],
            ),
        ],
    )))
}

/// A subset of the nyc taxi parquet files hosted by Ursa Labs
/// Note that some nyc parquet files hosted by Ursa Labs have many small row groups which is inefficient
pub fn nyc_taxi_ursa() -> CatalogTable {
    CatalogTable::new(Box::new(StaticCatalogTable::new(
        nyc_taxi_v1_schema(TimeUnit::Nanosecond),
        "us-east-2".to_owned(),
        "ursa-labs-taxi-data".to_owned(),
        vec!["month".to_owned()],
        vec![
            CatalogFile::new(
                "2009/01/data.parquet",
                461966527,
                vec!["2009/01".to_owned()],
            ),
            CatalogFile::new(
                "2009/02/data.parquet",
                436405669,
                vec!["2009/02".to_owned()],
            ),
            CatalogFile::new(
                "2009/03/data.parquet",
                474795751,
                vec!["2009/03".to_owned()],
            ),
            CatalogFile::new(
                "2009/04/data.parquet",
                470914229,
                vec!["2009/04".to_owned()],
            ),
            CatalogFile::new(
                "2009/05/data.parquet",
                489248585,
                vec!["2009/05".to_owned()],
            ),
            CatalogFile::new(
                "2009/06/data.parquet",
                465578495,
                vec!["2009/06".to_owned()],
            ),
            CatalogFile::new(
                "2009/07/data.parquet",
                448227037,
                vec!["2009/07".to_owned()],
            ),
            CatalogFile::new(
                "2009/08/data.parquet",
                450774566,
                vec!["2009/08".to_owned()],
            ),
            CatalogFile::new(
                "2009/09/data.parquet",
                460835784,
                vec!["2009/09".to_owned()],
            ),
            CatalogFile::new(
                "2009/10/data.parquet",
                517609313,
                vec!["2009/10".to_owned()],
            ),
            CatalogFile::new(
                "2009/11/data.parquet",
                471148697,
                vec!["2009/11".to_owned()],
            ),
            CatalogFile::new(
                "2009/12/data.parquet",
                479899902,
                vec!["2009/12".to_owned()],
            ),
        ],
    )))
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
