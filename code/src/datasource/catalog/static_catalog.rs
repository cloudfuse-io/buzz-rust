use std::sync::Arc;

use super::{CatalogTable, SplittableTable};
use crate::datasource::{HBeeTable, S3ParquetTable};
use crate::models::SizedFile;
use arrow::datatypes::*;
use datafusion::datasource::datasource::Statistics;

/// A catalog table that contains a static list of files.
/// Only supports S3 parquet files for now and simply sends each file into a different hbee.
pub struct StaticCatalogTable {
    schema: SchemaRef,
    region: String,
    bucket: String,
    files: Vec<SizedFile>,
}

impl StaticCatalogTable {
    pub fn new(
        schema: SchemaRef,
        region: String,
        bucket: String,
        files: Vec<SizedFile>,
    ) -> CatalogTable {
        CatalogTable::new(Box::new(Self {
            schema,
            region,
            bucket,
            files,
        }))
    }
}

impl SplittableTable for StaticCatalogTable {
    fn split(&self) -> Vec<HBeeTable> {
        self.files
            .iter()
            .map(|file| -> HBeeTable {
                HBeeTable::new_s3_parquet(S3ParquetTable::new(
                    self.region.clone(),
                    self.bucket.clone(),
                    vec![file.clone()],
                    Arc::clone(&self.schema),
                ))
            })
            .collect::<Vec<_>>()
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
