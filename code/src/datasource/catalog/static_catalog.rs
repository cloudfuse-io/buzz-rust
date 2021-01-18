use std::collections::HashMap;
use std::sync::Arc;

use super::{CatalogTable, SplittableTable};
use crate::datasource::{HBeeTable, S3ParquetTable};
use crate::models::SizedFile;
use arrow::datatypes::*;
use datafusion::datasource::MemTable;
use datafusion::logical_plan::Expr;

pub struct CatalogFile {
    sized_file: SizedFile,
    partitions: HashMap<String, String>,
}

impl CatalogFile {
    pub fn new(key: &str, length: u64, partitions: &[(&str, &str)]) -> Self {
        CatalogFile {
            sized_file: SizedFile {
                key: key.to_owned(),
                length,
            },
            partitions: partitions
                .iter()
                .map(|kv| (kv.0.to_owned(), kv.1.to_owned()))
                .collect(),
        }
    }
}

/// A catalog table that contains a static list of files.
/// Only supports S3 parquet files for now and simply sends each file into a different hbee.
pub struct StaticCatalogTable {
    schema: SchemaRef,
    region: String,
    bucket: String,
    file_table: MemTable,
    // files: Vec<CatalogFile>,
    // partition_cols: Vec<String>,
}

impl StaticCatalogTable {
    pub fn new(
        schema: SchemaRef,
        region: String,
        bucket: String,
        files: Vec<CatalogFile>,
    ) -> CatalogTable {
        let mut file_iter = files.iter();
        let partition_cols = file_iter
            .next()
            .expect("Static catalog cannot have 0 file")
            .partitions
            .keys()
            .map(|key| key.to_owned())
            .collect::<Vec<_>>();
        file_iter.all(|catalog_file| {
            catalog_file
                .partitions
                .keys()
                .map(|key| key.to_owned())
                .collect::<Vec<_>>()
                == partition_cols
        });
        CatalogTable::new(Box::new(Self {
            schema,
            region,
            bucket,
            files,
            partition_cols,
        }))
    }
}

impl SplittableTable for StaticCatalogTable {
    fn split(&self, partition_filters: &[Expr]) -> Vec<HBeeTable> {
        self.files
            .iter()
            .map(|file| {
                S3ParquetTable::new(
                    self.region.clone(),
                    self.bucket.clone(),
                    vec![file.sized_file.clone()],
                    Arc::clone(&self.schema),
                )
            })
            .collect::<Vec<_>>()
    }
    fn partition_columns(&self) -> &[String] {
        &self.partition_cols
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
