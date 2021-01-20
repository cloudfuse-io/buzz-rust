use std::any::Any;
use std::sync::{Arc, Mutex};

use super::HBeeTable;
use crate::clients::s3;
use crate::clients::CachedFile;
use crate::clients::RangeCache;
use crate::execution_plan::ParquetExec;
use crate::models::SizedFile;
use arrow::datatypes::*;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;

/// Table-based representation of a `ParquetFile` backed by S3.
pub struct S3ParquetTable {
    region: String,
    bucket: String,
    files: Vec<SizedFile>,
    schema: SchemaRef,
    cache: Mutex<Option<Arc<RangeCache>>>,
}

impl S3ParquetTable {
    /// Initialize a new `ParquetTable` from a list of s3 files and an expected schema.
    pub fn new(
        region: String,
        bucket: String,
        files: Vec<SizedFile>,
        schema: SchemaRef,
    ) -> HBeeTable {
        HBeeTable::S3Parquet(Self {
            schema,
            region,
            bucket,
            files,
            cache: Mutex::new(None),
        })
    }

    pub fn set_cache(&self, cache: Arc<RangeCache>) {
        self.cache.lock().unwrap().replace(cache);
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn files(&self) -> &[SizedFile] {
        &self.files
    }
}

impl TableProvider for S3ParquetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let cache_guard = self.cache.lock().unwrap();
        let cache =
            cache_guard
                .as_ref()
                .ok_or(datafusion::error::DataFusionError::Plan(
                    "Download should be started before execution".to_owned(),
                ))?;
        let s3_files = self
            .files
            .iter()
            .map(|file| {
                let (dler_id, dler_creator) = s3::downloader_creator(&self.region);
                let file_id = s3::file_id(&self.bucket, &file.key);
                CachedFile::new(
                    file_id,
                    file.length,
                    Arc::clone(&cache),
                    dler_id,
                    dler_creator,
                )
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(ParquetExec::new(
            s3_files,
            projection.clone(),
            batch_size,
            Arc::clone(&self.schema),
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
