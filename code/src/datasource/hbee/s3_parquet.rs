use std::sync::Arc;

use super::HBeeTableDesc;
use crate::clients::s3;
use crate::clients::CachedFile;
use crate::clients::RangeCache;
use crate::execution_plan::ParquetExec;
use crate::models::SizedFile;
use arrow::datatypes::*;
use datafusion::error::Result;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;

/// Table-based representation of a `ParquetFile` backed by S3.
#[derive(Debug)]
pub struct S3ParquetTable {
    region: String,
    bucket: String,
    files: Vec<SizedFile>,
    schema: SchemaRef,
}

impl S3ParquetTable {
    /// Initialize a new `ParquetTable` from a list of s3 files and an expected schema.
    pub fn new(
        region: String,
        bucket: String,
        files: Vec<SizedFile>,
        schema: SchemaRef,
    ) -> HBeeTableDesc {
        HBeeTableDesc::S3Parquet(Self {
            schema,
            region,
            bucket,
            files,
        })
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

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn scan(
        &self,
        cache: Arc<RangeCache>,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
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
}
