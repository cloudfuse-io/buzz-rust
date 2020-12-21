use std::any::Any;
use std::sync::{Arc, Mutex};

use crate::datasource::catalog::SizedFile;
use crate::execution_plan::ParquetExec;
use crate::s3::{self, S3FileAsync};
use arrow::datatypes::*;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::{FuturesOrdered, StreamExt};

/// Table-based representation of a `ParquetFile` backed by S3.
pub struct S3ParquetTable {
    region: String,
    bucket: String,
    files: Vec<SizedFile>,
    schema: SchemaRef,
    downloads: Mutex<Option<Vec<S3FileAsync>>>,
}

impl S3ParquetTable {
    /// Initialize a new `ParquetTable` from a list of s3 files and an expected schema.
    pub fn new(
        region: String,
        bucket: String,
        files: Vec<SizedFile>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            schema,
            region,
            bucket,
            files,
            downloads: Mutex::new(None),
        }
    }

    pub async fn start_download(&self) {
        println!("[hbee] start_download");
        // TODO mutualize client even further?
        let client = s3::new_client(&self.region);

        let downloads = self
            .files
            .iter()
            .map(|sized_file| {
                // TODO better coordinate download scheduling
                s3::S3FileAsync::new(
                    self.bucket.clone(),
                    sized_file.key.clone(),
                    sized_file.length,
                    Arc::clone(&client),
                )
            })
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<_>>()
            .await;
        *(self.downloads.lock().unwrap()) = Some(downloads);
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let downloads = self.downloads.lock().unwrap();
        let files =
            downloads
                .as_ref()
                .ok_or(datafusion::error::DataFusionError::Plan(
                    "Download should be started before execution".to_owned(),
                ))?;
        Ok(Arc::new(ParquetExec::try_new(
            files.clone(),
            projection.clone(),
            batch_size,
            Arc::clone(&self.schema),
        )?))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
