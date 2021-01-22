use std::sync::Arc;

use super::{CatalogTable, SplittableTable};
use crate::datasource::{HBeeTableDesc, S3ParquetTable};
use crate::error::Result;
use crate::models::SizedFile;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, TableProvider};

pub struct CatalogFile {
    sized_file: SizedFile,
    partitions: Vec<String>,
}

impl CatalogFile {
    pub fn new(key: &str, length: u64, partitions: Vec<String>) -> Self {
        CatalogFile {
            sized_file: SizedFile {
                key: key.to_owned(),
                length,
            },
            partitions,
        }
    }
}

/// A catalog table that contains a static list of files.
/// Only supports S3 parquet files for now and simply sends each file into a different hbee.
pub struct StaticCatalogTable {
    schema: SchemaRef,
    region: String,
    bucket: String,
    files: Vec<CatalogFile>,
    partition_cols: Vec<String>,
}

impl StaticCatalogTable {
    pub fn new(
        schema: SchemaRef,
        region: String,
        bucket: String,
        partition_cols: Vec<String>,
        files: Vec<CatalogFile>,
    ) -> CatalogTable {
        CatalogTable::new(Box::new(Self {
            schema,
            region,
            bucket,
            files,
            partition_cols,
        }))
    }

    fn to_table(&self) -> Result<Arc<dyn TableProvider + Send + Sync>> {
        let mut key_builder = StringBuilder::new(self.files.len());
        let mut length_builder = UInt64Builder::new(self.files.len());
        let mut partition_builders = self
            .partition_cols
            .iter()
            .map(|_| StringBuilder::new(self.files.len()))
            .collect::<Vec<_>>();
        for catalog_file in &self.files {
            assert_eq!(catalog_file.partitions.len(), self.partition_cols.len(),"Each catalog entry should have as many partition values as partition cols.");
            key_builder.append_value(&catalog_file.sized_file.key)?;
            length_builder.append_value(catalog_file.sized_file.length)?;
            for (i, part_val) in catalog_file.partitions.iter().enumerate() {
                partition_builders[i].append_value(part_val)?;
            }
        }

        // finish all builders
        let mut col_arrays: Vec<ArrayRef> = vec![
            ArrayBuilder::finish(&mut key_builder),
            ArrayBuilder::finish(&mut length_builder),
        ];
        for mut partition_builder in partition_builders {
            col_arrays.push(ArrayBuilder::finish(&mut partition_builder));
        }

        // build schema
        let mut fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("length", DataType::UInt64, false),
        ];
        for col in &self.partition_cols {
            fields.push(Field::new(col, DataType::Utf8, false));
        }
        let schema = Arc::new(Schema::new(fields));

        let record_batch = RecordBatch::try_new(Arc::clone(&schema), col_arrays)?;
        Ok(Arc::new(MemTable::try_new(
            schema,
            vec![vec![record_batch]],
        )?))
    }
}

impl SplittableTable for StaticCatalogTable {
    fn split(&self, files: Vec<SizedFile>) -> Vec<HBeeTableDesc> {
        files
            .into_iter()
            .map(|file| {
                S3ParquetTable::new(
                    self.region.clone(),
                    self.bucket.clone(),
                    vec![file],
                    Arc::clone(&self.schema),
                )
            })
            .collect()
    }
    fn partition_columns(&self) -> &[String] {
        &self.partition_cols
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn file_table(&self) -> Arc<dyn TableProvider + Send + Sync> {
        self.to_table().unwrap()
    }
}
