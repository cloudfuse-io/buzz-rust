use std::sync::Arc;

use super::{catalog_schema, SplittableTable};
use crate::datasource::{HBeeTableDesc, S3ParquetTable};
use crate::error::Result;
use crate::models::SizedFile;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::{MemTable, TableProvider};
use deltalake::storage::{
    file::FileStorageBackend, s3::S3StorageBackend, StorageBackend,
};
use deltalake::{DeltaTable, Schema as DeltaSchema};
use rusoto_core::Region;
use rusoto_s3::S3Client;
use std::convert::{TryFrom, TryInto};
use std::str::FromStr;

/// A catalog table that gets values from delta-rs.
/// Only supports S3 parquet files for now and simply sends each file into a different hbee.
pub struct DeltaCatalogTable {
    region: String,
    bucket: String,
    root: String,
    delta_table: DeltaTable,
}

impl DeltaCatalogTable {
    /// Create a catalog from a Delta S3 URI and a region.
    pub async fn new(uri: &str, region: String) -> Self {
        let region_enum = Region::from_str(&region).unwrap();
        let storage_backend: Box<dyn StorageBackend> =
            Box::new(S3StorageBackend::new_with(S3Client::new(region_enum), None));
        let mut delta_table =
            DeltaTable::new(&uri, storage_backend).expect("TODO Handle Delta failure");
        delta_table.load().await.expect("TODO Handle Delta failure");
        let uri_split = uri.split("/").collect::<Vec<_>>();
        let bucket = uri_split[2].to_owned();
        let root = uri_split[3..].join("/") + "/";
        Self {
            delta_table,
            bucket,
            root,
            region,
        }
    }

    /// Create a catalog from a local Delta path.
    pub async fn new_local(path: &str) -> Self {
        let mut delta_table =
            DeltaTable::new(path, Box::new(FileStorageBackend::new(path)))
                .expect("DeltaTable from local storage created");
        delta_table
            .load()
            .await
            .expect("DeltaTable from local storage loaded");

        Self {
            delta_table,
            bucket: "local".to_owned(),
            root: path.to_owned() + "/",
            region: "local".to_owned(),
        }
    }

    async fn to_table(&self) -> Result<Arc<dyn TableProvider + Send + Sync>> {
        let actions = self.delta_table.get_actions();
        let mut key_builder = StringBuilder::new(actions.len());
        let mut length_builder = UInt64Builder::new(actions.len());
        let delta_metadata = self
            .delta_table
            .get_metadata()
            .expect("TODO Handle Delta failure");
        let mut partition_builders = delta_metadata
            .partition_columns
            .iter()
            .map(|_| StringBuilder::new(actions.len()))
            .collect::<Vec<_>>();
        for catalog_action in actions {
            key_builder.append_value(&(self.root.clone() + &catalog_action.path))?;
            length_builder.append_value(
                catalog_action
                    .size
                    .try_into()
                    .expect("TODO Handle Delta failure"),
            )?;
            for (i, part_col) in delta_metadata.partition_columns.iter().enumerate() {
                // TODO check whether partition value might be some kind of wildcard
                partition_builders[i]
                    .append_value(catalog_action.partition_values[part_col].clone())?;
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

        let schema = catalog_schema(&delta_metadata.partition_columns);

        let record_batch = RecordBatch::try_new(Arc::clone(&schema), col_arrays)?;
        Ok(Arc::new(MemTable::try_new(
            schema,
            vec![vec![record_batch]],
        )?))
    }
}

#[async_trait]
impl SplittableTable for DeltaCatalogTable {
    fn split(&self, files: Vec<SizedFile>) -> Vec<HBeeTableDesc> {
        files
            .into_iter()
            .map(|file| {
                S3ParquetTable::new(
                    self.region.clone(),
                    self.bucket.clone(),
                    vec![file],
                    self.schema(),
                )
            })
            .collect()
    }
    fn partition_columns(&self) -> &[String] {
        &self
            .delta_table
            .get_metadata()
            .expect("TODO Handle Delta failure")
            .partition_columns
    }
    fn schema(&self) -> SchemaRef {
        Arc::new(
            <Schema as TryFrom<&DeltaSchema>>::try_from(
                DeltaTable::schema(&self.delta_table).unwrap(),
            )
            .unwrap(),
        )
    }
    async fn file_table(&self) -> Arc<dyn TableProvider + Send + Sync> {
        self.to_table().await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple_delta_catalog() {
        let catalog =
            DeltaCatalogTable::new_local("./examples/delta-tbl-overwrite").await;
        let table = catalog.to_table().await.expect("Logical table created");
        let table_phys = table
            .scan(&None, 100, &[], None)
            .expect("Physical table created");
        let catalog_rgs = datafusion::physical_plan::collect(table_phys)
            .await
            .expect("Table collected");

        let expected_batches = vec![RecordBatch::try_new(
            catalog_schema(&[]),
            vec![
                Arc::new(StringArray::from(vec![
                    "./examples/delta-tbl-overwrite/part-00000-f4a247c9-a3bb-4b1e-adc7-7269808b8d73-c000.snappy.parquet",
                ])),
                Arc::new(UInt64Array::from(vec![1006])),
            ],
        )
        .expect("Build target RecordBatch")];

        assert_eq!(
            format!("{:?}", catalog_rgs),
            format!("{:?}", expected_batches)
        );
    }

    #[tokio::test]
    async fn test_partitioned_delta_catalog() {
        let catalog =
            DeltaCatalogTable::new_local("./examples/delta-tbl-partition").await;
        let table = catalog.to_table().await.expect("Logical table created");
        let table_phys = table
            .scan(&None, 100, &[], None)
            .expect("Physical table created");
        let catalog_rgs = datafusion::physical_plan::collect(table_phys)
            .await
            .expect("Table collected");

        let expected_batches = vec![RecordBatch::try_new(
            catalog_schema(&["year".to_owned()]),
            vec![
                Arc::new(StringArray::from(vec![
                    "./examples/delta-tbl-partition/year=2020/part-00000-755eb925-59ad-4167-b46b-db694e7f3b2c.c000.snappy.parquet",
                    "./examples/delta-tbl-partition/year=2021/part-00000-c22c8742-53c8-4ffa-b9c3-ba7705218ca2.c000.snappy.parquet",
                ])),
                Arc::new(UInt64Array::from(vec![744, 744])),
                Arc::new(StringArray::from(vec!["2020", "2021"])),
            ],
        )
        .expect("Build target RecordBatch")];

        assert_eq!(
            format!("{:?}", catalog_rgs),
            format!("{:?}", expected_batches)
        );
    }
}
