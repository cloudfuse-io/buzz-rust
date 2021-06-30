//! Test Fixtures for Catalog Tables

use super::*;
use crate::datasource::{S3ParquetTable, SplittableTable};
use crate::models::SizedFile;
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::{MemTable, TableProvider};

/// `pattern_vec(pattern, len)` creates a vector of String of length `len`
macro_rules! pattern_vec {
    ($x:expr, $y:expr) => {
        (1..=$y).map(|i| format!($x, i)).collect::<Vec<String>>()
    };
}

/// A SplittableTable that splits into `nb_split` S3Parquet tables
pub struct MockSplittableTable {
    nb_split: usize,
    partitions: Vec<String>,
}

impl MockSplittableTable {
    /// File keys range from file_1 to file_{nb_split}
    /// Partition keys range from part_key_1 to part_key_{partition_keys}
    /// For each partition key, values range from part_value_001 to part_value_{nb_split}
    pub fn new(nb_split: usize, partition_keys: usize) -> Self {
        Self {
            nb_split,
            partitions: pattern_vec!("part_key_{}", partition_keys),
        }
    }
}

#[async_trait]
impl SplittableTable for MockSplittableTable {
    fn split(&self, files: Vec<SizedFile>) -> Vec<HBeeTableDesc> {
        files
            .into_iter()
            .map(|file| {
                S3ParquetTable::new(
                    "north-pole-1".to_owned(),
                    "santas-bucket".to_owned(),
                    vec![file],
                    test_schema(),
                )
            })
            .collect::<Vec<_>>()
    }
    fn partition_columns(&self) -> &[String] {
        &self.partitions
    }
    fn schema(&self) -> SchemaRef {
        let mut fields = test_schema().fields().clone();
        for partition_col in &self.partitions {
            fields.push(Field::new(partition_col, DataType::Utf8, false))
        }
        Arc::new(Schema::new_with_metadata(
            fields,
            test_schema().metadata().clone(),
        ))
    }
    async fn file_table(&self) -> Arc<dyn TableProvider + Send + Sync> {
        let mut fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("length", DataType::UInt64, false),
        ];
        for partition in &self.partitions {
            fields.push(Field::new(partition, DataType::Utf8, false));
        }

        let file_table_schema = Arc::new(Schema::new(fields));

        let keys = pattern_vec!("file_{}", self.nb_split);
        let lengths = vec![999999999 as u64; self.nb_split];
        let parts =
            vec![pattern_vec!("part_value_{:03}", self.nb_split); self.partitions.len()];

        let mut arrays = vec![
            Arc::new(StringArray::from(refvec(&keys))) as ArrayRef,
            Arc::new(UInt64Array::from(lengths)) as ArrayRef,
        ];
        for i in 1..=parts.len() {
            arrays.push(Arc::new(StringArray::from(refvec(&parts[i - 1]))) as ArrayRef);
        }

        let batches = RecordBatch::try_new(Arc::clone(&file_table_schema), arrays)
            .expect("Invalid test data");

        Arc::new(
            MemTable::try_new(file_table_schema, vec![vec![batches]])
                .expect("invalid test table"),
        )
    }
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "data_col",
        DataType::Int64,
        true,
    )]))
}

fn refvec(vec: &Vec<String>) -> Vec<Option<&str>> {
    vec.iter()
        .map(|item| Some(item.as_ref()))
        .collect::<Vec<_>>()
}
