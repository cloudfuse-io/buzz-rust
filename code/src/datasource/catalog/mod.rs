use std::any::Any;
use std::sync::Arc;

use crate::datasource::HBeeTableDesc;
use crate::error::{BuzzError, Result};
use crate::models::SizedFile;
use arrow::array::*;
use arrow::datatypes::*;
use async_trait::async_trait;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;

/// A specific type of TableProvider that cannot be converted to a physical plan
/// but can be splitted to be distributed to hbees
#[async_trait]
pub trait SplittableTable {
    fn split(&self, files: Vec<SizedFile>) -> Vec<HBeeTableDesc>;
    /// Get the names of the partitioning columns, in order of evaluation.
    fn partition_columns(&self) -> &[String];
    /// schema including the partition columns
    fn schema(&self) -> SchemaRef;
    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
    async fn file_table(&self) -> Arc<dyn TableProvider + Send + Sync>;
}

/// A generic catalog table that wraps splittable tables
pub struct CatalogTable {
    source_table: Box<dyn SplittableTable + Send + Sync>,
}

impl CatalogTable {
    pub fn new(source_table: Box<dyn SplittableTable + Send + Sync>) -> Self {
        Self { source_table }
    }

    /// Explore the catalog with the given `partition_filter` and generate the tables to be processed by each hbee.
    pub async fn split(
        &self,
        partition_filters: &Option<String>,
    ) -> Result<Vec<HBeeTableDesc>> {
        let files = self.filter_catalog(partition_filters).await?;
        Ok(self.source_table.split(files))
    }

    /// Applies the given filters
    async fn filter_catalog(
        &self,
        partition_filters: &Option<String>,
    ) -> Result<Vec<SizedFile>> {
        let phys_plan;
        {
            let mut context = ExecutionContext::new();
            context.register_table("catalog", self.source_table.file_table().await);
            let sql_pattern = "SELECT * FROM catalog";
            let sql_statement = match partition_filters {
                Some(sql_where) => format!("{} WHERE {}", sql_pattern, sql_where),
                None => sql_pattern.to_owned(),
            };
            let df = context.sql(&sql_statement)?;
            phys_plan = context.create_physical_plan(&df.to_logical_plan())?;
        }

        let file_rec = datafusion::physical_plan::collect(phys_plan).await?;

        file_rec
            .iter()
            .map(|rec_batch| {
                let key_array = rec_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or(BuzzError::Execution(format!(
                        "Invalid type for catalog keys"
                    )))?;
                let length_array = rec_batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or(BuzzError::Execution(format!(
                        "Invalid type for catalog lengths"
                    )))?;
                let sized_files = (0..rec_batch.num_rows()).map(move |i| SizedFile {
                    key: key_array.value(i).to_owned(),
                    length: length_array.value(i),
                });
                Ok(sized_files)
            })
            .flat_map(|rec_iter_res| match rec_iter_res {
                Ok(rec_iter) => rec_iter.map(|rec| Ok(rec)).collect(),
                Err(er) => vec![Err(er)],
            })
            .collect::<Result<Vec<_>>>()
    }
}

impl TableProvider for CatalogTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.source_table.schema()
    }

    fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Catalog table cannot generate an execution plan".to_owned(),
        ))
    }

    fn statistics(&self) -> Statistics {
        self.source_table.statistics()
    }
}

pub fn catalog_schema(partition_names: &[String]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("length", DataType::UInt64, false),
    ];
    for col in partition_names {
        fields.push(Field::new(col, DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

//// Implems ////

pub mod delta_catalog;
pub mod static_catalog;
pub(crate) mod test_catalog;
// pub(crate) mod utils;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::CatalogTable;

    #[tokio::test]
    async fn test_filter_catalog() {
        let nb_split = 5;
        let catalog_table = CatalogTable::new(Box::new(
            test_catalog::MockSplittableTable::new(nb_split, 0),
        ));

        let result = catalog_table.filter_catalog(&None).await.unwrap();
        assert_eq!(result.len(), 5);
    }

    #[tokio::test]
    async fn test_filter_partitioned_catalog() {
        let nb_split = 5;
        let catalog_table = CatalogTable::new(Box::new(
            test_catalog::MockSplittableTable::new(nb_split, 1),
        ));

        let result = catalog_table.filter_catalog(&None).await.unwrap();
        assert_eq!(result.len(), 5);

        let result = catalog_table
            .filter_catalog(&Some("part_key_1='part_value_002'".to_owned()))
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, "file_2");
    }
}
