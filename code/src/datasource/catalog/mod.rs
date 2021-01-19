use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use crate::datasource::HBeeTable;
use crate::error::{BuzzError, Result};
use crate::models::SizedFile;
use crate::plan_utils;
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::Expr;
use datafusion::optimizer::utils::expr_to_column_names;
use datafusion::physical_plan::ExecutionPlan;

/// A specific type of TableProvider that cannot be converted to a physical plan
/// but can be splitted to be distributed to hbees
pub trait SplittableTable {
    fn split(&self, files: Vec<SizedFile>) -> Vec<HBeeTable>;
    /// Get the names of the partitioning columns, in order of evaluation.
    fn partition_columns(&self) -> &[String];
    fn schema(&self) -> SchemaRef;
    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
    fn file_table(&self) -> Arc<dyn TableProvider + Send + Sync>;
}

/// A generic catalog table that wraps splittable tables
pub struct CatalogTable {
    source_table: Box<dyn SplittableTable + Send + Sync>,
}

impl CatalogTable {
    pub fn new(source_table: Box<dyn SplittableTable + Send + Sync>) -> Self {
        Self { source_table }
    }

    /// Explore the catalog with the given `partition_filter` and generate the tables
    /// To be processed by each hbee.
    pub async fn split(&self, partition_filters: &[Expr]) -> Result<Vec<HBeeTable>> {
        let files = self.filter_catalog(partition_filters).await?;
        Ok(self.source_table.split(files))
    }

    /// Split expressions to (regular_exprs, parition_exprs)
    pub fn extract_partition_exprs(
        &self,
        exprs: Vec<Expr>,
    ) -> Result<(Vec<Expr>, Vec<Expr>)> {
        let mut regular_exprs = vec![];
        let mut partition_exprs = vec![];
        let partition_cols = self
            .source_table
            .partition_columns()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        for expr in exprs {
            let mut cols_in_expr = HashSet::new();
            expr_to_column_names(&expr, &mut cols_in_expr)?;
            if cols_in_expr.is_disjoint(&partition_cols) {
                regular_exprs.push(expr);
            } else if partition_cols.is_superset(&cols_in_expr) {
                partition_exprs.push(expr);
            } else {
                return Err(BuzzError::Execution(
                    format!("Invalid expression for filtering: cannot use both partition and non partition columns: {:?}", &cols_in_expr)
                ));
            }
        }
        Ok((regular_exprs, partition_exprs))
    }

    /// Applies the given filters
    async fn filter_catalog(&self, partition_filters: &[Expr]) -> Result<Vec<SizedFile>> {
        let phys_plan;
        {
            let mut context = ExecutionContext::new();
            let mut df = context.read_table(self.source_table.file_table())?;
            if partition_filters.len() > 0 {
                let filter_expr = plan_utils::merge_expr(partition_filters);
                df = df.filter(filter_expr)?;
            }

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
        let mut fields = self.source_table.schema().fields().clone();
        for partition_col in self.source_table.partition_columns() {
            fields.push(Field::new(partition_col, DataType::Utf8, false))
        }
        Arc::new(Schema::new_with_metadata(
            fields,
            self.source_table.schema().metadata().clone(),
        ))
    }

    fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Catalog table cannot generate an execution plan".to_owned(),
        ))
    }

    fn statistics(&self) -> Statistics {
        self.source_table.statistics()
    }
}

//// Implems ////

pub mod static_catalog;
pub(crate) mod test_catalog;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::CatalogTable;
    use datafusion::logical_plan::{col, lit};

    #[tokio::test]
    async fn test_filter_catalog() {
        let nb_split = 5;
        let catalog_table = CatalogTable::new(Box::new(
            test_catalog::MockSplittableTable::new(nb_split, 0),
        ));

        let result = catalog_table.filter_catalog(&[lit(true)]).await.unwrap();
        assert_eq!(result.len(), 5);
    }

    #[tokio::test]
    async fn test_filter_partitioned_catalog() {
        let nb_split = 5;
        let catalog_table = CatalogTable::new(Box::new(
            test_catalog::MockSplittableTable::new(nb_split, 1),
        ));

        let result = catalog_table.filter_catalog(&[lit(true)]).await.unwrap();
        assert_eq!(result.len(), 5);

        let result = catalog_table
            .filter_catalog(&[col("part_key_1").eq(lit("part_value_002"))])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, "file_2");
    }
}
