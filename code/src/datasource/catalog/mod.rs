use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use crate::datasource::HBeeTable;
use crate::error::{BuzzError, Result as BuzzResult};
use arrow::datatypes::*;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_plan::Expr;
use datafusion::optimizer::utils::expr_to_column_names;
use datafusion::physical_plan::ExecutionPlan;

/// A specific type of TableProvider that cannot be converted to a physical plan
/// but can be splitted to be distributed to hbees
pub trait SplittableTable {
    fn split(&self, partition_filters: &[Expr]) -> Vec<HBeeTable>;
    /// Get the names of the partitioning columns, in order of evaluation.
    fn partition_columns(&self) -> &[String];
    fn schema(&self) -> SchemaRef;
    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
    fn file_table(&self) -> Arc<dyn TableProvider>;
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
    pub fn split(&self, partition_filters: &[Expr]) -> Vec<HBeeTable> {
        self.source_table.split(partition_filters)
    }

    /// Split expressions to (regular_exprs, parition_exprs)
    pub fn extract_partition_exprs(
        &self,
        exprs: Vec<Expr>,
    ) -> BuzzResult<(Vec<Expr>, Vec<Expr>)> {
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
