use crate::error::Result;
use crate::not_impl_err;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::LogicalPlan;

/// Search a TableProvider of the given type in the plan.
/// Only works with linear plans (only one datasource).
pub fn find_table_name<'a, T: TableProvider + 'static>(
    plan: &'a LogicalPlan,
) -> Result<&'a str> {
    let new_inputs = plan.inputs();
    if new_inputs.len() > 1 {
        Err(not_impl_err!(
            "Operations with more than one inputs are not supported",
        ))
    } else if new_inputs.len() == 1 {
        // recurse
        find_table_name::<T>(new_inputs[0])
    } else {
        if let Some(result_table) = as_table_name::<T>(&plan) {
            Ok(result_table)
        } else {
            Err(not_impl_err!(
                "Expected root to be a {}",
                std::any::type_name::<T>()
            ))
        }
    }
}

fn as_table_name<'a, T: TableProvider + 'static>(
    plan: &'a LogicalPlan,
) -> Option<&'a str> {
    if let LogicalPlan::TableScan {
        source, table_name, ..
    } = plan
    {
        source
            .as_any()
            .downcast_ref::<T>()
            .map(|_| table_name.as_ref())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::datasource::CatalogTable;
    use arrow::datatypes::Schema;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::execution::context::ExecutionContext;
    use datafusion::logical_plan::{sum, Expr};
    use datafusion::scalar::ScalarValue;

    #[test]
    fn search_table_df_plan() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        let empty_table = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
        let scalar_expr = Expr::Literal(ScalarValue::from(10));

        let source_df = ctx.read_table(empty_table.clone())?;
        let log_plan = &source_df.to_logical_plan();
        find_table_name::<EmptyTable>(log_plan)?;

        let filtered_df =
            source_df.filter(scalar_expr.clone().eq(scalar_expr.clone()))?;
        let log_plan = &filtered_df.to_logical_plan();
        find_table_name::<EmptyTable>(log_plan)?;

        let grouped_df = filtered_df
            .aggregate(vec![scalar_expr.clone()], vec![sum(scalar_expr.clone())])?;
        let log_plan = &grouped_df.to_logical_plan();
        find_table_name::<EmptyTable>(log_plan)?;

        // search but not found
        find_table_name::<CatalogTable>(&grouped_df.to_logical_plan())
            .expect_err("Catalog table should not have been found");

        Ok(())
    }

    #[test]
    fn search_table_sql_plan() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        let empty_table = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
        ctx.register_table("test_tbl", empty_table)?;
        let df = ctx.sql("SELECT * FROM test_tbl")?;
        let log_plan = df.to_logical_plan();
        let found_name = find_table_name::<EmptyTable>(&log_plan).unwrap();
        assert_eq!(found_name, "test_tbl");

        Ok(())
    }
}
