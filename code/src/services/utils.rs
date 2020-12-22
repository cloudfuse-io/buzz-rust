use crate::error::Result;
use crate::not_impl_err;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::LogicalPlan;

/// Search a TableProvider of the given type in the plan.
/// Only works with linear plans (only one datasource).
pub fn find_table<'a, T: TableProvider + 'static>(
    plan: &'a LogicalPlan,
) -> Result<&'a T> {
    let new_inputs = datafusion::optimizer::utils::inputs(&plan);
    if new_inputs.len() > 1 {
        Err(not_impl_err!(
            "Operations with more than one inputs are not supported",
        ))
    } else if new_inputs.len() == 1 {
        // recurse
        find_table(new_inputs[0])
    } else {
        if let Some(result_table) = as_table::<T>(&plan) {
            Ok(result_table)
        } else {
            // TODO find a way to print T
            Err(not_impl_err!("Expected root to be a T"))
        }
    }
}

fn as_table<'a, T: TableProvider + 'static>(plan: &'a LogicalPlan) -> Option<&'a T> {
    if let LogicalPlan::TableScan { source: table, .. } = plan {
        table.as_any().downcast_ref::<T>()
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
    fn search_table_linear_plan() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        let empty_table = EmptyTable::new(Arc::new(Schema::empty()));
        let scalar_expr = Expr::Literal(ScalarValue::from(10));

        let source_df = ctx.read_table(Arc::new(empty_table))?;

        let filtered_df =
            source_df.filter(scalar_expr.clone().eq(scalar_expr.clone()))?;

        let grouped_df = filtered_df
            .aggregate(vec![scalar_expr.clone()], vec![sum(scalar_expr.clone())])?;

        assert!(find_table::<EmptyTable>(&source_df.to_logical_plan()).is_ok());
        assert!(find_table::<EmptyTable>(&filtered_df.to_logical_plan()).is_ok());
        assert!(find_table::<EmptyTable>(&grouped_df.to_logical_plan()).is_ok());
        assert!(find_table::<CatalogTable>(&grouped_df.to_logical_plan()).is_err());

        Ok(())
    }
}
