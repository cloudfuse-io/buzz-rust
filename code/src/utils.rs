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
