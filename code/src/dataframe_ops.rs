use std::sync::Arc;

use crate::error::Result;
use datafusion::dataframe::DataFrame;

pub trait DataframeOperations {
    fn apply_to(&self, source: Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>>;
}

pub struct ClosureDataframeOps {
    pub ops:
        Arc<dyn Fn(Arc<dyn DataFrame>) -> datafusion::error::Result<Arc<dyn DataFrame>>>,
}

impl DataframeOperations for ClosureDataframeOps {
    fn apply_to(&self, source: Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>> {
        let ops_closure = Arc::clone(&self.ops);
        ops_closure(source).map_err(|e| e.into())
    }
}
