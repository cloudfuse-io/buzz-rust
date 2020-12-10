use std::sync::Arc;
use std::time::Instant;

use crate::dataframe_ops::DataframeOperations;
use crate::datasource::StreamTable;
use crate::error::Result;
use crate::results_service::ResultsService;
use arrow::datatypes::Schema;
use arrow::util::pretty;
use datafusion::prelude::*;

// TODO delete file

pub struct HCombQuery {
    pub query_id: String,
    pub nb_hbees: usize,
    pub schema: Arc<Schema>,
    pub ops: Box<dyn DataframeOperations>,
}

pub struct HCombQueryRunner {
    pub results_service: Arc<ResultsService>,
    pub concurrency: usize,
    pub batch_size: usize,
}

impl HCombQueryRunner {
    pub fn new(results_service: Arc<ResultsService>) -> Self {
        Self {
            results_service,
            concurrency: 1,
            batch_size: 2048,
        }
    }
    pub async fn run(&self, query: HCombQuery) -> Result<()> {
        let debug = true;
        let mut start = Instant::now();

        let config = ExecutionConfig::new()
            .with_concurrency(self.concurrency)
            .with_batch_size(self.batch_size);

        let stream = self
            .results_service
            .new_query(query.query_id, query.nb_hbees);

        let stream_table =
            StreamTable::try_new(Box::pin(stream), Arc::clone(&query.schema))?;

        let mut ctx = ExecutionContext::with_config(config);

        let df = query
            .ops
            .apply_to(ctx.read_table(Arc::new(stream_table))?)?;

        let logical_plan = df.to_logical_plan();
        if debug {
            println!("=> Original logical plan:\n{:?}", logical_plan);
        }

        let logical_plan = ctx.optimize(&logical_plan)?;
        if debug {
            println!("=> Optimized logical plan:\n{:?}", logical_plan);
        }

        let physical_plan = ctx.create_physical_plan(&logical_plan).unwrap();
        if debug {
            // println!("=> Physical plan:\n{:?}", physical_plan);
            println!("=> Input Schema:\n{:?}", query.schema);
            println!("=> Output Schema:\n{:?}", physical_plan.schema());
        }

        let setup_duration = start.elapsed().as_millis();
        start = Instant::now();

        let result = ctx.collect(physical_plan).await.unwrap();

        if debug {
            pretty::print_batches(&result)?;
            println!("Setup took {} ms", setup_duration);
            println!("Processing took {} ms", start.elapsed().as_millis());
        }

        Ok(())
    }
}
