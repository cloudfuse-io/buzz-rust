use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use tokio::stream::Stream;
use tokio::sync::mpsc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use datafusion::error::Result;
use datafusion::prelude::*;

struct IntermediateRes {
    tx: Option<mpsc::UnboundedSender<RecordBatch>>,
    remaining_tasks: usize,
}

pub struct IntermediateResults {
    // rx: Mutex<HashMap<String, mpsc::Receiver<RecordBatch>>>,
    tx_map: Arc<Mutex<HashMap<String, IntermediateRes>>>,
}

impl IntermediateResults {
    pub fn new() -> Self {
        IntermediateResults {
            // rx: Mutex::new(HashMap::new()),
            tx_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn new_query(
        &self,
        query_id: String,
        nb_bee: usize,
    ) -> impl Stream<Item = RecordBatch> {
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut sender_map = self.tx_map.lock().unwrap();
            sender_map.insert(
                query_id,
                IntermediateRes {
                    tx: Some(tx),
                    remaining_tasks: nb_bee,
                },
            );
        }
        rx
    }

    pub fn add_result(&self, query_id: &str, data: RecordBatch) {
        println!("add_result({},data)", query_id);
        let sender_map = self.tx_map.lock().unwrap();
        let res_opt = sender_map.get(query_id);
        match res_opt {
            Some(res) => {
                res.tx.as_ref().unwrap().send(data).unwrap();
            }
            None => {
                println!("Query '{}' not registered in IntermediateResults", query_id)
            }
        }
    }

    pub fn task_finished(&self, query_id: &str) {
        println!("task_finished({})", query_id);
        let mut sender_map = self.tx_map.lock().unwrap();
        let res_opt = sender_map.get_mut(query_id);
        match res_opt {
            Some(res) => {
                res.remaining_tasks -= 1;
                if res.remaining_tasks == 0 {
                    println!("cleaning up tx for {}", query_id);
                    res.tx = None;
                }
            }
            None => {
                println!("Query '{}' not registered in IntermediateResults", query_id)
            }
        }
    }
}

pub struct QueryConfig<St> {
    pub concurrency: usize,
    pub batch_size: usize,
    pub stream: St,
    pub schema: Arc<Schema>,
}

pub async fn run<St, F>(query_conf: QueryConfig<St>, query: F) -> Result<()>
where
    F: Fn(Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>>,
    St: Stream<Item = RecordBatch> + Send + 'static,
{
    let debug = true;
    let mut start = Instant::now();

    let config = ExecutionConfig::new()
        .with_concurrency(query_conf.concurrency)
        .with_batch_size(query_conf.batch_size);

    use crate::datasource::StreamTable;
    let stream_table = StreamTable::try_new(
        Box::pin(query_conf.stream),
        Arc::clone(&query_conf.schema),
    )?;

    let mut ctx = ExecutionContext::with_config(config);

    let df = query(ctx.read_table(Arc::new(stream_table))?)?;

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
        println!("=> Input Schema:\n{:?}", query_conf.schema);
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
