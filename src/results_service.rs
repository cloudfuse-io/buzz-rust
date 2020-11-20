use std::collections::HashMap;
use std::sync::Mutex;

use arrow::record_batch::RecordBatch;
use tokio::stream::Stream;
use tokio::sync::mpsc;

struct IntermediateRes {
    tx: Option<mpsc::UnboundedSender<RecordBatch>>,
    remaining_tasks: usize,
}

pub struct ResultsService {
    tx_map: Mutex<HashMap<String, IntermediateRes>>,
}

impl ResultsService {
    pub fn new() -> Self {
        Self {
            tx_map: Mutex::new(HashMap::new()),
        }
    }

    pub fn new_query(
        &self,
        query_id: String,
        nb_bees: usize,
    ) -> impl Stream<Item = RecordBatch> {
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut sender_map_guard = self.tx_map.lock().unwrap();
            sender_map_guard.insert(
                query_id,
                IntermediateRes {
                    tx: Some(tx),
                    remaining_tasks: nb_bees,
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
                println!("Query '{}' not registered in IntermediateResults", query_id);
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
                println!("Query '{}' not registered in IntermediateResults", query_id);
            }
        }
    }
}
