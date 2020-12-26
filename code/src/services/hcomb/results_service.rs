use std::collections::HashMap;
use std::sync::Mutex;

use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use tokio::stream::Stream;
use tokio::sync::mpsc;

struct IntermediateRes {
    tx: Option<mpsc::UnboundedSender<ArrowResult<RecordBatch>>>,
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
        nb_hbees: usize,
    ) -> impl Stream<Item = ArrowResult<RecordBatch>> {
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut sender_map_guard = self.tx_map.lock().unwrap();
            sender_map_guard.insert(
                query_id,
                IntermediateRes {
                    tx: Some(tx),
                    remaining_tasks: nb_hbees,
                },
            );
        }
        rx
    }

    pub fn add_result(&self, query_id: &str, data: ArrowResult<RecordBatch>) {
        let sender_map = self.tx_map.lock().unwrap();
        let res_opt = sender_map.get(query_id);
        match res_opt {
            Some(res) => {
                let send_res = res.tx.as_ref().unwrap().send(data);
                if send_res.is_err() {
                    println!("[hcomb] Result chan closed because query '{}' failed, ignoring result", query_id);
                }
            }
            None => {
                println!(
                    "[hcomb] Query '{}' not registered in IntermediateResults",
                    query_id
                );
            }
        }
    }

    pub fn task_finished(&self, query_id: &str) {
        let mut sender_map = self.tx_map.lock().unwrap();
        let res_opt = sender_map.get_mut(query_id);
        match res_opt {
            Some(res) => {
                res.remaining_tasks -= 1;
                if res.remaining_tasks == 0 {
                    res.tx = None;
                }
            }
            None => {
                println!(
                    "[hcomb] Query '{}' not registered in IntermediateResults",
                    query_id
                );
            }
        }
    }
}
