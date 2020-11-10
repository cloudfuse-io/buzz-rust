use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::stream::Stream;
use tokio::sync::mpsc;

pub struct IntermediateResults {
    // rx: Mutex<HashMap<String, mpsc::Receiver<RecordBatch>>>,
    tx: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<RecordBatch>>>>,
}

impl IntermediateResults {
    pub fn new() -> Self {
        IntermediateResults {
            // rx: Mutex::new(HashMap::new()),
            tx: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn new_query(&self, query_id: String) -> impl Stream {
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut sender_map = self.tx.lock().unwrap();
            sender_map.insert(query_id, tx);
        }
        rx
    }

    pub async fn add_result(&self, query_id: String, data: RecordBatch) {}
}
