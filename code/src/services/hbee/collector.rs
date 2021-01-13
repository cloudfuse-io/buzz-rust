use crate::clients::flight_client;
use crate::error::Result;
use crate::internal_err;
use crate::models::HCombAddress;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use async_trait::async_trait;

#[async_trait]
pub trait Collector: Send + Sync {
    /// send results back to hcomb
    async fn send_back(
        &self,
        query_id: String,
        data: Result<Vec<RecordBatch>>,
        address: HCombAddress,
    ) -> Result<()>;
}

pub struct NoopCollector {}

#[async_trait]
impl Collector for NoopCollector {
    async fn send_back(
        &self,
        _query_id: String,
        data: Result<Vec<RecordBatch>>,
        _address: HCombAddress,
    ) -> Result<()> {
        let result = data?;
        pretty::print_batches(&result).unwrap();
        Ok(())
    }
}

pub struct HttpCollector {}

#[async_trait]
impl Collector for HttpCollector {
    async fn send_back(
        &self,
        query_id: String,
        data: Result<Vec<RecordBatch>>,
        address: HCombAddress,
    ) -> Result<()> {
        match data {
            Ok(query_res) => flight_client::call_do_put(query_id, &address, query_res)
                .await
                .map_err(|e| internal_err!("Could not do_put hbee result: {}", e)),
            Err(query_err) => {
                flight_client::call_fail_action(
                    query_id,
                    &address,
                    format!("{}", query_err),
                )
                .await
                .map_err(|e| internal_err!("Could not do_action(FAIL) hbee: {}", e))?;
                Err(query_err)
            }
        }
    }
}
