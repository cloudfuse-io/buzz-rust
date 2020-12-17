use crate::error::Result;
use crate::hcomb_manager::HCombAddress;
use crate::internal_err;
use async_trait::async_trait;
use datafusion::logical_plan::LogicalPlan;

use hyper::{Body, Client, Request};
// use tokio::io::{self, AsyncWriteExt as _};

#[async_trait]
pub trait HBeeScheduler {
    async fn schedule(&self, address: &HCombAddress, plan: LogicalPlan) -> Result<()>;
}

pub struct TestHBeeScheduler {
    pub domain: String,
}

#[async_trait]
impl HBeeScheduler for TestHBeeScheduler {
    async fn schedule(&self, address: &HCombAddress, plan: LogicalPlan) -> Result<()> {
        let client = Client::new();

        let req = Request::builder()
            .method("POST")
            .uri(format!("http://{}:3000", self.domain))
            .body(Body::from("Hallo!"))
            .expect("request builder");

        let res = client
            .request(req)
            .await
            .map_err(|e| internal_err!("hbee scheduling failed: {}", e))?;

        println!("Response: {}", res.status());
        println!("Headers: {:#?}\n", res.headers());

        // Stream the body, writing each chunk to stdout as we get it
        // (instead of buffering and printing at the end).
        // while let Some(next) = res.data().await {
        //     let chunk =
        //         next.map_err(|e| internal_err!("hbee scheduling failed: {}", e))?;
        //     io::stdout().write_all(&chunk).await?;
        // }

        Ok(())
    }
}

// TODO implementations:
// TODO - for lambd
