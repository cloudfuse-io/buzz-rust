use std::convert::Into;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{BuzzError, Result};
use crate::models::env;
use rusoto_core::Region;
use rusoto_lambda::{InvocationRequest, Lambda, LambdaClient};
use tokio::time::timeout;

pub struct LambdaInvokeClient {
    client: Arc<LambdaClient>,
    lambda_name: String,
}

impl LambdaInvokeClient {
    pub fn try_new() -> Result<Self> {
        let config = env::get_lambda_config()?;
        Ok(Self {
            client: new_client(&config.aws_region),
            lambda_name: config.hbee_lambda_name,
        })
    }
}

impl LambdaInvokeClient {
    /// Invoke an async lambda
    pub async fn invoke(&self, body: Vec<u8>) -> Result<()> {
        let input = InvocationRequest {
            client_context: None,
            function_name: self.lambda_name.clone(),
            invocation_type: Some("Event".to_owned()),
            log_type: None,
            payload: Some(body.into()),
            qualifier: None,
        };
        let result = timeout(Duration::from_secs(5), self.client.invoke(input))
            .await
            .map_err(|e| BuzzError::CloudClient(format!("{}", e)))? // first unwrap timeout
            .map_err(|e| BuzzError::CloudClient(format!("{}", e)))?;
        if let Some(is_handled) = result.function_error {
            Err(BuzzError::CloudClient(format!(
                "An error occured with AWS Lambda invokation: {}",
                is_handled
            )))
        } else {
            Ok(())
        }
    }
}

//// Lambda Client ////

fn new_client(region: &str) -> Arc<LambdaClient> {
    let region = Region::from_str(region).unwrap();
    Arc::new(LambdaClient::new(region))
}
