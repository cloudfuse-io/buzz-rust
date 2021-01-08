use std::convert::Into;
use std::str::FromStr;
use std::sync::Arc;

use crate::error::{BuzzError, Result};
use rusoto_core::Region;
use rusoto_lambda::{InvocationRequest, Lambda, LambdaClient};

pub struct LambdaInvokeClient {
    client: Arc<LambdaClient>,
}

impl LambdaInvokeClient {
    pub fn new(region: &str) -> Self {
        Self {
            client: new_client(region),
        }
    }
}

impl LambdaInvokeClient {
    /// Invoke an async lambda
    pub async fn invoke(&self, body: Vec<u8>) -> Result<()> {
        let input = InvocationRequest {
            client_context: None,
            function_name: String::new(), // TODO
            invocation_type: Some("event".to_owned()),
            log_type: None,
            payload: Some(body.into()),
            qualifier: None,
        };
        let result = self
            .client
            .invoke(input)
            .await
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
