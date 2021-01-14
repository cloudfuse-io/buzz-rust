use std::time::{Duration, Instant};

use crate::clients::fargate::FargateCreationClient;
use crate::clients::flight_client;
use crate::error::Result;
use crate::internal_err;
use crate::models::{query::HCombCapacity, HCombAddress};
use async_trait::async_trait;
use tokio::time::timeout;

#[async_trait]
pub trait HCombManager {
    /// Search for availaible combs or start new ones if none was found.
    async fn find_or_start(&self, capactity: &HCombCapacity)
        -> Result<Vec<HCombAddress>>;
}

pub struct TestHCombManager {
    pub domain: String,
}

#[async_trait]
impl HCombManager for TestHCombManager {
    async fn find_or_start(
        &self,
        capactity: &HCombCapacity,
    ) -> Result<Vec<HCombAddress>> {
        assert_eq!(capactity.zones, 1, "Only single zone supported for now");
        Ok(vec![format!("http://{}:3333", self.domain)])
    }
}

pub struct FargateHCombManager {
    client: FargateCreationClient,
}

impl FargateHCombManager {
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            client: FargateCreationClient::try_new()?,
        })
    }
}

#[async_trait]
impl HCombManager for FargateHCombManager {
    async fn find_or_start(
        &self,
        capactity: &HCombCapacity,
    ) -> Result<Vec<HCombAddress>> {
        assert_eq!(capactity.zones, 1, "Only single zone supported for now");

        let private_ip = self.client.create_new().await?;

        let address = format!("http://{}:3333", private_ip);

        let start = Instant::now();
        let timeout_sec = 20;
        loop {
            match timeout(
                Duration::from_millis(1),
                flight_client::try_connect(&address),
            )
            .await
            {
                Ok(Ok(())) => break,
                Ok(Err(e)) if start.elapsed().as_secs() >= timeout_sec => {
                    Err(internal_err!(
                        "Couldn't connect to hcomb for more than {}s with: {}",
                        timeout_sec,
                        e
                    ))?
                }
                Err(_) if start.elapsed().as_secs() >= timeout_sec => {
                    Err(internal_err!(
                        "Couldn't connect to hcomb for more than {}s because of timeouts",
                        timeout_sec
                    ))?
                }
                Ok(Err(_)) | Err(_) => continue,
            }
        }
        println!(
            "[fuse] took {}ms to connect to hcomb",
            start.elapsed().as_millis()
        );
        Ok(vec![address])
    }
}
