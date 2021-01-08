use crate::clients::fargate::FargateCreationClient;
use crate::error::Result;
use crate::models::{query::HCombCapacity, HCombAddress};
use async_trait::async_trait;

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
        Ok(vec![format!("http://{}:50051", self.domain)])
    }
}

pub struct FargateHCombManager {
    client: FargateCreationClient,
}

impl FargateHCombManager {
    pub fn new(region: &str) -> Self {
        Self {
            client: FargateCreationClient::new(region),
        }
    }
}

#[async_trait]
impl HCombManager for FargateHCombManager {
    async fn find_or_start(
        &self,
        capactity: &HCombCapacity,
    ) -> Result<Vec<HCombAddress>> {
        assert_eq!(capactity.zones, 1, "Only single zone supported for now");

        // TODO not only create new but also check available
        let private_ip = self.client.create_new().await?;

        Ok(vec![format!("http://{}:3333", private_ip)])
    }
}
