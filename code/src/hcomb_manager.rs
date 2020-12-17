use crate::query::HCombCapacity;
use async_trait::async_trait;

pub struct HCombAddress {
    pub endpoint: String,
}

#[async_trait]
pub trait HCombManager {
    /// Search for availaible combs or start new ones if none was found.
    async fn find_or_start(&self, capactity: &HCombCapacity) -> Vec<HCombAddress>;
}

pub struct TestHCombManager {
    pub domain: String,
}

#[async_trait]
impl HCombManager for TestHCombManager {
    async fn find_or_start(&self, capactity: &HCombCapacity) -> Vec<HCombAddress> {
        (0..capactity.zones as usize)
            .map(|i| HCombAddress {
                endpoint: format!("http://{}:50051", self.domain),
            })
            .collect()
    }
}

// TODO implementations:
// TODO - for fargate
