use crate::query::HCombCapacity;
use async_trait::async_trait;

pub type HCombAddress = String;

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
        assert_eq!(capactity.zones, 1, "Only single zone supported for now");
        vec![format!("http://{}:50051", self.domain)]
    }
}

// TODO implementations:
// TODO - for fargate
