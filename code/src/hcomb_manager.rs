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

// TODO implementations:
// TODO - for docker test
// TODO - for fargate
