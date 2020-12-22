#[derive(PartialEq)]
pub enum BuzzStepType {
    HBee,
    HComb,
}

pub struct BuzzStep {
    pub sql: String,
    pub name: String,
    pub step_type: BuzzStepType,
}

pub struct HCombCapacity {
    pub ram: i16,
    pub cores: i16,
    pub zones: i16,
}

pub struct BuzzQuery {
    pub steps: Vec<BuzzStep>,
    pub capacity: HCombCapacity,
}
