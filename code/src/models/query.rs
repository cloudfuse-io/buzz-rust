use serde::Deserialize;

#[derive(PartialEq, Deserialize)]
pub enum BuzzStepType {
    HBee,
    HComb,
}

#[derive(Deserialize)]
pub struct BuzzStep {
    pub sql: String,
    pub name: String,
    pub partition_filter: Option<String>,
    pub step_type: BuzzStepType,
}

#[derive(Deserialize)]
pub struct HCombCapacity {
    /// For now only 1 zone is supported (I know, I know... YAGNI! :)
    pub zones: i16,
}

#[derive(PartialEq, Deserialize)]
pub enum BuzzCatalogType {
    DeltaLake,
    Static,
}

#[derive(Deserialize)]
pub struct BuzzCatalog {
    pub name: String,
    pub uri: String,
    pub r#type: BuzzCatalogType,
}

#[derive(Deserialize)]
pub struct BuzzQuery {
    pub steps: Vec<BuzzStep>,
    pub capacity: HCombCapacity,
    pub catalogs: Vec<BuzzCatalog>,
}
