//! Models are entities that are common to services

mod hbee_event;
pub mod query;

pub use hbee_event::{HBeeEvent, LogicalPlanBytes};

pub type HCombAddress = String;

#[derive(Clone)]
pub struct SizedFile {
    pub key: String,
    pub length: u64,
}
