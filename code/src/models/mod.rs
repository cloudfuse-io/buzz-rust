//! Models are entities that are common to services

pub mod actions;
pub mod env;
mod hbee_event;
pub mod query;

pub use hbee_event::{HBeeEvent, HBeePlanBytes};

pub type HCombAddress = String;

#[derive(Clone, Debug)]
pub struct SizedFile {
    pub key: String,
    pub length: u64,
}
