pub mod clients;
pub mod datasource;
pub mod error;
pub mod example_catalog;
pub mod execution_plan;
pub mod flight_utils;
pub mod models;
pub mod plan_utils;
pub mod serde;
pub mod services;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/buzz.protobuf.rs"));
}
