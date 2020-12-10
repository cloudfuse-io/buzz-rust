pub mod catalog;
pub mod dataframe_ops;
pub mod datasource;
pub mod error;
pub mod execution_plan;
pub mod flight_client;
pub mod flight_service;
pub mod fuse_service;
pub mod hbee_query;
pub mod hbee_scheduler;
pub mod hcomb_manager;
pub mod hcomb_query;
pub mod hcomb_scheduler;
pub mod query;
pub mod query_planner;
pub mod range_cache;
pub mod results_service;
pub mod s3;
pub mod serde;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/buzz.protobuf.rs"));
}
