//! modules that help connecting to the outside world

mod cached_file;
pub mod fargate;
pub mod flight_client;
pub mod lambda;
mod range_cache;
pub mod s3;

pub use cached_file::CachedFile;
pub use range_cache::{Downloader, RangeCache};
