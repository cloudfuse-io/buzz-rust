//! Datasources are implementations of DataFusion's TableProvider trait

mod catalog;
mod hbee;
mod hcomb;

pub use catalog::static_catalog::{CatalogFile, StaticCatalogTable};
pub use catalog::test_catalog::MockSplittableTable;
pub use catalog::{CatalogTable, SplittableTable};
pub use hbee::{s3_parquet::S3ParquetTable, HBeeTable, HBeeTableDesc};
pub use hcomb::{HCombTable, HCombTableDesc};
