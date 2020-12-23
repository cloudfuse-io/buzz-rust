use std::sync::Arc;

use buzz::services::hbee::{range_cache::RangeCache, s3::S3FileAsync};
use parquet::file::reader::{FileReader, Length, SerializedFileReader};

async fn async_main() {
    let cache = RangeCache::new().await;
    let file = S3FileAsync::new(
        "eu-west-1",
        "cloudfuse-taxi-data",
        // "raw_small/2009/01/data.parquet",
        // 27301328,
        "raw_5M/2009/01/data.parquet",
        388070114,
        Arc::new(cache),
    );

    // download footer
    let prefetch_size = 1024 * 1024;
    file.prefetch(file.len() - prefetch_size, prefetch_size as usize);

    let reader =
        SerializedFileReader::new(file).expect("Failed to create serialized reader");
    println!("{:?}", reader.metadata().num_row_groups())
}

fn main() {
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_main());
    println!("Result:{:?}", result);
}
