use std::sync::Arc;

use buzz::clients::{s3::S3FileAsync, RangeCache};
use parquet::file::reader::{FileReader, Length, SerializedFileReader};

async fn async_main() {
    let cache = RangeCache::new().await;
    // let file = S3FileAsync::new(
    //     "eu-west-1",
    //     "cloudfuse-taxi-data",
    //     // "raw_small/2009/01/data.parquet",
    //     // 27301328,
    //     "raw_5M/2009/01/data.parquet",
    //     388070114,
    //     Arc::new(cache),
    // );

    let file = S3FileAsync::new(
        "us-east-2",
        "ursa-labs-taxi-data",
        "2009/01/data.parquet",
        461966527,
        Arc::new(cache),
    );

    // download footer
    let prefetch_size = 1024 * 1024;
    file.prefetch(file.len() - prefetch_size, prefetch_size as usize);

    let reader =
        SerializedFileReader::new(file).expect("Failed to create serialized reader");
    println!("num_row_groups: {:?}", reader.metadata().num_row_groups())
}

fn main() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_main());
}
