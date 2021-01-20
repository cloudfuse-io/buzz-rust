use std::sync::Arc;

use arrow_parquet::file::reader::{FileReader, Length, SerializedFileReader};
use buzz::clients::{s3, CachedFile, RangeCache};

async fn async_main() {
    let cache = RangeCache::new().await;
    // let file = CachedFile::new(
    //     "us-east-2",
    //     "cloudfuse-taxi-data",
    //     // "raw_small/2009/01/data.parquet",
    //     // 27301328,
    //     "raw_5M/2009/01/data.parquet",
    //     388070114,
    //     Arc::new(cache),
    // );

    let (dler_id, dler_creator) = s3::downloader_creator("us-east-2");
    let file_id = s3::file_id("ursa-labs-taxi-data", "2009/01/data.parquet");

    let file =
        CachedFile::new(file_id, 461966527, Arc::new(cache), dler_id, dler_creator);

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
