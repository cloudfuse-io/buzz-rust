use std::sync::Arc;

use buzz::s3::S3FileAsync;
use parquet::file::reader::{FileReader, SerializedFileReader};

use buzz::s3;

async fn async_main() {
    let s3_client = Arc::new(s3::new_client("eu-west-1"));
    let file = S3FileAsync::new(
        "cloudfuse-taxi-data".to_owned(),
        // "raw_small/2009/01/data.parquet".to_owned(),
        // 27301328,
        "raw_5M/2009/01/data.parquet".to_owned(),
        388070114,
        Arc::clone(&s3_client),
    );

    file.download_footer().await;

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
