use std::sync::Arc;

use buzz::s3::S3FileAsync;
use parquet::file::reader::{FileReader, SerializedFileReader};

use buzz::s3;

async fn async_main() {
    let s3_client = Arc::new(s3::new_client("eu-west-1"));
    let file = S3FileAsync::new(
        "bb-test-data-dev".to_owned(),
        "bid-small.parquet".to_owned(),
        2418624,
        // "bid-large.parquet".to_owned(),
        // 218890209,
        // "yellow_tripdata_2009-01.parquet".to_owned(),
        // 0,
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
