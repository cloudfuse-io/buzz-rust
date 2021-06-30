use std::str::FromStr;
use std::sync::Arc;

use super::range_cache::Downloader;
use crate::error::{BuzzError, Result};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_s3::{GetObjectOutput, GetObjectRequest, S3Client, S3};
use tokio::io::AsyncReadExt;

//// Implementation of the `download` function used by the range cache to fetch data

#[derive(Clone)]
struct S3Downloader {
  client: Arc<S3Client>,
}

#[async_trait]
impl Downloader for S3Downloader {
  async fn download(
    &self,
    file_id: String,
    start: u64,
    length: usize,
  ) -> Result<Vec<u8>> {
    let mut file_id_split = file_id.split("/");
    let range = format!("bytes={}-{}", start, start + length as u64 - 1);
    println!("file_id:{}", file_id);
    let get_obj_req = GetObjectRequest {
      bucket: file_id_split.next().unwrap().to_owned(),
      key: file_id_split.collect::<Vec<&str>>().join("/"),
      range: Some(range),
      ..Default::default()
    };
    let obj: GetObjectOutput = self
      .client
      .get_object(get_obj_req)
      .await
      .map_err(|e| BuzzError::Download(format!("{}", e)))?;
    let mut reader = obj.body.unwrap().into_async_read();
    let mut res = vec![];
    res.reserve(length);
    let bytes_read = reader
      .read_to_end(&mut res)
      .await
      .map_err(|e| BuzzError::Download(format!("{}", e)))?;
    if bytes_read != length {
      Err(BuzzError::Download(
        "Not the expected number of bytes".to_owned(),
      ))
    } else {
      Ok(res)
    }
  }
}

pub fn downloader_creator(
  region: &str,
) -> (String, Box<dyn Fn() -> Arc<dyn Downloader>>) {
  let region_clone = region.to_owned();
  let creator: Box<dyn Fn() -> Arc<dyn Downloader>> = Box::new(move || {
    Arc::new(S3Downloader {
      client: new_client(&region_clone),
    })
  });

  (format!("s3::{}", region), creator)
}

pub fn file_id(bucket: &str, key: &str) -> String {
  format!("{}/{}", bucket, key)
}

//// S3 Client ////

fn new_client(region: &str) -> Arc<S3Client> {
  let region = Region::from_str(region).unwrap();
  Arc::new(S3Client::new(region))
}
