use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::range_cache::{CachedRead, Downloader, RangeCache};
use async_trait::async_trait;
use parquet::file::reader::{ChunkReader, Length};
use rusoto_core::Region;
use rusoto_s3::{GetObjectOutput, GetObjectRequest, S3Client, S3};
use tokio::io::AsyncReadExt;

//// S3 Client ////

pub fn new_client(region: &str) -> Arc<S3Client> {
  let region = Region::from_str(region).unwrap();
  Arc::new(S3Client::new(region))
}

//// Implementation of the `download` function used by the range cache to fetch data

#[derive(Clone)]
struct S3Downloader {
  bucket: String,
  key: String,
  client: Arc<S3Client>,
}

#[async_trait]
impl Downloader for S3Downloader {
  async fn download(&self, start: u64, length: usize) -> Vec<u8> {
    let range = format!("bytes={}-{}", start, start + length as u64 - 1);
    let get_obj_req = GetObjectRequest {
      bucket: self.bucket.clone(),
      key: self.key.clone(),
      range: Some(range),
      ..Default::default()
    };
    let obj: GetObjectOutput = self
      .client
      .get_object(get_obj_req)
      .await
      .expect("get failed");
    let mut reader = obj.body.unwrap().into_async_read();
    let mut res = vec![];
    res.reserve(length);
    let bytes_read = reader.read_to_end(&mut res).await.unwrap();
    if bytes_read != length {
      panic!("Not the expected number of bytes");
    }
    res
  }
}

//// File implementation that uses

#[derive(Clone)]
pub struct S3FileAsync {
  bucket: String,
  key: String,
  length: u64,
  data: Arc<RangeCache>,
}

impl fmt::Debug for S3FileAsync {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("S3FileAsync")
      .field("bucket", &self.bucket)
      .field("key", &self.key)
      .field("length", &self.length)
      .finish()
  }
}

impl S3FileAsync {
  pub fn new(bucket: String, key: String, length: u64, client: Arc<S3Client>) -> Self {
    let downloader = S3Downloader {
      bucket: bucket.clone(),
      key: key.clone(),
      client: Arc::clone(&client),
    };
    S3FileAsync {
      bucket,
      key,
      length,
      data: Arc::new(RangeCache::new(downloader)),
    }
  }

  pub fn prefetch(&self, start: u64, length: usize) {
    self.data.schedule(start, length);
  }

  // TODO the cache should download instead of return an error, this way we could avoid to specify this footer delete
  pub fn download_footer(&self) {
    let end_length = 1024 * 1024;
    let (end_start, end_length) = match self.length.checked_sub(end_length) {
      Some(val) => (val, end_length),
      None => (0, self.length),
    };
    self.prefetch(end_start, end_length as usize);
  }
}

impl Length for S3FileAsync {
  fn len(&self) -> u64 {
    self.length
  }
}

impl ChunkReader for S3FileAsync {
  type T = CachedRead;

  fn get_read(&self, start: u64, length: usize) -> parquet::errors::Result<Self::T> {
    Ok(self.data.get(start, length).unwrap())
  }
}
