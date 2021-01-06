use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use super::range_cache::{CachedRead, Downloader, RangeCache};
use crate::error::{BuzzError, Result};
use async_trait::async_trait;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
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

//// File implementation that uses

#[derive(Clone)]
pub struct S3FileAsync {
  region: String,
  dler_id: String,
  file_id: String,
  length: u64,
  cache: Arc<RangeCache>,
}

impl fmt::Debug for S3FileAsync {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("S3FileAsync")
      .field("downloader_id", &self.dler_id)
      .field("file_id", &self.file_id)
      .field("length", &self.length)
      .finish()
  }
}

impl S3FileAsync {
  pub fn new(
    region: &str,
    bucket: &str,
    key: &str,
    length: u64,
    cache: Arc<RangeCache>,
  ) -> Self {
    let dler_id = format!("s3::{}", region);
    // if downloader not defined in range cache, add it
    cache.register_downloader(&dler_id, || {
      Arc::new(S3Downloader {
        client: new_client(region),
      })
    });
    S3FileAsync {
      region: region.to_owned(),
      dler_id,
      file_id: format!("{}/{}", bucket, key),
      length,
      cache,
    }
  }

  pub fn prefetch(&self, start: u64, length: usize) {
    self
      .cache
      .schedule(self.dler_id.clone(), self.file_id.clone(), start, length);
  }
}

impl Length for S3FileAsync {
  fn len(&self) -> u64 {
    self.length
  }
}

impl ChunkReader for S3FileAsync {
  type T = CachedRead;

  fn get_read(&self, start: u64, length: usize) -> ParquetResult<Self::T> {
    self
      .cache
      .get(self.dler_id.clone(), self.file_id.clone(), start, length)
      .map_err(|e| match e {
        BuzzError::ParquetError(err) => err,
        err => ParquetError::General(format!("{}", err)),
      })
  }
}

//// S3 Client ////

fn new_client(region: &str) -> Arc<S3Client> {
  let region = Region::from_str(region).unwrap();
  Arc::new(S3Client::new(region))
}
