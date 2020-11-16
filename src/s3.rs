use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use parquet::file::reader::{ChunkReader, Length};
use rusoto_core::Region;
use rusoto_s3::{GetObjectOutput, GetObjectRequest, S3Client, S3};
use tokio::io::AsyncReadExt;

pub fn new_client(region: &str) -> Arc<S3Client> {
  let region = Region::from_str(region).unwrap();
  Arc::new(S3Client::new(region))
}

#[derive(Clone)]
pub struct S3FileAsync {
  bucket: String,
  key: String,
  client: Arc<S3Client>,
  length: u64,
  data: Arc<Mutex<HashMap<u64, Arc<Vec<u8>>>>>,
  dl_queue: Arc<Mutex<Vec<(i64, i64)>>>,
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
    S3FileAsync {
      bucket,
      key,
      length,
      client,
      data: Arc::new(Mutex::new(HashMap::new())),
      dl_queue: Arc::new(Mutex::new(vec![])),
    }
  }

  pub async fn download_footer(&self) {
    // TODO better way to read end of file (1MB)
    let end_length = 64 * 1024;
    let (end_start, end_length) = match self.length.checked_sub(end_length) {
      Some(val) => (val, end_length),
      None => (0, self.length),
    };
    self.get_range(end_start, end_length as usize).await;
  }

  pub fn set_dl_queue(&mut self, projection: Vec<(i64, i64)>) {
    *self.dl_queue.lock().unwrap() = projection;
  }

  pub async fn download_columns(&self) {
    let queue = self.dl_queue.lock().unwrap().drain(..).collect::<Vec<_>>();
    for range in queue {
      self.get_range(range.0 as u64, range.1 as usize).await;
      println!("downloaded from {} len {}", range.0, range.1);
    }
  }

  async fn get_range(&self, start: u64, length: usize) {
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
    // println!("Insert: [{}-{}[", start, start + length as u64);
    self.data.lock().unwrap().insert(start, Arc::new(res));
  }
}

pub struct S3Read {
  data: Arc<Vec<u8>>,
  position: u64,
  remaining: u64,
}

impl Read for S3Read {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    // compute len to read
    let len = std::cmp::min(buf.len(), self.remaining as usize);
    // get downloaded data
    buf[0..len].clone_from_slice(
      &self.data[self.position as usize..(self.position as usize + len)],
    );

    // update reader position
    self.remaining -= len as u64;
    self.position += len as u64;
    Ok(len)
  }
}

impl Length for S3FileAsync {
  fn len(&self) -> u64 {
    self.length
  }
}

impl ChunkReader for S3FileAsync {
  type T = S3Read;

  fn get_read(&self, start: u64, length: usize) -> parquet::errors::Result<Self::T> {
    // println!("Get:  [{}-{}[", start, start + length as u64);
    let data_guard = self.data.lock().unwrap();
    let data = data_guard
      .get(&start)
      .expect(&format!("Chunk not found at offset {}", start));
    Ok(S3Read {
      data: Arc::clone(data),
      position: 0,
      remaining: length as u64,
    })
  }
}
