use std::collections::BTreeMap;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::{ensure, internal_err};
use async_trait::async_trait;

pub struct CachedRead {
    data: Arc<Vec<u8>>,
    position: u64,
    remaining: u64,
}

impl Read for CachedRead {
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

enum Download {
    Pending,
    Done(Arc<Vec<u8>>),
}

#[async_trait]
pub trait Downloader: Send + Sync + Clone {
    async fn download(&self, start: u64, length: usize) -> Vec<u8>;
}

pub struct RangeCache {
    data: Arc<Mutex<BTreeMap<u64, Download>>>,
    cv: Arc<std::sync::Condvar>,
    tx: tokio::sync::mpsc::UnboundedSender<(u64, usize)>,
}

impl RangeCache {
    /// Spawns a task that will listen for new chunks to download and schedule them for download
    pub async fn new<T: Downloader + 'static>(downloader: T) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(u64, usize)>();
        let cache = Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
            cv: Arc::new(std::sync::Condvar::new()),
            tx,
        };
        let data_ref = Arc::clone(&cache.data);
        let cv_ref = Arc::clone(&cache.cv);
        tokio::spawn(async move {
            let pool = Arc::new(tokio::sync::Semaphore::new(8));
            while let Some(message) = rx.recv().await {
                // obtain a permit, it will be released in the spawned download task
                let permit = pool.acquire().await;
                permit.forget();
                // run download in a dedicated task
                let downloader = downloader.clone();
                let data_ref = Arc::clone(&data_ref);
                let cv_ref = Arc::clone(&cv_ref);
                let pool_ref = Arc::clone(&pool);
                tokio::spawn(async move {
                    let downloaded_chunk =
                        downloader.download(message.0, message.1).await;
                    pool_ref.add_permits(1);
                    data_ref
                        .lock()
                        .unwrap()
                        .insert(message.0, Download::Done(Arc::new(downloaded_chunk)));
                    cv_ref.notify_all();
                });
            }
        });
        cache
    }

    /// Add a new chunk to the download queue
    pub fn schedule(&self, start: u64, length: usize) {
        self.data.lock().unwrap().insert(start, Download::Pending);
        self.tx.send((start, length)).unwrap();
    }

    /// Get a chunk from the cache
    /// For now the cache can only get get single chunck readers and fails if the dl was not scheduled
    /// If the download is not finished, this waits synchronously for the chunk to be ready
    pub fn get(&self, start: u64, length: usize) -> Result<CachedRead> {
        use std::ops::Bound::{Included, Unbounded};
        let mut data_guard = self.data.lock().unwrap();

        let mut before = data_guard.range((Unbounded, Included(start))).next_back();

        while let Some((_, Download::Pending)) = before {
            // wait for the dl to be finished
            data_guard = self.cv.wait(data_guard).unwrap();
            before = data_guard.range((Unbounded, Included(start))).next_back();
        }

        let before = before.ok_or(internal_err!(
            "Download not scheduled: (start={},end={})",
            start,
            length,
        ))?;

        let unused_start = start - before.0;

        match before.1 {
            Download::Done(bytes) => {
                ensure!(
                    bytes.len() >= unused_start as usize + length,
                    "Download not scheduled: (start={},end={})",
                    start,
                    length,
                );
                Ok(CachedRead {
                    data: Arc::clone(bytes),
                    position: unused_start,
                    remaining: length as u64,
                })
            }
            Download::Pending => unreachable!(),
        }
    }
}
