use std::collections::BTreeMap;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use snafu::{ensure, Backtrace, OptionExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Download not finished for bytes {}-{}", start, start + *length as u64))]
    DownloadNotFinished {
        start: u64,
        length: usize,
        backtrace: Backtrace,
    },
    #[snafu(display("Download was not scheduled for bytes {}-{}", start, start + *length as u64))]
    DownloadNotScheduled {
        start: u64,
        length: usize,
        backtrace: Backtrace,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

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
pub trait Downloader: Send + Sync {
    async fn download(&self, start: u64, length: usize) -> Vec<u8>;
}

pub struct RangeCache {
    data: Arc<Mutex<BTreeMap<u64, Download>>>,
    cv: Arc<std::sync::Condvar>,
    tx: tokio::sync::mpsc::UnboundedSender<(u64, usize)>,
}

impl RangeCache {
    pub fn new<T: Downloader + 'static>(downloader: T) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(u64, usize)>();
        let cache = Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
            cv: Arc::new(std::sync::Condvar::new()),
            tx,
        };
        let data_ref = Arc::clone(&cache.data);
        let cv_ref = Arc::clone(&cache.cv);
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                // let downloader = Arc::clone(&downloader_arc);
                let downloaded_chunk = downloader.download(message.0, message.1).await;
                data_ref
                    .lock()
                    .unwrap()
                    .insert(message.0, Download::Done(Arc::new(downloaded_chunk)));
                cv_ref.notify_all();
            }
        });
        cache
    }

    pub fn schedule(&self, start: u64, length: usize) {
        self.data.lock().unwrap().insert(start, Download::Pending);
        self.tx.send((start, length)).unwrap();
    }

    /// For now the cache can only get get single chunck readers and fails if the dl was not scheduled
    pub fn get(&self, start: u64, length: usize) -> Result<CachedRead> {
        let mut data_guard = self.data.lock().unwrap();

        let mut before = data_guard
            .range((std::ops::Bound::Unbounded, std::ops::Bound::Included(start)))
            .next_back();

        while let Some((_, Download::Pending)) = before {
            // wait for the dl to be finished
            data_guard = self.cv.wait(data_guard).unwrap();
            before = data_guard
                .range((std::ops::Bound::Unbounded, std::ops::Bound::Included(start)))
                .next_back();
        }

        let before = before.context(DownloadNotScheduled { start, length })?;

        let unused_start = start - before.0;

        match before.1 {
            Download::Done(bytes) => {
                ensure!(
                    bytes.len() >= unused_start as usize + length,
                    DownloadNotScheduled { start, length }
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
