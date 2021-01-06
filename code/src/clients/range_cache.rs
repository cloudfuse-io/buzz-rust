use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read};
use std::sync::{Arc, Mutex};

use crate::error::{BuzzError, Result};
use crate::{ensure, internal_err};
use async_trait::async_trait;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// A reader that points to a cached chunk
/// TODO this cannot read from multiple concatenated chunks
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

/// The status and content of the download
enum Download {
    Pending,
    Done(Arc<Vec<u8>>),
    Error(String),
}

/// An "all or nothing" representation of the download.
#[async_trait]
pub trait Downloader: Send + Sync {
    async fn download(
        &self,
        file_id: String,
        start: u64,
        length: usize,
    ) -> Result<Vec<u8>>;
}

type DownloaderId = String;
type FileId = String;
type FileData = BTreeMap<u64, Download>;
type CacheKey = (DownloaderId, FileId);
type CacheData = Arc<Mutex<HashMap<CacheKey, FileData>>>;
type DownloaderMap = Arc<Mutex<HashMap<DownloaderId, Arc<dyn Downloader>>>>;
type DownloadRequest = (DownloaderId, FileId, u64, usize);

/// A caching struct that queues up download requests and executes them with
/// the appropriate registered donwloader.
pub struct RangeCache {
    data: CacheData,
    downloaders: DownloaderMap,
    cv: Arc<std::sync::Condvar>,
    tx: UnboundedSender<DownloadRequest>,
}

impl RangeCache {
    /// Spawns a task that will listen for new chunks to download and schedule them for download
    pub async fn new() -> Self {
        let (tx, rx) = unbounded_channel::<DownloadRequest>();
        let cache = Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            downloaders: Arc::new(Mutex::new(HashMap::new())),
            cv: Arc::new(std::sync::Condvar::new()),
            tx,
        };
        cache.start(rx).await;
        cache
    }

    pub async fn start(&self, mut rx: UnboundedReceiver<DownloadRequest>) {
        let data_ref = Arc::clone(&self.data);
        let cv_ref = Arc::clone(&self.cv);
        let downloaders_ref = Arc::clone(&self.downloaders);
        tokio::spawn(async move {
            let pool = Arc::new(tokio::sync::Semaphore::new(8));
            while let Some(message) = rx.recv().await {
                // obtain a permit, it will be released in the spawned download task
                let permit = pool.acquire().await;
                permit.forget();
                // run download in a dedicated task
                let downloaders_ref = Arc::clone(&downloaders_ref);
                let data_ref = Arc::clone(&data_ref);
                let cv_ref = Arc::clone(&cv_ref);
                let pool_ref = Arc::clone(&pool);
                tokio::spawn(async move {
                    // get ref to donwloader
                    let downloader;
                    {
                        let downloaders_guard = downloaders_ref.lock().unwrap();
                        let downloader_ref = downloaders_guard
                            .get(&message.0)
                            .expect("Downloader not found");
                        downloader = Arc::clone(downloader_ref);
                    }
                    // download using that ref
                    let downloaded_res = downloader
                        .download(message.1.clone(), message.2, message.3)
                        .await;
                    pool_ref.add_permits(1);
                    // update the cache data with the result
                    let mut data_guard = data_ref.lock().unwrap();
                    let file_map = data_guard
                        .entry((message.0, message.1))
                        .or_insert_with(|| BTreeMap::new());
                    match downloaded_res {
                        Ok(downloaded_chunk) => {
                            file_map.insert(
                                message.2,
                                Download::Done(Arc::new(downloaded_chunk)),
                            );
                        }
                        Err(err) => {
                            file_map.insert(message.2, Download::Error(err.reason()));
                        }
                    }
                    cv_ref.notify_all();
                });
            }
        });
    }

    /// Registers a new downloader for the given id if necessary
    pub fn register_downloader<F>(&self, downloader_id: &str, downloader_creator: F)
    where
        F: Fn() -> Arc<dyn Downloader>,
    {
        let mut dls_guard = self.downloaders.lock().unwrap();
        let current = dls_guard.get(downloader_id);
        if current.is_none() {
            dls_guard.insert(downloader_id.to_owned(), downloader_creator());
        }
    }

    /// Add a new chunk to the download queue
    pub fn schedule(
        &self,
        downloader_id: DownloaderId,
        file_id: FileId,
        start: u64,
        length: usize,
    ) {
        let mut data_guard = self.data.lock().unwrap();
        let file_map = data_guard
            .entry((downloader_id.clone(), file_id.clone()))
            .or_insert_with(|| BTreeMap::new());
        file_map.insert(start, Download::Pending);
        self.tx
            .send((downloader_id, file_id, start, length))
            .unwrap();
    }

    /// Get a chunk from the cache
    /// For now the cache can only get get single chunck readers and fails if the dl was not scheduled
    /// If the download is not finished, this waits synchronously for the chunk to be ready
    pub fn get(
        &self,
        downloader_id: DownloaderId,
        file_id: FileId,
        start: u64,
        length: usize,
    ) -> Result<CachedRead> {
        use std::ops::Bound::{Included, Unbounded};
        let mut data_guard = self.data.lock().unwrap();
        let identifier = (downloader_id.clone(), file_id.clone());
        let file_map = data_guard.get(&identifier).ok_or(internal_err!(
            "No download scheduled for: {} / {}",
            &downloader_id,
            &file_id,
        ))?;

        let mut before = file_map.range((Unbounded, Included(start))).next_back();

        while let Some((_, Download::Pending)) = before {
            // wait for the dl to be finished
            data_guard = self.cv.wait(data_guard).unwrap();
            before = data_guard
                .get(&identifier)
                .expect("files should not disappear during download")
                .range((Unbounded, Included(start)))
                .next_back();
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
            Download::Error(err) => Err(BuzzError::Download(err.to_owned())),
            Download::Pending => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_correct_ranges() {
        let cache = Arc::new(RangeCache::new().await);
        cache.register_downloader("dl1", || Arc::new(MockDownloader));
        cache.schedule("dl1".to_owned(), "file1".to_owned(), 145, 50);

        // first read will need to wait (sync) because of MockDownloader delay
        let content1 = read_from_cache(Arc::clone(&cache), "dl1", "file1", 145, 50).await;
        assert!(content1.is_ok());
        assert_eq!(content1.unwrap(), pattern(0, 50));

        // the second read should hit the cache
        let content2 = read_from_cache(Arc::clone(&cache), "dl1", "file1", 150, 30).await;
        assert!(content2.is_ok());
        assert_eq!(content2.unwrap(), pattern(5, 35));
    }

    #[tokio::test]
    async fn test_invalid_ranges() {
        let cache = Arc::new(RangeCache::new().await);
        cache.register_downloader("dl1", || Arc::new(MockDownloader));
        cache.schedule("dl1".to_owned(), "file1".to_owned(), 145, 50);

        // read overflows existing chunck to the left
        let read_res = read_from_cache(Arc::clone(&cache), "dl1", "file1", 140, 50).await;
        assert!(read_res.is_err());

        // read overflows existing chunck to the right
        let read_res = read_from_cache(Arc::clone(&cache), "dl1", "file1", 150, 50).await;
        assert!(read_res.is_err());

        // read from a downloader that is not present
        let read_res = read_from_cache(Arc::clone(&cache), "dl2", "file1", 145, 50).await;
        assert!(read_res.is_err());

        // read from a file that is not present
        let read_res = read_from_cache(Arc::clone(&cache), "dl1", "file2", 145, 50).await;
        assert!(read_res.is_err());
    }

    #[tokio::test]
    async fn test_registers_twice() {
        let cache = Arc::new(RangeCache::new().await);
        cache.register_downloader("dl1", || Arc::new(MockDownloader));
        cache.schedule("dl1".to_owned(), "file1".to_owned(), 145, 50);
        cache.register_downloader("dl1", || Arc::new(MockDownloader));

        // registering the downloader twice should be a noop
        let content = read_from_cache(Arc::clone(&cache), "dl1", "file1", 145, 50).await;
        assert!(content.is_ok());
        assert_eq!(content.unwrap(), pattern(0, 50));
    }

    //// Test Fixtures: ////

    /// A downloader that returns a simple pattern (1,2,3...254,255,1,2...)
    /// Waits for 10ms before returning its result to trigger cache misses
    struct MockDownloader;

    #[async_trait]
    impl Downloader for MockDownloader {
        async fn download(
            &self,
            _file: String,
            _start: u64,
            length: usize,
        ) -> Result<Vec<u8>> {
            tokio::time::delay_for(Duration::from_millis(10)).await;
            Ok(pattern(0, length))
        }
    }

    /// The pattern (1,2,3...254,255,1,2...) in the range [start,end[
    fn pattern(start: usize, end: usize) -> Vec<u8> {
        (start..end).map(|i| (i % 256) as u8).collect::<Vec<_>>()
    }

    /// Read the given bytes from the given file in the given cache
    /// Spawns a new thread to do the read because it is blocking
    async fn read_from_cache(
        cache: Arc<RangeCache>,
        downloader: &'static str,
        file: &'static str,
        start: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let mut reader =
                cache.get(downloader.to_owned(), file.to_owned(), start, length)?;
            let mut content = vec![];
            reader.read_to_end(&mut content)?;
            Ok(content)
        })
        .await
        .unwrap()
    }
}
