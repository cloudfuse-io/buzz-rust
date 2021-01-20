use fmt::Debug;
use std::any::Any;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, thread};

use crate::clients::CachedFile;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use arrow_parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use arrow_parquet::file::reader::{FileReader, Length, SerializedFileReader};
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::stream::Stream;

/// Execution plan for scanning a Parquet file
#[derive(Debug, Clone)]
pub struct ParquetExec {
    files: Vec<CachedFile>,
    /// Schema before projection is applied
    file_schema: SchemaRef,
    /// Schema after projection is applied
    projected_schema: SchemaRef,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan
    pub fn new(
        files: Vec<CachedFile>,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        schema: SchemaRef,
    ) -> Self {
        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };
        let projected_schema = Schema::new(
            projection
                .iter()
                .map(|col| schema.field(*col).clone())
                .collect(),
        );
        Self {
            files,
            file_schema: schema,
            projected_schema: Arc::new(projected_schema),
            projection,
            batch_size,
        }
    }

    /// Read the footer and schedule the downloads of all the required chunks
    async fn init_file(&self, partition: usize) -> DataFusionResult<()> {
        let end_dl_chunk_start = Self::download_footer(self.files[partition].clone());
        let file_schema = self.file_schema.clone();
        let file = self.files[partition].clone();
        let projection = self.projection.clone();

        // Reading the footer is blocking so it should be started on a specific thread
        tokio::task::spawn_blocking(move || {
            let file_reader = Arc::new(
                SerializedFileReader::new(file.clone())
                    .map_err(|e| DataFusionError::ParquetError(e))?,
            );
            let mut arrow_reader = ParquetFileArrowReader::new(file_reader.clone());

            // TODO what about metadata ?
            if file_schema.fields() != arrow_reader.get_schema()?.fields() {
                return Err(DataFusionError::Plan(format!(
                    "Expected and parsed schema fields are not equal: {:?} != {:?}",
                    file_schema.fields(),
                    arrow_reader.get_schema()?.fields()
                )));
            }
            // prefetch usefull byte ranges
            let metadata = file_reader.metadata();
            for i in 0..metadata.num_row_groups() {
                for proj in &projection {
                    let rg_metadata = metadata.row_group(i);
                    let col_metadata = rg_metadata.column(*proj);
                    let (start, length) = col_metadata.byte_range();
                    if start < end_dl_chunk_start {
                        file.prefetch(start, length as usize);
                    }
                }
            }
            Ok(())
        })
        .await
        .unwrap()
    }

    // returns the start of the downloaded chunk
    fn download_footer(file: CachedFile) -> u64 {
        let end_length = 1024 * 1024;
        let (end_start, end_length) = match file.len().checked_sub(end_length) {
            Some(val) => (val, end_length),
            None => (0, file.len()),
        };
        file.prefetch(end_start, end_length as usize);
        end_start
    }
}

#[async_trait]
impl ExecutionPlan for ParquetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.files.len())
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.init_file(partition).await?;
        // because the parquet implementation is not thread-safe, it is necessary to execute
        // on a thread and communicate with channels
        let (response_tx, response_rx): (
            SyncSender<Option<ArrowResult<RecordBatch>>>,
            Receiver<Option<ArrowResult<RecordBatch>>>,
        ) = sync_channel(2);

        let file = self.files[partition].clone();
        let projection = self.projection.clone();
        let batch_size = self.batch_size;

        thread::spawn(move || {
            if let Err(e) = read_file(file, projection, batch_size, response_tx) {
                println!("Parquet reader thread terminated due to error: {:?}", e);
            }
        });

        Ok(Box::pin(ParquetStream {
            schema: self.projected_schema.clone(),
            response_rx,
        }))
    }
}

fn send_result(
    response_tx: &SyncSender<Option<ArrowResult<RecordBatch>>>,
    result: Option<ArrowResult<RecordBatch>>,
) -> DataFusionResult<()> {
    response_tx
        .send(result)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(())
}

fn read_file(
    file: CachedFile,
    projection: Vec<usize>,
    batch_size: usize,
    response_tx: SyncSender<Option<ArrowResult<RecordBatch>>>,
) -> DataFusionResult<()> {
    // TODO avoid footing being parsed twice here
    let file_reader = Arc::new(
        SerializedFileReader::new(file.clone())
            .map_err(|e| DataFusionError::ParquetError(e))?,
    );
    let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
    let mut batch_reader =
        arrow_reader.get_record_reader_by_columns(projection.clone(), batch_size)?;
    loop {
        match batch_reader.next() {
            Some(Ok(batch)) => send_result(&response_tx, Some(Ok(batch)))?,
            None => {
                // finished reading file
                send_result(&response_tx, None)?;
                break;
            }
            Some(Err(e)) => {
                let err_msg =
                    format!("Error reading batch from {:?}: {}", file, e.to_string());
                // send error to operator
                send_result(
                    &response_tx,
                    Some(Err(ArrowError::ParquetError(err_msg.clone()))),
                )?;
                // terminate thread with error
                return Err(DataFusionError::Execution(err_msg));
            }
        }
    }
    Ok(())
}

struct ParquetStream {
    schema: SchemaRef,
    response_rx: Receiver<Option<ArrowResult<RecordBatch>>>,
}

impl Stream for ParquetStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.response_rx.recv() {
            Ok(batch) => Poll::Ready(batch),
            // RecvError means receiver has exited and closed the channel
            Err(_) => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for ParquetStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::io::SeekFrom;
    use std::path::PathBuf;

    use super::*;
    use crate::clients::Downloader;
    use crate::clients::RangeCache;
    use crate::error::Result as BuzzResult;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_parquet::arrow::ArrowWriter;
    use async_trait::async_trait;
    use tokio::fs::File as TokioFile;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_small_parquet() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let rec_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let filename = "test_small_parquet.parquet";
        let results = write_and_exec(&rec_batch, filename).await;
        assert_eq!(results.len(), 1);
        assert_eq!(format!("{:?}", results[0]), format!("{:?}", rec_batch));
    }

    // Without the threaded_scheduler the async machine gets stuck because of blocking call
    #[tokio::test(threaded_scheduler)]
    async fn test_larger_parquet() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, false)]));
        let rec_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(
                (0..200_000).map(|val| val as f64 * 0.1).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();

        let filename = "test_larger_parquet.parquet";
        let results = write_and_exec(&rec_batch, filename).await;
        assert_eq!(
            results.iter().map(|rb| rb.num_rows()).sum::<usize>(),
            200_000
        );
    }

    /// Write the given `rec_batch` as a parquet file then make it into an exec plan
    async fn write_and_exec(rec_batch: &RecordBatch, filename: &str) -> Vec<RecordBatch> {
        let (tmp_file, path) = get_temp_file(filename);

        let mut writer =
            ArrowWriter::try_new(tmp_file.try_clone().unwrap(), rec_batch.schema(), None)
                .unwrap();
        writer.write(&rec_batch).unwrap();
        writer.close().unwrap();

        let file = CachedFile::new(
            path.into_os_string().into_string().unwrap(),
            tmp_file.metadata().unwrap().len(),
            Arc::new(RangeCache::new().await),
            "file_downloader".to_owned(),
            || Arc::new(FileDownloader {}),
        );

        let exec_plan = ParquetExec::new(vec![file], None, 2048, rec_batch.schema());

        datafusion::physical_plan::collect(Arc::new(exec_plan))
            .await
            .unwrap()
    }

    /// A downloader that simply reads from file system (file_id is the file path)
    #[derive(Clone)]
    struct FileDownloader {}

    #[async_trait]
    impl Downloader for FileDownloader {
        async fn download(
            &self,
            file_id: String,
            start: u64,
            length: usize,
        ) -> BuzzResult<Vec<u8>> {
            let mut result = vec![0; length];
            let mut f = TokioFile::open(&file_id).await?;
            f.seek(SeekFrom::Start(start)).await?;
            f.read_exact(&mut result).await?;
            Ok(result)
        }
    }

    /// Returns file handle for a temp file in 'target' directory with an empty content
    fn get_temp_file(file_name: &str) -> (fs::File, PathBuf) {
        // build tmp path to a file in "target/debug/testdata"
        let mut path_buf = env::current_dir().unwrap();
        path_buf.push("target");
        path_buf.push("debug");
        path_buf.push("testdata");
        fs::create_dir_all(&path_buf).unwrap();
        path_buf.push(file_name);

        // create empty file (truncate)
        fs::File::create(path_buf.as_path()).unwrap();

        // return file handle for both read and write
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path_buf.as_path());
        assert!(file.is_ok());
        (file.unwrap(), path_buf)
    }
}
