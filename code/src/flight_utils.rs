//! Utils to convert flight objects to and from record batches

use std::convert::TryFrom;
use std::error::Error;
use std::sync::Arc;

use crate::internal_err;
use arrow::datatypes::Schema;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::utils::{
    flight_data_from_arrow_batch, flight_data_from_arrow_schema,
    flight_data_to_arrow_batch,
};
use arrow_flight::{flight_descriptor, FlightData, FlightDescriptor};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use tonic::Status;

/// Convert a flight stream to a tuple with the cmd in the first flight and a stream of RecordBatch
pub async fn flight_to_batches(
    flights: tonic::Streaming<FlightData>,
) -> Result<(String, impl Stream<Item = ArrowResult<RecordBatch>>), Box<dyn Error>> {
    let mut flights = Box::pin(flights);
    let flight_data = flights.next().await.unwrap()?;
    let schema = Arc::new(Schema::try_from(&flight_data)?);
    let cmd = descriptor_to_cmd(flight_data.flight_descriptor)?;

    // all the remaining stream messages should be dictionary and record batches
    let record_batch_stream = flights.map(move |flight_data_res| match flight_data_res {
        Ok(flight_data) => {
            flight_data_to_arrow_batch(&flight_data, Arc::clone(&schema)).unwrap()
        }
        Err(e) => Err(ArrowError::ExternalError(Box::new(e))),
    });
    Ok((cmd, record_batch_stream))
}

/// Convert a stream of RecordBatches and a cmd to a stream of flights
pub async fn batch_stream_to_flight(
    cmd: &str,
    batches: SendableRecordBatchStream,
) -> Result<impl Stream<Item = Result<FlightData, Status>> + Send + Sync, Box<dyn Error>>
{
    // TODO are all this IpcWriteOptions creations a problem?
    let (sender, result) =
        tokio::sync::mpsc::unbounded_channel::<Result<FlightData, Status>>();

    // create an initial FlightData message that sends schema
    let options = IpcWriteOptions::default();
    let mut flight_schema = flight_data_from_arrow_schema(&batches.schema(), &options);
    flight_schema.flight_descriptor = cmd_to_descriptor(&cmd);
    sender.send(Ok(flight_schema))?;

    // use channels to make stream sync (required by tonic)
    // TODO what happens with errors (currently all unwrapped in spawned task)
    tokio::spawn(async move {
        // then stream the rest
        batches
            .for_each(|batch_res| async {
                let options = IpcWriteOptions::default();
                match batch_res {
                    Ok(batch) => {
                        flight_data_from_arrow_batch(&batch, &options)
                            .into_iter()
                            .for_each(|flight| (&sender).send(Ok(flight)).unwrap());
                    }
                    Err(err) => {
                        (&sender)
                            .send(Err(Status::aborted(format!("{}", err))))
                            .unwrap();
                    }
                };
            })
            .await;
    });

    Ok(result)
}

/// Convert a vector of RecordBatches and a cmd to a stream of flights
/// If there are no batches (empty vec), a flight with an empty schema is sent
pub async fn batch_vec_to_flight(
    cmd: &str,
    batches: Vec<RecordBatch>,
) -> Result<impl Stream<Item = FlightData>, Box<dyn Error>> {
    let schema;
    if batches.len() == 0 {
        schema = Arc::new(Schema::empty());
    } else {
        schema = batches[0].schema();
    }
    // create an initial FlightData message that sends schema
    let options = IpcWriteOptions::default();
    let mut flight_schema = flight_data_from_arrow_schema(&schema, &options);
    flight_schema.flight_descriptor = cmd_to_descriptor(&cmd);

    let mut flight_vec = vec![flight_schema];

    let mut batches: Vec<FlightData> = batches
        .iter()
        .flat_map(|batch| flight_data_from_arrow_batch(batch, &options))
        .collect();

    // append batch vector to schema vector, so that the first message sent is the schema
    flight_vec.append(&mut batches);

    Ok(futures::stream::iter(flight_vec))
}

fn cmd_to_descriptor(cmd: &str) -> Option<FlightDescriptor> {
    Some(FlightDescriptor {
        r#type: flight_descriptor::DescriptorType::Cmd as i32,
        cmd: cmd.as_bytes().to_owned(),
        path: vec![],
    })
}

fn descriptor_to_cmd(
    descriptor: Option<FlightDescriptor>,
) -> Result<String, Box<dyn Error>> {
    let descriptor = descriptor.ok_or(Box::new(internal_err!(
        "Descriptor not found in first flight"
    )))?;
    if descriptor.r#type != flight_descriptor::DescriptorType::Cmd as i32 {
        Err(Box::new(internal_err!("Descriptor type should be cmd")))
    } else {
        Ok(String::from_utf8(descriptor.cmd).unwrap())
    }
}
