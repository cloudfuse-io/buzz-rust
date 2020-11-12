use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tonic::{Request, Response, Status, Streaming};

use arrow::datatypes::Schema;
use datafusion::datasource::parquet::ParquetTable;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;

use arrow_flight::{
    flight_service_server::FlightService, utils::flight_data_to_arrow_batch, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};

use crate::hive_query::IntermediateResults;

#[derive(Clone)]
pub struct FlightServiceImpl {
    pub result_service: Arc<IntermediateResults>,
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = Pin<
        Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>,
    >;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoActionStream = Pin<
        Box<
            dyn Stream<Item = Result<arrow_flight::Result, Status>>
                + Send
                + Sync
                + 'static,
        >,
    >;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let request = request.into_inner();

        let table = ParquetTable::try_new(&request.path[0]).unwrap();

        Ok(Response::new(SchemaResult::from(table.schema().as_ref())))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        match String::from_utf8(ticket.ticket.to_vec()) {
            Ok(sql) => {
                println!("do_get: {}", sql);

                // create local execution context
                let mut ctx = ExecutionContext::new();

                let testdata = std::env::var("PARQUET_TEST_DATA")
                    .expect("PARQUET_TEST_DATA not defined");

                // register parquet file with the execution context
                ctx.register_parquet(
                    "alltypes_plain",
                    &format!("{}/alltypes_plain.parquet", testdata),
                )
                .map_err(|e| to_tonic_err(&e))?;

                // create the query plan
                let plan = ctx
                    .create_logical_plan(&sql)
                    .and_then(|plan| ctx.optimize(&plan))
                    .and_then(|plan| ctx.create_physical_plan(&plan))
                    .map_err(|e| to_tonic_err(&e))?;

                // execute the query
                let results = ctx
                    .collect(plan.clone())
                    .await
                    .map_err(|e| to_tonic_err(&e))?;
                if results.is_empty() {
                    return Err(Status::internal("There were no results from ticket"));
                }

                // add an initial FlightData message that sends schema
                let schema = plan.schema();
                let mut flights: Vec<Result<FlightData, Status>> =
                    vec![Ok(FlightData::from(schema.as_ref()))];

                let mut batches: Vec<Result<FlightData, Status>> = results
                    .iter()
                    .map(|batch| Ok(FlightData::from(batch)))
                    .collect();

                // append batch vector to schema vector, so that the first message sent is the schema
                flights.append(&mut batches);

                let output = futures::stream::iter(flights);

                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {:?}", e))),
        }
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        mut request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        println!("metadata: {:?}", request.metadata());
        let header_flight_data = request.get_mut().message().await?.unwrap();
        let schema = Arc::new(Schema::try_from(&header_flight_data).unwrap());
        println!("Received Schema: {:?}", schema);
        if let Some(flight_descriptor) = header_flight_data.flight_descriptor {
            println!("flight_descriptor: {:?}", flight_descriptor);
        }

        while let Some(flight_data) = request.get_mut().message().await? {
            let batch = flight_data_to_arrow_batch(&flight_data, Arc::clone(&schema))
                .unwrap()
                .unwrap();
            // TODO get query id from flight descriptor
            self.result_service.add_result("test0", batch);
        }
        self.result_service.task_finished("test0");
        let output = futures::stream::empty();
        Ok(Response::new(Box::pin(output) as Self::DoPutStream))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

fn to_tonic_err(e: &datafusion::error::DataFusionError) -> Status {
    Status::internal(format!("{:?}", e))
}
