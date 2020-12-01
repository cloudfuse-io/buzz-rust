use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;

use crate::results_service::ResultsService;
use arrow::datatypes::Schema;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{
    flight_descriptor, flight_service_server::FlightService,
    utils::flight_data_to_arrow_batch, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket,
};
use futures::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct FlightServiceImpl {
    results_service: Arc<ResultsService>,
}

impl FlightServiceImpl {
    pub fn new(results_service: Arc<ResultsService>) -> Self {
        Self { results_service }
    }

    pub async fn start(&self) -> tokio::task::JoinHandle<()> {
        let addr = "0.0.0.0:50051".parse().unwrap();
        let svc = FlightServiceServer::new(self.clone());
        tokio::spawn(async move {
            println!("Listening on {:?}", addr);
            Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .unwrap();
        })
    }
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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
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
        // the first batch is metadata
        let header_flight_data = request.get_mut().message().await?.unwrap();

        // parse the schema
        let schema = Arc::new(Schema::try_from(&header_flight_data).unwrap());
        println!("Received Schema: {:?}", schema);

        // unwrap query id
        let mut query_id = String::new();
        if let Some(desc) = header_flight_data.flight_descriptor {
            if desc.r#type == flight_descriptor::DescriptorType::Cmd as i32 {
                query_id = String::from_utf8(desc.cmd).unwrap_or(String::new());
            }
        }
        if query_id.is_empty() {
            return Err(Status::invalid_argument(
                "FlightDescriptor should containe a Cmd with the ut8 encoding of the query_id",
            ));
        }

        while let Some(flight_data) = request.get_mut().message().await? {
            let batch = flight_data_to_arrow_batch(&flight_data, Arc::clone(&schema))
                .unwrap()
                .unwrap();
            self.results_service.add_result(&query_id, batch);
        }
        self.results_service.task_finished(&query_id);
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

// fn to_tonic_err(e: &datafusion::error::DataFusionError) -> Status {
//     Status::internal(format!("{:?}", e))
// }
