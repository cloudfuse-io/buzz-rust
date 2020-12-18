use std::convert::TryInto;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

use crate::flight_utils;
use crate::hcomb_service::HCombService;
use crate::protobuf::LogicalPlanNode;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaResult, Ticket,
};
use datafusion::logical_plan::LogicalPlan;
use futures::{Stream, StreamExt};
use prost::Message;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct FlightServiceImpl {
    hcomb_service: Arc<HCombService>,
}

impl FlightServiceImpl {
    pub fn new(hcomb_service: HCombService) -> Self {
        Self {
            hcomb_service: Arc::new(hcomb_service),
        }
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
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // parse request
        let ticket = request.into_inner().ticket;
        let plan_node =
            LogicalPlanNode::decode(&mut Cursor::new(ticket)).map_err(|_| {
                Status::invalid_argument("Plan could not be parsed from bytes")
            })?;
        let plan: LogicalPlan = (&plan_node).try_into().map_err(|_| {
            Status::invalid_argument("Plan could not be converted from proto")
        })?;
        // execute query
        let results = self
            .hcomb_service
            .execute_query(plan)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {}", e)))?;
        // serialize response
        let flights = flight_utils::batches_to_flight("query0", results)
            .await
            .map_err(|_| Status::internal("Plan could not be converted into flight"))?
            .map(|flt| Ok(flt));
        Ok(Response::new(Box::pin(flights)))
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
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let (cmd, batches) = flight_utils::flight_to_batches(request.into_inner())
            .await
            .map_err(|e| {
                Status::invalid_argument(format!("Invalid put request:{}", e))
            })?;

        self.hcomb_service.add_results(&cmd, batches).await;
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
