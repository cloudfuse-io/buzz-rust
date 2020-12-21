use std::error::Error;

use buzz::hbee_scheduler::HBeeEvent;
use buzz::hbee_service::HBeeService;
use futures::StreamExt;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

type DynError = Box<dyn Error + Send + Sync + 'static>;

async fn exec(mut req: Request<Body>) -> Result<Response<Body>, DynError> {
    println!("[hbee] hbee_server.serve()");
    let mut body = Vec::new();
    while let Some(chunk) = req.body_mut().next().await {
        body.extend_from_slice(&chunk?);
    }
    let hbee_event: HBeeEvent = serde_json::from_slice(&body)?;
    let hbee_service = HBeeService::new();
    hbee_service
        .execute_query(
            hbee_event.query_id,
            hbee_event.plan.parse()?,
            hbee_event.hcomb_address,
        )
        .await?;
    Ok(Response::new(Body::from("Ok!")))
}

async fn serve(req: Request<Body>) -> Result<Response<Body>, DynError> {
    exec(req).await.map_err(|e| {
        println!("[hbee] error: {}", e);
        e
    })
}

// this endpoint helps simulating lambda locally
pub async fn start_hbee_server() -> Result<(), DynError> {
    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, DynError>(service_fn(serve)) });
    let addr = ([0, 0, 0, 0], 3000).into();
    let server = Server::bind(&addr).serve(make_svc);
    println!("[hbee] Listening on http://{}", addr);
    server.await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    start_hbee_server().await
}
