use std::convert::Infallible;

use buzz::hbee_service::HBeeService;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

async fn serve(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    println!("Body: {:?}", req.body());
    let hbee_service = HBeeService {};
    hbee_service.execute_query().await;
    Ok(Response::new(Body::from("Ok!")))
}

// this endpoint helps simulating lambda locally
pub async fn start_hbee_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(serve)) });
    let addr = ([0, 0, 0, 0], 3000).into();
    let server = Server::bind(&addr).serve(make_svc);
    println!("Listening on http://{}", addr);
    server.await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_hbee_server().await
}
