use std::error::Error;

use buzz::models::HBeeEvent;
use buzz::services::hbee::{HBeeService, HttpCollector};
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
    let (hbee_table_desc, sql, source) = hbee_event.plan.parse()?;
    tokio::spawn(async {
        let collector = Box::new(HttpCollector {});
        let mut hbee_service = HBeeService::new(collector).await;
        let res = hbee_service
            .execute_query(
                hbee_event.query_id,
                hbee_table_desc,
                sql,
                source,
                hbee_event.hcomb_address,
            )
            .await;
        match res {
            Ok(_) => println!("[hbee] success"),
            Err(e) => println!("[hbee] exec error: {}", e),
        };
    });
    Ok(Response::new(Body::from("Ok!")))
}

// this endpoint helps simulating lambda locally
pub async fn start_hbee_server() -> Result<(), DynError> {
    let make_svc = make_service_fn(|_conn| async { Ok::<_, DynError>(service_fn(exec)) });
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
