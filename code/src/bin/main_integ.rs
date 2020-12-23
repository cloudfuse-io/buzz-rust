use tracing::info;
use tracing_subscriber::fmt::{time::ChronoUtc, SubscriberBuilder};

mod main_fuse;
mod main_hbee;
mod main_hcomb;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    SubscriberBuilder::default().init();
    // SubscriberBuilder::default()
    //     .json()
    //     .with_timer(ChronoUtc::rfc3339())
    //     .init();

    tokio::select! {
        res = main_fuse::start_fuse("localhost", "localhost") => {
            info!("fuse result: {:?}", res);
        }
        res = main_hbee::start_hbee_server() => {
            info!("hbee server failed: {:?}", res);
        }
        res = main_hcomb::start_hcomb_server() => {
            info!("hcomb server failed: {:?}", res);
        }
    }
    Ok(())
}
