mod main_fuse;
mod main_hbee;
mod main_hcomb;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::select! {
        res = main_fuse::start_fuse("localhost", "localhost") => {
            println!("fuse result: {:?}", res);
        }
        res = main_hbee::start_hbee_server() => {
            println!("hbee server failed: {:?}", res);
        }
        res = main_hcomb::start_hcomb_server() => {
            println!("hcomb server failed: {:?}", res);
        }
    }
    Ok(())
}
