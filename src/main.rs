
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr};

mod broker;
mod client;

use broker::Broker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    Broker::start_server(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1883).await?;
    Ok(())

}