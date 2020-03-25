mod broker;
use broker::Broker;

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr};


/*
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use mqtt_codec::Codec;
use mqtt_codec::Packet::*;
use mqtt_codec::ConnectCode::*;

use tokio::stream::{StreamExt};
use futures::SinkExt;

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

//use broker::*;


async fn handle_client(stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut packets = Framed::new(stream, Codec::new());

    let connect = match packets.next().await {
        Some(Ok(Connect(packet))) => {
            packets.send(ConnectAck{session_present: false, return_code: ConnectionAccepted}).await;
            packet
        },
        _ => {
            println!("Did not receive connect packet");
            return Ok(());
        }
    };
    println!("{:#?}", connect);
    while let Some(Ok(packet)) = packets.next().await {
        match packet {
            Disconnect => return Ok(()),
            PingRequest => {
                println!("Ping");
                packets.send(PingResponse).await;
            },
            Subscribe {packet_id: id, ..} => {
                println!("{:#?}", packet);
                packets.send(SubscribeAck {packet_id: id, status: vec!(mqtt_codec::SubscribeReturnCode::Success(mqtt_codec::QoS::ExactlyOnce))}).await;
            }
            _ => {
            }
        }
    }
    Ok(())
}
*/

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    /*let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1883);
    let mut listener = TcpListener::bind(address).await?;

    loop {
        let (stream, _) = listener.accept().await?;
            println!("New connection: {}", stream.peer_addr().unwrap());
            tokio::spawn(async move {
                // connection succeeded
                handle_client(stream).await;
        });
    }*/
    Broker::start_server(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1883).await?;
    Ok(())

}
