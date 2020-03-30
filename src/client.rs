use crate::broker::BrokerMessage;
use crate::broker::BrokerMessage::*;
use std::collections::HashSet;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_util::codec::Framed;

use futures::{SinkExt, StreamExt};

use mqtt_codec::{Codec, ConnectCode::*, Packet, Packet::*};

#[derive(Debug)]
pub struct Client {
    pub id: String,
    addr: SocketAddr,
    pub topics: HashSet<String>,
    tx_broker: Sender<BrokerMessage>,
}

impl Client {
    pub fn new(id: String, addr: SocketAddr, tx_broker: Sender<BrokerMessage>) -> Client {
        Client {
            id,
            addr,
            topics: HashSet::new(),
            tx_broker,
        }
    }

    async fn handle_messages(
        framed: &mut Framed<TcpStream, Codec>,
        tx_broker: &mut Sender<BrokerMessage>,
        client_key: String,
        packet: Packet,
    ) {
        match packet {
            PingRequest => {
                println!("Ping");
                framed.send(PingResponse).await;
            }
            _ => {
                tx_broker
                    .send(Message {
                        id: client_key,
                        packet,
                    })
                    .await;
            }
        }
    }

    pub async fn handle_client(
        stream: TcpStream,
        addr: SocketAddr,
        tx_broker: &mut Sender<BrokerMessage>,
    ) {
        let mut framed = Framed::new(stream, Codec::new());

        // do connection handshake
        let packet = match framed.next().await {
            Some(Ok(Connect(packet))) => {
                framed
                    .send(ConnectAck {
                        session_present: false,
                        return_code: ConnectionAccepted,
                    })
                    .await;
                packet
            }
            _ => {
                println!("Did not receive connect packet");
                return;
            }
        };

        // create client
        let client_key = packet.client_id.to_string();
        let (tx_client, mut rx_client): (Sender<Packet>, Receiver<Packet>) = mpsc::channel(100);
        let client = Client::new(client_key.clone(), addr, tx_broker.clone());
        // send it to the broker
        tx_broker.send(NewConnection { client, tx_client }).await;

        loop {
            tokio::select! {
                Some(Ok(packet)) = framed.next() => {
                    match packet {
                        Disconnect => {
                            Client::handle_messages(&mut framed, tx_broker, client_key.clone(), packet).await;
                            break;
                        },
                        _ => {
                            Client::handle_messages(&mut framed, tx_broker, client_key.clone(), packet).await;
                        }

                    }
                },
                Some(packet) = rx_client.next() => {
                    //println!("Sending {:#?}", packet);
                    framed.send(packet).await.unwrap();
                },
                else => break
            }
        }

        println!("Connection with client {} ended", client_key);
    }
}
