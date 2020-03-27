use std::collections::HashMap;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc;
use tokio::stream::StreamExt;
use std::error::Error;
use std::net::{IpAddr, SocketAddr};
//use std::sync::Arc;
//use std::ops::DerefMut;

use tokio::net::{TcpListener};
//use tokio::sync::Mutex;
//use tokio::time;
//use tokio::time::{Duration};

use mqtt_codec::{Packet, SubscribeReturnCode, QoS, Packet::*};



use sequence_trie::SequenceTrie;
use bytes::Bytes;
use bytestring::ByteString;

use crate::client::Client;


pub struct Broker;

pub enum BrokerMessage {
    NewConnection {client: Client, tx_client: Sender<Packet>},
    Message {packet: Packet, id: String},
}

#[derive(Debug)]
struct Topic {
    name: String,
    subscribers: Vec<(String, QoS)>,
}

struct BrokerState {
    clients: HashMap<String, (Client, Sender<Packet>)>,
    topics: SequenceTrie<String, Topic>,
}

impl Topic {
    pub fn new(path: String) -> Topic {
        Topic {
            name: path,
            subscribers: Vec::new()
        }
    }
}

impl BrokerState {
    fn new() -> BrokerState {
        BrokerState {
            clients: HashMap::new(),
            topics: SequenceTrie::new(),
        }
    }
}


impl Broker {
    pub async fn start_server(addr: IpAddr, port: u16) -> Result<(), Box<dyn Error>> {
        let address = SocketAddr::new(addr, port);
        let mut listener = TcpListener::bind(address).await?;
        let (tx_broker, mut rx_broker): (Sender<BrokerMessage>, Receiver<BrokerMessage>) = mpsc::channel(100);
        let mut state = BrokerState::new();

        /*
        let path_str = "$/timer".to_string();
        let path = path_str.split('/');
        state.topics.insert(path, Topic::new(path_str.clone()));

        {
            let mut tx = tx_broker.clone();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(10));

                while let Some(_) = interval.next().await {
                    //println!("sending message");
                    tx.send(BrokerMessage::Message{
                        packet: Publish (mqtt_codec::Publish{
                            dup: false,
                            retain: false,
                            qos:mqtt_codec::QoS::AtLeastOnce,
                            packet_id: Some(10),
                            topic: ByteString::from("$/timer"),
                            payload: Bytes::from("foo"),
                        }),
                        id: "foobar".to_string()
                    }).await;

                }
            });
        }*/

        tokio::spawn(async move {
            loop {
                let (stream, addr) = listener.accept().await.unwrap();
                println!("New connection: {}", stream.peer_addr().unwrap());
                let mut tx_broker = tx_broker.clone();
                tokio::spawn(async move {
                    Client::handle_client(stream, addr, &mut tx_broker).await;
                });
            }
        });
        while let Some(message) = rx_broker.next().await {
            match message {
                BrokerMessage::NewConnection {client, tx_client} => {
                    let id = client.id.clone();
                    state.clients.insert(id.clone(), (client, tx_client));
                    println!("Registered Client: {}", id);
                },
                BrokerMessage::Message {packet, id} => {
                    let (client, tx) = state.clients.get_mut(&id).unwrap();
                    println!("{:#?}", packet);
                    match packet {
                        Disconnect => {
                            for topic_str in &client.topics {
                                let topic = topic_str.split('/');
                                state.topics.remove(topic);
                            }
                            state.clients.remove(&id);
                            println!("{:#?}", state.clients);
                        },
                        // TODO: handle wildcards and store QoS for the client
                        Subscribe{packet_id, topic_filters} => {
                            let mut qos_response: Vec<SubscribeReturnCode> = Vec::new();
                            for (topic, qos) in topic_filters {
                                let path_str = topic.to_string();
                                client.topics.push(path_str.clone());
                                let path = path_str.split('/');
                                println!("{:?}", path.clone().collect::<Vec<_>>());
                                match state.topics.get_mut(path.clone())  {
                                    Some(topic) => {
                                        // add the client to the existing subscribers, if they aren't already
                                        println!("topic already exists");
                                        if !client.topics.contains(&topic.name) {
                                            topic.subscribers.push((id.clone(), qos));
                                        }
                                    },
                                    None => {
                                        let mut topic = Topic::new(topic.to_string());
                                        topic.subscribers.push((id.clone(), qos));
                                        state.topics.insert(path.clone(), topic);
                                    }
                                };
                                println!("{:#?}", state.topics.get_prefix_nodes(path.clone()));
                                qos_response.push(SubscribeReturnCode::Success(qos));
                            }
                            tx.send(SubscribeAck{packet_id, status: qos_response}).await;
                        },
                        Publish(pub_packet) => {
                            // TODO: handle dups/wildcards
                            Broker::send_publish(&mut state, pub_packet.payload, pub_packet.topic, pub_packet.packet_id).await;
                        },
                        PublishAck{..} => {
                            // TODO: remove from map
                        },
                        PublishReceived {packet_id} => {
                            tx.send(PublishRelease {packet_id}).await;
                        },
                        PublishRelease {packet_id} => {
                            tx.send(PublishReceived {packet_id}).await;
                        },
                        PublishComplete {..} => {
                            // TODO: remove from map
                        },
                        _ => {

                        }
                    }
                }
            }
        };
        Ok(())
    }

    async fn send_publish(state: &mut BrokerState, message: Bytes, topic: ByteString, packet_id: Option<u16>) {
        let path_str = topic.clone().to_string();
        let path = path_str.split('/');
        //println!("{:#?}", path);
        let clients_to_send = &match state.topics.get(path.clone()) {
            Some(val) => val,
            None => {
                let topic = Topic::new(topic.to_string());
                state.topics.insert(path.clone(), topic);
                &state.topics.get(path.clone()).unwrap()
            }
        }.subscribers;

        /*let clients_to_send = state.topics.get(path.clone()).unwrap_or_else(|| {
            let topic = Topic::new(topic.to_string());
            state.topics.insert(path.clone(), topic);
            topic
        }).subscribers;*/
        //println!("{:#?}", state.topics);
        for (client_name, qos) in clients_to_send {
            let (_, tx) = state.clients.get_mut(client_name).unwrap();
            println!("sending packet to {} on topic {} with message: {:#?}", client_name, topic, message);
            tx.send(Publish(mqtt_codec::Publish {
                dup: false,
                retain: false,
                qos: *qos,
                packet_id,
                topic: topic.clone(),
                payload: message.clone()
            })).await;
            println!("sent publish packet to {}",client_name);

            match qos {
                // if packet_id is None, the Codec failed and we are in a world of hurt
                QoS::AtLeastOnce => {
                    tx.send(PublishAck{
                        packet_id: packet_id.unwrap()
                    }).await;
                },
                QoS::ExactlyOnce => {
                    // TODO: add to map of awaiting publishes
                    tx.send(PublishReceived{
                        packet_id: packet_id.unwrap()
                    }).await;
                },
                QoS::AtMostOnce => {
                    // don't need to do anything
                }
            };
        }
    }

}