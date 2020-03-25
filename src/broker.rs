use std::collections::HashMap;
use std::sync::Arc;
use std::ops::DerefMut;

use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tokio::sync::Mutex;
use tokio::time;
use tokio::time::{Duration};

use mqtt_codec::Codec;
use mqtt_codec::Packet::*;
use mqtt_codec::ConnectCode::*;

//use tokio::stream::StreamExt;

use futures::{StreamExt, SinkExt};

use std::error::Error;
use std::net::{IpAddr, SocketAddr};

use sequence_trie::SequenceTrie;
use bytes::Bytes;
use bytestring::ByteString;


#[derive(Debug)]
struct Client {
    id: String,
    addr: SocketAddr,
    topics: Vec<String>,
    framed: Arc<Mutex<Framed<TcpStream, Codec>>>
}

pub struct Broker;

struct Topic {
    name: String,
    subscribers: Vec<String>,
}

struct BrokerState {
    clients: HashMap<String, Client>,
    topics: SequenceTrie<String, Topic>

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
            topics: SequenceTrie::new()
        }
    }
}

impl Client {
    pub fn new(id: String, addr: SocketAddr, framed: Arc<Mutex<Framed<TcpStream, Codec>>>) -> Client {
        Client {
            id: id,
            addr: addr,
            topics: Vec::new(),
            framed: framed,
        }
    }

    fn process_results()
}

impl Broker {

    pub async fn start_server(addr: IpAddr, port: u16) -> Result<(), Box<dyn Error>> {
        let address = SocketAddr::new(addr, port);
        let mut listener = TcpListener::bind(address).await?;
        let state = Arc::new(Mutex::new(BrokerState::new()));

        {
            let state_inner = Arc::clone(&state);
            {
                let mut state_mutex = state_inner.lock().await;

                let path_str = "$/timer".to_string();
                let path = path_str.split('/');

                state_mutex.topics.insert(path, Topic::new(path_str.clone()));
            }
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(10));
                while let Some(_) = interval.next().await {
                    //println!("sending message");
                    Broker::send_publish(Arc::clone(&state_inner), Bytes::from("foo"), ByteString::from("$/timer"), None).await;
                }
            });
        }

        loop {
            let (stream, addr) = listener.accept().await?;
                println!("New connection: {}", stream.peer_addr().unwrap());

                let state_inner = Arc::clone(&state);

                tokio::spawn(async move {
                    // connection succeeded
                    Broker::handle_client(state_inner, stream, addr).await;
            });
        }
    }

    async fn handle_client(state: Arc<Mutex<BrokerState>>, stream: TcpStream, addr: SocketAddr) -> Result<(), Box<dyn Error + Send>> {

        let packets_mutex = Arc::new(Mutex::new(Framed::new(stream, Codec::new())));
        let client_key;

        {
            let mut packets = packets_mutex.lock().await;
            client_key = match packets.next().await {
                Some(Ok(Connect(packet))) => {
                    packets.send(ConnectAck{session_present: false, return_code: ConnectionAccepted}).await;

                    let key = packet.client_id.to_string();
                    let client = Client::new(key.clone(), addr, Arc::clone(&packets_mutex));

                    let mut state = state.lock().await;
                    state.clients.insert(key.clone(), client);
                    key
                },
                _ => {
                    println!("Did not receive connect packet");
                    return Ok(());
                }
            };
        }

        let mut packets = packets_mutex.lock().await;
        println!("{:#?}", client_key);
        while let Some(Ok(packet)) = packets.next().await {
            match packet {
                Disconnect =>{
                    let mut state = state.lock().await;
                    state.clients.remove(&client_key);
                    println!("{:#?}", state.clients);
                    return Ok(())
                },
                PingRequest => {
                    println!("Ping");
                    packets.send(PingResponse).await;
                },
                Subscribe {packet_id: id, topic_filters: topics} => {
                    println!("Recieved subscribe to: {:#?}", topics);
                    packets.send(SubscribeAck {packet_id: id, status: vec!(mqtt_codec::SubscribeReturnCode::Success(mqtt_codec::QoS::ExactlyOnce))}).await;

                    let mut state_guard = state.lock().await;
                    let state = state_guard.deref_mut();

                    let client = state.clients.get_mut(&client_key).unwrap();
                    for (topic, _) in topics {
                        client.topics.push(topic.to_string());
                        let path = topic.to_string();
                        let path = path.split("/");
                        match state.topics.get_mut(path.clone())  {
                            Some(topic) => {
                                // add the client to the existing subscribers
                                println!("topic already exists");
                                topic.subscribers.push(client_key.clone());
                            },
                            None => {
                                state.topics.insert(path.clone(), Topic::new(topic.to_string()));
                            }
                        }
                    }
                },
                _ => {
                }
            }
        }
        Ok(())
    }

    async fn send_publish(state: Arc<Mutex<BrokerState>>, message: Bytes, topic: ByteString, packet_id: Option<u16>) {
        let state = state.lock().await;
        let path = topic.split('/');

        let clients_to_send = &state.topics.get(path.clone()).unwrap().subscribers;
        for client_name in clients_to_send {
            let client = state.clients.get(client_name).unwrap();
            // send stuff using client's tx/framed
            let mut framed = client.framed.lock().await;
            println!("sending packet to {}",client_name);
            framed.send(Publish(mqtt_codec::Publish {
                dup: false,
                retain: false,
                qos:mqtt_codec::QoS::AtLeastOnce,
                packet_id: packet_id,
                topic: topic.clone(),
                payload: message.clone()
            })).await;
        }
    }
}