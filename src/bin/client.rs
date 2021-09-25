use futures::sink::SinkExt;
use futures::stream::StreamExt;
use nym_addressing::clients::Recipient;
use nym_duplex::socks::receive_request;
use nym_duplex::transport::{ConnectionId, ConnectionMessage, Packet, Payload};
use nym_websocket::responses::ServerResponse;
use rand::Rng;
use std::collections::BTreeMap;
use structopt::StructOpt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, trace, warn};
use tracing_subscriber::EnvFilter;

#[derive(StructOpt)]
struct Options {
    #[structopt(short, long, default_value = "ws://127.0.0.1:1977")]
    websocket: String,
    #[structopt(
        long,
        parse(try_from_str = Recipient::try_from_base58_string),
        help = "Exit node address",
    )]
    service_provider: Recipient,
    #[structopt(
        long,
        default_value = "127.0.0.1:1080",
        help = "Address the socks server should listen on"
    )]
    socks_bind_addr: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let opts: Options = StructOpt::from_args();

    let (mut ws, _) = connect_async(&opts.websocket)
        .await
        .expect("Couldn't connect to nym websocket");

    let listener = TcpListener::bind(&opts.socks_bind_addr).await.unwrap();

    let (outgoing_sender, mut outgoing_receiver) =
        tokio::sync::mpsc::channel::<ConnectionMessage<Packet>>(4);

    let mut connections = BTreeMap::<ConnectionId, Sender<Packet>>::new();

    loop {
        select! {
            connection_res = listener.accept() => {
                let (connection, _) = connection_res.expect("Listener died");
                tokio::spawn(handle_connection(connection, outgoing_sender.clone()));
            },
            ws_packet_res = ws.next() => {
                let ws_packet = ws_packet_res.and_then(Result::ok).expect("Web socket stream died");
                let nym_bytes = match ws_packet {
                    Message::Binary(bin) => bin,
                    _ => {
                        warn!("Received non-binary packet from websocket");
                        continue;
                    },
                };

                let bytes = match ServerResponse::deserialize(&nym_bytes) {
                    Ok(ServerResponse::Received(msg)) => msg.message,
                    _ => {
                        warn!("Received unexpected packet from websocket");
                        continue;
                    }
                };

                let packet: Packet = match bincode::deserialize(&bytes) {
                    Ok(packet) => packet,
                    Err(_) => {
                        warn!("Received malformed packet from websocket");
                        continue;
                    }
                };

                let stream = packet.stream;
                let stream_sender = match connections.get_mut(&stream) {
                    Some(stream_sender) => stream_sender,
                    None => {
                        warn!("Received packet for unknown stream {:?}", stream);
                        continue;
                    }
                };
                if stream_sender.send(packet).await.is_err() {
                    connections.remove(&stream);
                }
            }
            outgoing_res = outgoing_receiver.recv() => {
                let outgoing = outgoing_res.expect("Outgoing channel closed, this should not happen!");
                match outgoing {
                    ConnectionMessage::Establish { connection, sender } => {
                        connections.insert(connection, sender);
                    },
                    ConnectionMessage::Data(packet) => {
                        let nym_packet = nym_websocket::requests::ClientRequest::Send {
                            recipient: opts.service_provider,
                            message: bincode::serialize(&packet).expect("serialization can't fail"),
                            with_reply_surb: true,
                        };

                        ws.send(Message::Binary(nym_packet.serialize()))
                            .await
                            .expect("couldn't send websocket packet");
                    }
                }
            }
        }
    }
}

async fn handle_connection(mut socket: TcpStream, out: Sender<ConnectionMessage<Packet>>) {
    let connection: ConnectionId = rand::thread_rng().gen();
    let (incoming_sender, mut incoming_receiver) = tokio::sync::mpsc::channel(4);
    out.send(ConnectionMessage::Establish {
        connection,
        sender: incoming_sender,
    })
    .await
    .expect("Outgoing channel failed");

    let socks_reqest = match receive_request(&mut socket).await {
        Ok(request) => request,
        Err(e) => {
            error!("Socks connection error: {:?}", e);
            return;
        }
    };

    info!("Received request for {}", socks_reqest.fqdn);
    let mut last_received_idx = 0;
    let mut last_sent_idx = 0;
    let mut resend_timer = tokio::time::interval(tokio::time::Duration::from_secs(1));

    let mut last_sent_unack = Some(Packet {
        stream: connection,
        ack: last_received_idx,
        payload: Payload::Establish {
            host: socks_reqest.fqdn,
            port: socks_reqest.port,
        },
    });

    let mut buffer = [0u8; 500];
    loop {
        select! {
            _ = resend_timer.tick() => {
                if let Some(last_sent_unack) = &last_sent_unack {
                    trace!("Resending message {}", last_sent_unack.get_idx().unwrap());
                    out.send(ConnectionMessage::Data(last_sent_unack.clone()))
                        .await
                        .expect("Outgoing channel failed");
                } else {
                    trace!("Sending SURB");
                    let packet = Packet {
                        stream: connection,
                        ack: last_received_idx,
                        payload: Payload::SURB,
                    };
                    out.send(ConnectionMessage::Data(packet))
                        .await
                        .expect("Outgoing channel failed");
                }
            }
            incoming_res = incoming_receiver.recv() => {
                let incoming = incoming_res.expect("Incoming channel died, this should not happen.");

                // If the received ACK is for our last-sent message we can forget it
                if last_sent_unack.as_ref().map(|unack| unack.get_idx().unwrap() == incoming.ack).unwrap_or(false) {
                    last_sent_unack = None;
                }

                match incoming.payload {
                    Payload::Establish {..} => {
                        error!("We received an establish from the exit node, closing the connection.");
                        return;
                    },
                    Payload::Data { idx, data } => {
                        let expected_idx = last_received_idx + 1;
                        if idx == expected_idx {
                            if socket.write_all(&data).await.is_err() {
                                warn!("Connection closed");
                                return;
                            }
                            last_received_idx = idx;
                        } else {
                            warn!("Received unexpected message {}, expected {}", idx, expected_idx);
                        }

                        let packet = Packet {
                            stream: connection,
                            ack: last_received_idx,
                            payload: Payload::SURB,
                        };
                        out.send(ConnectionMessage::Data(packet))
                            .await
                            .expect("Outgoing channel failed");
                    },
                    Payload::SURB => {
                        trace!("Ignoring empty SURB answer");
                    }
                }
            }
            read_res = socket.read(&mut buffer), if last_sent_unack.is_none() => {
                let read = match read_res {
                    Ok(read) => read,
                    Err(e) => {
                        error!("Socks connection error: {:?}", e);
                        return;
                    }
                };

                if read == 0 {
                    continue;
                }

                last_sent_idx += 1;
                let packet = Packet {
                    stream: connection,
                    ack: last_received_idx,
                    payload: Payload::Data {
                        idx: last_sent_idx,
                        data: buffer[0..read].to_vec()
                    },
                };
                out.send(ConnectionMessage::Data(packet.clone()))
                    .await
                    .expect("Outgoing channel failed");
                last_sent_unack = Some(packet);
            }
        }
    }
}
