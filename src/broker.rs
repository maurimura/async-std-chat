use std::sync::Arc;
use async_std::{
    net::{TcpStream},
    prelude::*,
};

use std::collections::hash_map::{Entry, HashMap};
use futures::sink::SinkExt;
use futures::channel::mpsc;
use futures::{select, FutureExt};

use crate::types::{Receiver, Result, Sender, Event, Void};
use crate::utils::spawn_and_log_error;

async fn connection_writer_loop(
    messages: &mut Receiver<String>,
    stream: Arc<TcpStream>,
    shutdown: Receiver<Void>,
) -> Result<()> {
    let mut stream = &*stream;

    let mut messages = messages.fuse();
    let mut shutdown = shutdown.fuse();

    loop {
        select! {
            msg = messages.next().fuse() => match msg {
                Some(msg) => stream.write_all(msg.as_bytes()).await?,
                None => break,
            },
            void = shutdown.next().fuse() => match void {
                Some(void) => match void {}
                None => break
            }
        }
    }

    Ok(())
}

pub async fn broker_loop(events: Receiver<Event>) -> Result<()> {
    let (disconnect_sender, mut disconnect_receiver) =
        mpsc::unbounded::<(String, Receiver<String>)>();

    let mut peers: HashMap<String, Sender<String>> = HashMap::new();

    let mut events = events.fuse();

    loop {
        let event = select! {
            event = events.next().fuse() => match event {
                None => break,
                Some(event) => event
            },
            disconnect = disconnect_receiver.next().fuse() => {
                let (name, _pending_messages) = disconnect.unwrap();
                assert!(peers.remove(&name).is_some());
                continue;
            }
        };

        match event {
            Event::Message { from, to, msg } => {
                for addr in to {
                    if let Some(peer) = peers.get_mut(&addr) {
                        let msg = format!("from {}: {}\n", from, msg);
                        peer.send(msg).await?
                    }
                }
            }
            Event::NewPeer {
                name,
                stream,
                shutdown,
            } => match peers.entry(name.clone()) {
                Entry::Occupied(..) => (),
                Entry::Vacant(entry) => {
                    let (client_sender, mut client_receiver) = mpsc::unbounded();
                    entry.insert(client_sender);

                    let mut disconnect_sender = disconnect_sender.clone();

                    spawn_and_log_error(async move {
                        let res =
                            connection_writer_loop(&mut client_receiver, stream, shutdown).await;
                        disconnect_sender
                            .send((name, client_receiver))
                            .await
                            .unwrap();
                        res
                    });
                }
            },
        };
    }

    drop(peers);
    drop(disconnect_sender);

    while let Some((_name, _pending_messages)) = disconnect_receiver.next().await {}

    Ok(())
}
