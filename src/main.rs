use async_std::{
    net::{TcpListener, ToSocketAddrs},
    prelude::*,
    task,
};

use futures::channel::mpsc;



mod utils;
mod types;
mod connection;
mod broker;

use broker::broker_loop;
use types::{Result};
use utils::spawn_and_log_error;
use connection::connection_loop;

async fn accept_loop(addr: impl ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    let (broker_sender, broker_receiver) = mpsc::unbounded();
    let broker_handle = task::spawn(broker_loop(broker_receiver));
    let mut incoming = listener.incoming();

    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        println!("Accepting from: {}", stream.peer_addr()?);
        spawn_and_log_error(connection_loop(broker_sender.clone(), stream));
    }

    drop(broker_sender);
    broker_handle.await?;

    Ok(())
}

fn run() -> Result<()> {
    let fut = accept_loop("127.0.0.1:8080");
    task::block_on(fut)
}

fn main() {
    run().unwrap();
}
