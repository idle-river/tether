use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast::{self, error::RecvError},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:9001")
        .await
        .expect("Failed to bind to port 9001");
    let (tx, _) = broadcast::channel::<String>(256);

    while let Ok((stream, addr)) = listener.accept().await {
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let _ = handle(stream, addr, tx_clone).await;
        });
    }
}

async fn handle(stream: TcpStream, addr: SocketAddr, tx: broadcast::Sender<String>) -> Result<()> {
    // Does the HTTP upgrade handshake on the raw TCP stream.
    let ws = accept_async(stream).await?;
    let (mut sink, mut source) = ws.split();
    let mut rx = tx.subscribe();

    loop {
        tokio::select! {
            // Client -> server
            incoming = source.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    let _ = tx.send(format!("{addr}: {text}"));
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {} // Binary / Ping / Pong
                Some(Err(e)) => return Err(e.into()),
            },
            // Broadcast -> this client
            line = rx.recv() => match line {
                Ok(line) => sink.send(Message::Text(line.into())).await?,
                Err(RecvError::Lagged(_)) => continue, // slow client missed messages
                Err(RecvError::Closed) => break,
            },
        }
    }
    Ok(())
}
