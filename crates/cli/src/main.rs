use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() {
    let (ws, _response) = connect_async("ws://127.0.0.1:9001")
        .await
        .expect("Could not connect to server");
    let (mut sink, mut source) = ws.split();

    // Receive task: print whatever the server sends.
    let recv = tokio::spawn(async move {
        while let Some(Ok(msg)) = source.next().await {
            if let Message::Text(text) = msg {
                println!("{text}");
            }
        }
    });

    // Send loop: stdin lines -> server.
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await.unwrap() {
        sink.send(Message::Text(line.into())).await.unwrap();
    }

    sink.send(Message::Close(None)).await.unwrap();
    recv.abort();
}
