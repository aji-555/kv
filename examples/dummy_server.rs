use anyhow::Result;

use futures::prelude::*;
use kv::CommandRequest;
use prost::Message;
use tokio::{io::AsyncReadExt, net::TcpListener};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let (mut stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);
        tokio::spawn(async move {
            //
            let mut buf = vec![0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap();
            buf.truncate(n);
            let cmd = CommandRequest::decode(buf.as_slice()).unwrap();
            println!("{:?}", cmd.request_data);
        });
    }
}
