use anyhow::Result;

use bytes::BytesMut;
use futures::prelude::*;
use kv::{CommandRequest, CommandResponse};
use prost::Message;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    // 连接服务器
    let stream = TcpStream::connect(addr).await?;
    let mut stream = Framed::new(stream, LengthDelimitedCodec::new());

    // // 使用 AsyncProstStream 来处理 TCP Frame
    // let mut client =
    //     AsyncBincodeStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    // 生成一个 HSET 命令
    let cmd = CommandRequest::new_hset("table2", "heetrtllo", "yrtyytytrytr".into());
    println!("1: {}", cmd.encoded_len());

    let mut buf = BytesMut::new();

    let _ = cmd.encode(&mut buf)?;
    println!("2: {}", buf.len());

    stream.send(buf.freeze()).await?;

    // // 发送 HSET 命令
    // client.send(cmd).await?;
    // if let Some(Ok(data)) = client.next().await {
    //     info!("Got response {:?}", data);
    // }

    // buf.clear();

    // buf.resize(1024, 0u8);

    // let n = stream.read_buffer(&mut buf).await.unwrap();

    // buf.truncate(n);

    // let resp = CommandResponse::decode(buf)?;

    // println!("{}", resp.status);

    Ok(())
}
