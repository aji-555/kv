use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::thread;

use blake3::Hasher;
use rayon::prelude::*;

use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};

use tokio_util::codec::{Framed, LinesCodec};

pub const PREFIX_ZERO: &[u8] = &[0, 0, 0];

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9000";
    let listener = TcpListener::bind(addr).await?;
    println!("listen to: {}", addr);

    let (sender, mut receiver) = mpsc::channel::<(String, oneshot::Sender<String>)>(1024);
    thread::spawn(move || {
        while let Some((line, reply)) = receiver.blocking_recv() {
            let result = match pow(&line) {
                Some((hash, nonce)) => format!("hash: {}, nonce: {}", hash, nonce),
                None => "Not found".to_string(),
            };

            // 把计算结果从 oneshot channel 里发回
            if let Err(e) = reply.send(result) {
                println!("Failed to send: {}", e);
            }
        }
    });

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("accepted: {}", addr);

        let sender_ = sender.clone();

        tokio::spawn(async move {
            let frame = Framed::new(stream, LinesCodec::new());

            let (mut writer, mut reader) = frame.split();

            while let Some(line) = reader.next().await {
                let (reply, reply_receiver) = oneshot::channel();
                sender_.send((line?, reply)).await?;

                if let Ok(v) = reply_receiver.await {
                    writer.send(v).await?;
                }
            }
            Ok::<_, anyhow::Error>(())
        });
    }
}

pub fn pow(s: &str) -> Option<(String, u32)> {
    let hasher = blake3_base_hash(s.as_bytes());
    let nonce = (0..u32::MAX).into_par_iter().find_any(|n| {
        let hash = blake3_hash(hasher.clone(), n).as_bytes().to_vec();
        &hash[..PREFIX_ZERO.len()] == PREFIX_ZERO
    });
    nonce.map(|n| {
        let hash = blake3_hash(hasher, &n).to_hex().to_string();
        (hash, n)
    })
}

// 计算携带 nonce 后的哈希
fn blake3_hash(mut hasher: blake3::Hasher, nonce: &u32) -> blake3::Hash {
    hasher.update(&nonce.to_be_bytes()[..]);
    hasher.finalize()
}

// 计算数据的哈希
fn blake3_base_hash(data: &[u8]) -> Hasher {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher
}
