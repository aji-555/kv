use crate::*;
use bytes::BytesMut;
use bytes::{Buf, BufMut};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use http::header;
use prost::Message;
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

/// 长度为 4 个字节
pub const LEN_LEN: usize = 4;
/// 最大帧长度为 30bit，1G
const MAX_FRAME: usize = 1 * 1024 * 1024 * 1024;
/// 当 payload 大于 1436 字节就做压缩
const COMPRESSION_LIMIT: usize = 1436;

/// 代表压缩的 bit，4 字节的最高两位， 00: 不压缩, 01: gzip压缩, 10: xx压缩, 11: xx压缩
const COMPRESSION_BIT: usize = 3 << 30;

pub trait FrameCoder
where
    Self: Message + Sized + Default,
{
    /// 把一个 Message encode 成一个 frame
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        let size = self.encoded_len();
        if size >= MAX_FRAME {
            return Err(KvError::FrameError);
        }

        buf.put_u32(size as _);

        if size > COMPRESSION_LIMIT {
            let mut proto = Vec::with_capacity(size);
            self.encode(&mut proto)?;
            let payload = buf.split_off(LEN_LEN);
            buf.clear();

            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            encoder.write_all(&proto)?;

            let payload = encoder.finish()?.into_inner();
            debug!("Encode a frame: size {}({})", size, payload.len());

            buf.put_u32((payload.len() | COMPRESSION_BIT) as _);

            buf.unsplit(payload);

            Ok(())
        } else {
            self.encode(buf)?;
            Ok(())
        }
    }

    /// 把一个完整的 frame decode 成一个 Message
    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        let header = buf.get_u32() as usize;

        todo!()
    }
}

fn decode_header(header: usize) -> (usize, bool) {
    let len = header & !COMPRESSION_BIT;
    let compressd = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_header_work() {
        let encode_header = |len: usize| {
            if len >= COMPRESSION_LIMIT {
                len | COMPRESSION_BIT as usize
            } else {
                len
            }
        };

        let header = encode_header(1);
        println!("{}", header);
        assert_eq!(decode_header(header), (1, false));

        let header = encode_header(0);
        println!("{}", header);
        assert_eq!(decode_header(header), (0, false));

        let header = encode_header(1000);
        println!("{}", header);
        assert_eq!(decode_header(header), (1000, false));

        let header = encode_header(1436);
        println!("{}", header);
        assert_eq!(decode_header(header), (1436, true));

        let header = encode_header(10000);
        println!("{}", header);
        assert_eq!(decode_header(header), (10000, true));
    }
}
