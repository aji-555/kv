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
        let (len, compressed) = decode_header(header);
        debug!("Got a frame: msg len {}, compressed {}", len, compressed);

        if compressed {
            let mut decoder = GzDecoder::new(&buf[..len]);

            let mut proto = Vec::with_capacity(len * 2);
            decoder.read_to_end(&mut proto)?;
            buf.advance(len);

            Ok(Self::decode(proto.as_ref())?)
        } else {
            let msg = Self::decode(buf.as_ref())?;
            buf.advance(len);
            Ok(msg)
        }
    }
}

impl FrameCoder for CommandRequest {}
impl FrameCoder for CommandResponse {}

/// 从 stream 中读取一个完整的 frame
pub async fn read_frame<S>(stream: &mut S, buf: &mut BytesMut) -> Result<(), KvError>
where
    S: AsyncRead + Unpin + Send,
{
    let header = stream.read_u32().await? as usize;
    let (len, _compressed) = decode_header(header);
    // 如果没有这么大的内存，就分配至少一个 frame 的内存，保证它可用
    buf.reserve(LEN_LEN + len);
    buf.put_u32(header as _);
    // advance_mut 是 unsafe 的原因是，从当前位置 pos 到 pos + len，
    // 这段内存目前没有初始化。我们就是为了 reserve 这段内存，然后从 stream
    // 里读取，读取完，它就是初始化的。所以，我们这么用是安全的
    unsafe { buf.advance_mut(len) };
    stream.read_exact(&mut buf[LEN_LEN..]).await?;
    Ok(())
}

fn decode_header(header: usize) -> (usize, bool) {
    let len = header & !COMPRESSION_BIT;
    let compressd = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressd)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use bytes::Bytes;

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
        // println!("{}", header);
        assert_eq!(decode_header(header), (1, false));

        let header = encode_header(0);
        // println!("{}", header);
        assert_eq!(decode_header(header), (0, false));

        let header = encode_header(1000);
        // println!("{}", header);
        assert_eq!(decode_header(header), (1000, false));

        let header = encode_header(1436);
        // println!("{}", header);
        assert_eq!(decode_header(header), (1436, true));

        let header = encode_header(10000);
        // println!("{}", header);
        assert_eq!(decode_header(header), (10000, true));
    }

    #[test]
    fn command_request_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let cmd = CommandRequest::new_hdel("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();

        // 最高位没设置
        assert_eq!(is_compressed(&buf), false);

        let cmd1 = CommandRequest::decode_frame(&mut buf).unwrap();
        assert_eq!(cmd, cmd1);
    }

    #[test]
    fn command_response_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let values: Vec<Value> = vec![1.into(), "hello".into(), b"data".into()];
        let res: CommandResponse = values.into();
        res.encode_frame(&mut buf).unwrap();

        // 最高位没设置
        assert_eq!(is_compressed(&buf), false);

        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(res, res1);
    }

    #[test]
    fn command_response_compressed_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let value: Value = Bytes::from(vec![0u8; COMPRESSION_LIMIT + 1]).into();
        let res: CommandResponse = value.into();
        res.encode_frame(&mut buf).unwrap();

        // 最高位设置了
        assert_eq!(is_compressed(&buf), true);

        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(res, res1);
    }

    fn is_compressed(data: &[u8]) -> bool {
        if let &[v] = &data[..1] {
            v >> 7 == 1
        } else {
            false
        }
    }

    struct DummyStream {
        buf: BytesMut,
    }

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            // 看看 ReadBuf 需要多大的数据
            let len = buf.capacity();

            // split 出这么大的数据
            let data = self.get_mut().buf.split_to(len);

            // 拷贝给 ReadBuf
            buf.put_slice(&data);

            // 直接完工
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn read_frame_should_work() {
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hdel("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();
        let mut stream = DummyStream { buf };

        let mut data = BytesMut::new();
        read_frame(&mut stream, &mut data).await.unwrap();

        let cmd1 = CommandRequest::decode_frame(&mut data).unwrap();
        assert_eq!(cmd, cmd1);
    }
}
