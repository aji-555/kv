use std::io::Read;
use std::os::unix::fs::FileExt;
use std::{io::Cursor, sync::Arc};

use std::io::BufReader;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, ServerConfig};
use tokio_rustls::TlsConnector;
use tokio_rustls::{
    client::TlsStream as ClientTlsStream, server::TlsStream as ServerTlsStream, TlsAcceptor,
};

use crate::KvError;

/// KV Server 自己的 ALPN (Application-Layer Protocol Negotiation)
const ALPN_KV: &str = "kv";

#[derive(Clone)]
pub struct TlsServerAcceptor {
    inner: Arc<ServerConfig>,
}

#[derive(Clone)]
pub struct TlsClientConnector {
    pub config: Arc<ClientConfig>,
    pub domain: Arc<String>,
}

impl TlsClientConnector {
    /// load cert & key
    pub fn new(
        domain: impl Into<String>,
        identity: Option<(&str, &str)>,
        server_ca: Option<&str>,
    ) -> Result<Self, KvError> {
        let mut config = ClientConfig::builder();

        if let Some((cert, key)) = identity {
            let certs = load_certs(cert)?;
            let key = load_key(key)?;
            config
        }
        // 加载本地信任的根证书链
        config.root_store = match rustls_native_certs::load_native_certs() {
            Ok(store) | Err((Some(store), _)) => store,
            Err((None, error)) => return Err(error.into()),
        };
        // 如果有签署服务器的 CA 证书，则加载它，这样服务器证书不在根证书链 // 但是这个 CA 证书能验证它，也可以
        if let Some(cert) = server_ca {
            let mut buf = Cursor::new(cert);
            config.root_store.add_pem_file(&mut buf).unwrap();
        }
        Ok(Self {
            config: Arc::new(config),
            domain: Arc::new(domain.into()),
        })
    }

    pub async fn connect<S>(&self, stream: S) -> Result<ClientTlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        // let dns = DNSNameRef::
        let dns = ServerName::try_from(self.domain.as_str())
            .map_err(|_| KvError::Internal("Invalid DNS name".into()))?
            .to_owned();
        let stream = TlsConnector::from(self.config.clone())
            .connect(dns, stream)
            .await?;
        Ok(stream)
    }
}

impl TlsServerAcceptor {
    /// 加载 server cert / CA cert，生成 ServerConfig
    pub fn new(cert: &str, key: &str, client_ca: Option<&str>) -> Result<Self, KvError> {
        todo!()
    }

    pub async fn accept<S>(&self, stream: S) -> Result<ServerTlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let acceptor = TlsAcceptor::from(self.inner.clone());
        Ok(acceptor.accept(stream).await?)
    }
}

fn load_certs(cert: &str) -> Result<Vec<CertificateDer>, KvError> {
    let mut reader = BufReader::new(std::fs::File::open(cert)?);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| KvError::CertifcateParseError("server".to_owned(), "cert".to_owned()))
}

fn load_key(key: &str) -> Result<PrivateKeyDer, KvError> {
    let mut reader = BufReader::new(std::fs::File::open(key)?);
    if let Ok(keys) = rustls_pemfile::private_key(&mut reader) {
        if keys.is_some() {
            return Ok(keys.unwrap().into());
        }
    }

    Err(KvError::CertifcateParseError(
        "private".to_owned(),
        "key".to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use bytes::Bytes;

    #[test]
    fn load_key_with() {
        let key = include_str!("/home/aji/kv/fixtures/server.key");
        println!("{}", key);
        let mut key = BufReader::new(Cursor::new(key));
        let keys = rustls_pemfile::pkcs8_private_keys(&mut key)
            .next()
            .unwrap()
            .unwrap();
        println!("{:?}", keys.secret_pkcs8_der());
    }
}
