use crate::{
    protocol::{
        GetRequest, GetResponse, Header, NoopRequest, NoopResponse, Packet, ProtocolError, Request,
        Response, SetRequest, SetResponse, Status,
    },
    ring::Ring,
};
use async_trait::async_trait;
use deadpool::managed::{Manager, RecycleResult};
use futures_util::stream::{self, StreamExt};
use std::convert::{TryFrom, TryInto};
use stream::FuturesOrdered;

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    Protocol(ProtocolError),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl From<ProtocolError> for Error {
    fn from(err: ProtocolError) -> Self {
        Self::Protocol(err)
    }
}

/// A connection is an async interface to memcached, which requires a concrete
/// implementation using an underlying async runtime (e.g. tokio or async-std.)
#[async_trait]
pub trait Connection: Sized + Send + Sync {
    /// Connect to a of memcached node nodes.
    async fn connect(url: String) -> Result<Self, Error>;
    async fn read(&mut self, buf: &mut Vec<u8>) -> Result<usize, Error>;
    async fn write(&mut self, data: &[u8]) -> Result<(), Error>;

    async fn read_packet(&mut self) -> Result<Option<Packet>, Error> {
        let mut buf = vec![0_u8; 24];
        let size = self.read(&mut buf).await?;
        if size == 0 {
            return Ok(None);
        }
        let header = Header::try_from(&buf[..])?;
        let mut body = vec![0_u8; header.body_len as usize];
        if body.len() > 0 {
            self.read(&mut body).await?;
        }
        Ok(Some(Packet::new(header, &body)?))
    }

    async fn read_response<R: Response>(&mut self) -> Result<Option<R>, Error> {
        let packet = self.read_packet().await?;
        let response = packet.map(R::from_packet).transpose()?;
        Ok(response)
    }

    async fn write_packet(&mut self, packet: Packet) -> Result<(), Error> {
        let bytes: Vec<u8> = packet.into();
        self.write(&bytes[..]).await
    }

    async fn write_request<R: Request + Send + Sync>(&mut self, mut req: R) -> Result<(), Error> {
        let packet = Packet::from(&mut req);
        self.write_packet(packet).await
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    endpoints: Vec<String>,
}

impl ClientConfig {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self { endpoints }
    }
}

/// A client manages connections to every node in a memcached cluster.
#[derive(Debug)]
pub struct Client<C: Connection> {
    ring: Ring<C>,
}

impl<C: Connection> Client<C> {
    pub async fn new(config: ClientConfig) -> Result<Self, Error> {
        let ring = Ring::new(config.endpoints).await?;
        Ok(Self { ring })
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let conn = self.ring.get_conn(key)?;
        conn.write_request(GetRequest::new(key.into())).await?;
        let resp = match conn.read_response::<GetResponse>().await {
            Ok(resp) => resp,
            Err(Error::Protocol(ProtocolError::NonZeroStatus(Status::KeyNotFound))) => None,
            Err(err) => Err(err)?,
        };
        Ok(resp.and_then(|r| r.value))
    }

    pub async fn get_multi<'a>(
        &mut self,
        mut keys: Vec<&[u8]>,
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        unimplemented!()
    }

    pub async fn set(&mut self, key: &[u8], data: &[u8], expire: u32) -> Result<(), Error> {
        let conn = self.ring.get_conn(key)?;
        let req = SetRequest::new(key.into(), data.into(), expire);
        conn.write_request(req).await?;
        conn.read_response::<SetResponse>().await?;
        Ok(())
    }

    pub async fn set_multi(&self, data: &[(&[u8], &[u8])]) -> Result<(), Error> {
        unimplemented!()
    }

    pub async fn delete(&self, key: &[u8]) -> Result<(), Error> {
        unimplemented!()
    }

    pub async fn delete_multi(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Error> {
        unimplemented!()
    }

    async fn keep_alive(&mut self) -> Result<(), Error> {
        for conn in &mut self.ring.conns {
            conn.write_request(NoopRequest::new()).await?;
            conn.read_response::<NoopResponse>().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<C> Manager<Client<C>, Error> for ClientConfig
where
    C: Connection + Send + Sync + 'static,
{
    async fn create(&self) -> Result<Client<C>, Error> {
        let mut client = Client::new(self.clone()).await?;
        client.keep_alive().await?;
        Ok(client)
    }

    async fn recycle(&self, client: &mut Client<C>) -> RecycleResult<Error> {
        client.keep_alive().await?;
        Ok(())
    }
}

pub type Pool<C>
where
    C: Connection,
= deadpool::managed::Pool<Client<C>, Error>;
