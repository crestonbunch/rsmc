use crate::{
    protocol::{Header, Packet, ProtocolError, Status},
    ring::Ring,
};
use async_trait::async_trait;
use deadpool::managed::{Manager, RecycleResult};
use std::collections::HashMap;

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    Protocol(ProtocolError),
    Status(Status),
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

impl From<Status> for Error {
    fn from(err: Status) -> Self {
        Self::Status(err)
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

    async fn read_packet(&mut self) -> Result<Packet, Error> {
        let mut buf = vec![0_u8; 24];
        self.read(&mut buf).await?;
        let header = Header::read_response(&buf[..])?;
        let mut body = vec![0_u8; header.body_len as usize];
        if body.len() > 0 {
            self.read(&mut body).await?;
        }
        Ok(header.read_packet(&body[..])?)
    }

    async fn write_packet(&mut self, packet: Packet) -> Result<(), Error> {
        let bytes: Vec<u8> = packet.into();
        self.write(&bytes[..]).await
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
        conn.write_packet(Packet::get(key.into())).await?;

        let packet = conn.read_packet().await?;
        match packet.error_for_status() {
            Ok(()) => Ok(Some(packet.value)),
            Err(Status::KeyNotFound) => Ok(None),
            Err(status) => Err(status)?,
        }
    }

    pub async fn get_multi<'a>(
        &mut self,
        keys: Vec<&[u8]>,
    ) -> Result<HashMap<Vec<u8>, Result<Vec<u8>, Error>>, Error> {
        let mut out = HashMap::new();

        // TODO: parallelize
        for (conn, mut pipeline) in self.ring.get_conns(keys.clone()) {
            let last_key = pipeline.pop().unwrap();
            let reqs = pipeline
                .iter()
                .map(|key| Packet::getkq((*key).into()))
                .chain(vec![Packet::getk(last_key.into())]);

            for packet in reqs {
                let key = packet.key.clone();
                if let Err(err) = conn.write_packet(packet).await {
                    out.insert(key, Err(err));
                }
            }
        }

        // TODO: parallelize
        for (conn, mut pipeline) in self.ring.get_conns(keys.clone()) {
            let last_key = pipeline.pop().unwrap();
            let mut errs: Vec<Result<(), Error>> = vec![];
            let mut finished = false;
            while !finished {
                let packet = conn.read_packet().await?;
                let key = packet.key.clone();
                finished = packet.key == last_key;
                match packet.error_for_status() {
                    Ok(()) => {
                        out.insert(key, Ok(packet.value));
                    }
                    Err(Status::KeyNotFound) => (),
                    Err(err) => errs.push(Err(Error::Status(err))),
                }
            }
        }
        Ok(out)
    }

    pub async fn set(&mut self, key: &[u8], data: &[u8], expire: u32) -> Result<(), Error> {
        let conn = self.ring.get_conn(key)?;
        conn.write_packet(Packet::set(key.into(), data.into(), expire))
            .await?;
        conn.read_packet().await?;
        Ok(())
    }

    pub async fn set_multi<'a>(
        &mut self,
        mut data: HashMap<Vec<u8>, Vec<u8>>,
        expire: u32,
    ) -> Result<HashMap<Vec<u8>, Result<(), Error>>, Error> {
        let mut out = HashMap::new();
        let keys = data.keys().map(|k| k.clone()).collect::<Vec<_>>();
        let keys = keys.iter().map(|k| &k[..]).collect::<Vec<_>>();

        // TODO: parallelize
        for (conn, mut pipeline) in self.ring.get_conns(keys.clone()) {
            let last_key = pipeline.pop().unwrap();
            let last_val = data.remove(last_key).unwrap();
            let reqs = pipeline
                .into_iter()
                .map(|key| (key, data.remove(key).unwrap()))
                .map(|(key, value)| Packet::setq(key.into(), value, expire).into())
                .chain(vec![Packet::set(last_key.into(), last_val, expire)]);

            for packet in reqs {
                let key = packet.key.clone();
                if let Err(err) = conn.write_packet(packet).await {
                    out.insert(key, Err(err));
                }
            }
        }

        // TODO: parallelize
        for (conn, _) in self.ring.get_conns(keys.clone()) {
            let mut errs: Vec<Result<(), Error>> = vec![];
            let mut finished = false;
            while !finished {
                let packet = conn.read_packet().await?;
                finished = packet.header.vbucket_or_status == 0;
                match packet.error_for_status() {
                    Ok(()) => (),
                    Err(Status::KeyNotFound) => (),
                    Err(err) => errs.push(Err(Error::Status(err))),
                }
            }
        }

        Ok(out)
    }

    pub async fn delete(&self, key: &[u8]) -> Result<(), Error> {
        unimplemented!()
    }

    pub async fn delete_multi(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Error> {
        unimplemented!()
    }

    async fn keep_alive(&mut self) -> Result<(), Error> {
        // TODO: verify read_packet returns a noop code
        for conn in self.ring.into_iter() {
            conn.write_packet(Packet::noop()).await?;
            conn.read_packet().await?.error_for_status()?;
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
