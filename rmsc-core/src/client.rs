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

pub type BulkOkResponse = HashMap<Vec<u8>, Vec<u8>>;
pub type BulkErrResponse = HashMap<Vec<u8>, Error>;

pub type BulkUpdateResponse = Result<BulkErrResponse, Error>;
pub type BulkGetResponse = Result<(BulkOkResponse, BulkErrResponse), Error>;

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

pub trait Compressor: Clone + Copy + Send + Sync {
    fn compress(&self, packet: Packet) -> Result<Packet, Error>;
    fn decompress(&self, packet: Packet) -> Result<Packet, Error>;
}

#[derive(Debug, Clone, Copy)]
pub struct NoCompressor;

impl Compressor for NoCompressor {
    fn compress(&self, bytes: Packet) -> Result<Packet, Error> {
        Ok(bytes)
    }

    fn decompress(&self, bytes: Packet) -> Result<Packet, Error> {
        Ok(bytes)
    }
}

/// A connection is an async interface to memcached, which requires a concrete
/// implementation using an underlying async runtime (e.g. tokio or async-std.)
#[async_trait]
pub trait Connection: Sized + Send + Sync + 'static {
    /// Connect to a of memcached node nodes.
    async fn connect(url: String) -> Result<Self, Error>;
    async fn read(&mut self, buf: &mut Vec<u8>) -> Result<usize, Error>;
    async fn write(&mut self, data: &[u8]) -> Result<(), Error>;

    async fn read_packet<P: Compressor>(&mut self, compressor: P) -> Result<Packet, Error> {
        let mut buf = vec![0_u8; 24];
        self.read(&mut buf).await?;
        let header = Header::read_response(&buf[..])?;
        let mut body = vec![0_u8; header.body_len as usize];
        if !body.is_empty() {
            self.read(&mut body).await?;
        }
        let packet = header.read_packet(&body[..])?;
        compressor.decompress(packet)
    }

    async fn write_packet<P: Compressor>(
        &mut self,
        compressor: P,
        packet: Packet,
    ) -> Result<(), Error> {
        let packet = compressor.compress(packet)?;
        let bytes: Vec<u8> = packet.into();
        self.write(&bytes[..]).await
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig<P: Compressor> {
    endpoints: Vec<String>,
    compressor: P,
}

impl<P: Compressor> ClientConfig<P> {
    pub fn new(endpoints: Vec<String>, compressor: P) -> Self {
        Self {
            endpoints,
            compressor,
        }
    }
}

impl ClientConfig<NoCompressor> {
    pub fn new_uncompressed(endpoints: Vec<String>) -> Self {
        Self::new(endpoints, NoCompressor)
    }
}

/// A client manages connections to every node in a memcached cluster.
#[derive(Debug)]
pub struct Client<C: Connection, P: Compressor> {
    ring: Ring<C>,
    compressor: P,
}

impl<C: Connection, P: Compressor> Client<C, P> {
    pub async fn new(config: ClientConfig<P>) -> Result<Self, Error> {
        let ClientConfig {
            endpoints,
            compressor,
        } = config;
        let ring = Ring::new(endpoints).await?;
        Ok(Self { ring, compressor })
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let conn = self.ring.get_conn(key)?;
        conn.write_packet(self.compressor, Packet::get(key.into()))
            .await?;

        let packet = conn.read_packet(self.compressor).await?;
        match packet.error_for_status() {
            Ok(()) => Ok(Some(packet.value)),
            Err(Status::KeyNotFound) => Ok(None),
            Err(status) => Err(status.into()),
        }
    }

    pub async fn get_multi<'a>(&mut self, keys: Vec<&[u8]>) -> BulkGetResponse {
        let mut values = HashMap::new();
        let mut errors = HashMap::new();

        // TODO: parallelize
        for (conn, mut pipeline) in self.ring.get_conns(keys.clone()) {
            let last_key = pipeline.pop().unwrap();
            let reqs = pipeline
                .iter()
                .map(|key| Packet::getkq((*key).into()))
                .chain(vec![Packet::getk(last_key.into())]);

            for packet in reqs {
                let key = packet.key.clone();
                let result = conn.write_packet(self.compressor, packet).await;
                if let Err(err) = result {
                    errors.insert(key, err);
                }
            }
        }

        // TODO: parallelize
        for (conn, mut pipeline) in self.ring.get_conns(keys.clone()) {
            let last_key = pipeline.pop().unwrap();
            let mut finished = false;
            while !finished {
                let packet = conn.read_packet(self.compressor).await?;
                let key = packet.key.clone();
                finished = packet.key == last_key;
                match packet.error_for_status() {
                    Err(Status::KeyNotFound) => (),
                    Err(err) => {
                        errors.insert(key, Error::Status(err));
                    }
                    Ok(()) => {
                        values.insert(key, packet.value);
                    }
                }
            }
        }

        Ok((values, errors))
    }

    pub async fn set(&mut self, key: &[u8], data: &[u8], expire: u32) -> Result<(), Error> {
        let conn = self.ring.get_conn(key)?;
        conn.write_packet(
            self.compressor,
            Packet::set(key.into(), data.into(), expire),
        )
        .await?;
        conn.read_packet(self.compressor).await?;
        Ok(())
    }

    pub async fn set_multi<'a>(
        &mut self,
        mut data: HashMap<Vec<u8>, Vec<u8>>,
        expire: u32,
    ) -> BulkUpdateResponse {
        let mut errors = HashMap::new();

        let keys = data.keys().cloned().collect::<Vec<_>>();
        let keys = keys.iter().map(|k| &k[..]).collect::<Vec<_>>();

        // TODO: parallelize
        for (conn, mut pipeline) in self.ring.get_conns(keys.clone()) {
            let last_key = pipeline.pop().unwrap();
            let last_val = data.remove(last_key).unwrap();
            let reqs = pipeline
                .into_iter()
                .map(|key| (key, data.remove(key).unwrap()))
                .map(|(key, value)| Packet::setq(key.into(), value, expire))
                .chain(vec![Packet::set(last_key.into(), last_val, expire)]);

            for packet in reqs {
                let key = packet.key.clone();
                if let Err(err) = conn.write_packet(self.compressor, packet).await {
                    errors.insert(key, err);
                }
            }
        }

        // TODO: parallelize
        for (conn, _) in self.ring.get_conns(keys.clone()) {
            let mut finished = false;
            while !finished {
                let packet = conn.read_packet(self.compressor).await?;
                let key = packet.key.clone();
                finished = packet.header.vbucket_or_status == 0;
                match packet.error_for_status() {
                    Ok(()) => (),
                    Err(Status::KeyNotFound) => (),
                    Err(err) => {
                        errors.insert(key, Error::Status(err));
                    }
                }
            }
        }

        Ok(errors)
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        let conn = self.ring.get_conn(key)?;
        conn.write_packet(self.compressor, Packet::delete(key.into()))
            .await?;
        conn.read_packet(self.compressor).await?;
        Ok(())
    }

    pub async fn delete_multi(&mut self, keys: Vec<&[u8]>) -> BulkUpdateResponse {
        let mut errors = HashMap::new();

        // TODO: parallelize
        for (conn, pipeline) in self.ring.get_conns(keys.clone()) {
            let reqs = pipeline.into_iter().map(|key| Packet::delete(key.into()));
            for packet in reqs {
                let key = packet.key.clone();
                if let Err(err) = conn.write_packet(self.compressor, packet).await {
                    errors.insert(key, err);
                }
            }
        }

        // TODO: parallelize
        for (conn, pipeline) in self.ring.get_conns(keys.clone()) {
            for _ in pipeline {
                let packet = conn.read_packet(self.compressor).await?;
                let key = packet.key.clone();
                match packet.error_for_status() {
                    Ok(()) => (),
                    Err(err) => {
                        errors.insert(key, Error::Status(err));
                    }
                }
            }
        }

        Ok(errors)
    }

    async fn keep_alive(&mut self) -> Result<(), Error> {
        // TODO: verify read_packet returns a noop code
        for conn in self.ring.into_iter() {
            conn.write_packet(self.compressor, Packet::noop()).await?;
            let packet = conn.read_packet(self.compressor).await?;
            packet.error_for_status()?;
        }
        Ok(())
    }
}

#[async_trait]
impl<C, P> Manager<Client<C, P>, Error> for ClientConfig<P>
where
    C: Connection,
    P: Compressor,
{
    async fn create(&self) -> Result<Client<C, P>, Error> {
        let mut client = Client::new(self.clone()).await?;
        client.keep_alive().await?;
        Ok(client)
    }

    async fn recycle(&self, client: &mut Client<C, P>) -> RecycleResult<Error> {
        client.keep_alive().await?;
        Ok(())
    }
}

pub type Pool<C, P> = deadpool::managed::Pool<Client<C, P>, Error>;
