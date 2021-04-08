//! This module implements the high-level client used to communicate with
//! a memcached cluster. Regardless of the async runtime used, all
//! implementations use the same client interface with the same API.

use crate::{
    protocol::{Header, Packet, ProtocolError, Status},
    ring::Ring,
};
use async_trait::async_trait;
use deadpool::managed::{Manager, RecycleResult};
use std::collections::HashMap;

/// An error causing during client communication with Memcached.
#[derive(Debug)]
pub enum Error {
    /// An error communicating over the wire.
    IoError(std::io::Error),
    /// An error caused by incorrectly implementing the memcached protocol.
    Protocol(ProtocolError),
    /// An error caused by a non-zero status received from a packet.
    Status(Status),
}

/// The result of of a multi_get() request. A map of all of keys for which
/// memcached returned a found response, and their corresponding values.
pub type BulkOkResponse = HashMap<Vec<u8>, Vec<u8>>;

/// The result of a multi_*() request. A map of all keys for which there
/// was an error for specific keys. These can be treated as get misses
/// and ignored, but it may be desirable to log these errors to uncover
/// underlying issues.
pub type BulkErrResponse = HashMap<Vec<u8>, Error>;

/// The result of doing a multi_set(), multi_delete(), etc...
pub type BulkUpdateResponse = Result<BulkErrResponse, Error>;

/// The result of doing a multi_get(). The Ok result will be a tuple of ok, err
/// responses. The err responses can be treated as get misses, but should be
/// logged somewhere for visibility. Lots of them could indicate a serious
/// underlying issue.
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

/// A Compressor is used to implement compression of packet values. A default
/// implementation is provided for [`NoCompressor`], as well as
/// [`ZlibCompressor`].
///
/// If other compression algorithms are desired it is possible to implement
/// this trait yourself and pass it into [`Client::new`].
pub trait Compressor: Clone + Copy + Send + Sync {
    /// Consume a packet, returning a (possibly) modified packet with the
    /// packet value compressed. This should set the appropriate packet
    /// flags on the extras field.
    fn compress(&self, packet: Packet) -> Result<Packet, Error>;
    /// Consume a packet, returning a (possibly) modified packet with the
    /// packet value decompressed. This should unset the appropriate packet
    /// flags on the extras field.
    fn decompress(&self, packet: Packet) -> Result<Packet, Error>;
}

/// An implementation of [`Compressor`] that does nothing. This is useful if
/// you want to disable compression.
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
    /// Connect to a memcached server over TCP.
    async fn connect(url: String) -> Result<Self, Error>;

    /// Read to fill the incoming buffer.
    async fn read(&mut self, buf: &mut Vec<u8>) -> Result<usize, Error>;

    /// Write an entire buffer to the TCP stream.
    async fn write(&mut self, data: &[u8]) -> Result<(), Error>;

    /// Read a packet response, possibly decompressing it. It is most likely
    /// unnecessary to implement this yourself.
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

    /// Write a packet request, possibly compressing it. It is most likely
    /// unnecessary to implement this yourself.
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

/// Set configuration values for a memcached client.
#[derive(Debug, Clone)]
pub struct ClientConfig<P: Compressor> {
    endpoints: Vec<String>,
    compressor: P,
}

impl<P: Compressor> ClientConfig<P> {
    /// Create a new client config from the given memcached servers and
    /// compressor. If no compression is desired, then use
    /// [`ClientConfig::new_uncompressed`]
    pub fn new(endpoints: Vec<String>, compressor: P) -> Self {
        Self {
            endpoints,
            compressor,
        }
    }
}

impl ClientConfig<NoCompressor> {
    /// Create a new client config with no compression.
    pub fn new_uncompressed(endpoints: Vec<String>) -> Self {
        Self::new(endpoints, NoCompressor)
    }
}

/// A client manages connections to every node in a memcached cluster using
/// consistent hashing to decide which connection to use based on the key.
#[derive(Debug)]
pub struct Client<C: Connection, P: Compressor> {
    ring: Ring<C>,
    compressor: P,
}

impl<C: Connection, P: Compressor> Client<C, P> {
    /// Create a new client using the client config provided.
    pub async fn new(config: ClientConfig<P>) -> Result<Self, Error> {
        let ClientConfig {
            endpoints,
            compressor,
        } = config;
        let ring = Ring::new(endpoints).await?;
        Ok(Self { ring, compressor })
    }

    /// Get a single value from memcached. Returns None when the key is not
    /// found (i.e., a miss).
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

    /// Get multiple values from memcached at once. On success, it returns
    /// a tuple of (ok, err) responses. The error responses can be treated as
    /// misses, but should be logged for visibility. Lots of errors could be
    /// indicative of a serious problem.
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

    /// Set a single key/value pair in memcached to expire at the desired
    /// time. A value of 0 means "never expire", but the value could still be
    /// evicted by the LRU cache. Important: if `expire` is set to more than 30
    /// days in the future, then memcached will treat it as a unix timestamp
    /// instead of a duration.
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

    /// Set multiple key/value pairs in memcached to expire at the desired
    /// time. A value of 0 means "never expire", but the value could still be
    /// evicted by the LRU cache. Important: if `expire` is set to more than 30
    /// days in the future, then memcached will treat it as a unix timestamp
    /// instead of a duration.
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

    /// Delete a key from memcached. Does nothing if the key is not set.
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        let conn = self.ring.get_conn(key)?;
        conn.write_packet(self.compressor, Packet::delete(key.into()))
            .await?;
        conn.read_packet(self.compressor).await?;
        Ok(())
    }

    /// Delete multiple keys from memcached. Does nothing when a key is unset.
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

/// A connection pool for multiple connections. Using a pool is recommended
/// for best performance since it eliminates the overhead of having to
/// constantly recreate TCP connections, while also balancing the total
/// number of connections open at a time.
pub type Pool<C, P> = deadpool::managed::Pool<Client<C, P>, Error>;