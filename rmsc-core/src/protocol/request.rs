use std::mem;

use super::{
    Packet, ADDQ_OPCODE, ADD_OPCODE, GETKQ_OPCODE, GETK_OPCODE, GETQ_OPCODE, GET_OPCODE,
    MAGIC_REQUEST_VALUE, NOOP_OPCODE, REPLACEQ_OPCODE, REPLACE_OPCODE, SETQ_OPCODE, SET_OPCODE,
    VERSION_OPCODE,
};

/// All requests to memcached must specify a subset of fields as part of
/// the packet we send over the wire. This trait reduces boilerplate for
/// implementing a request by setting defaults for most fields and automatically
/// implementing the From<_> trait to convert the request into a Packet.
pub trait Request {
    /// Return the opcode for the request.
    fn opcode(&self) -> u8;

    /// Return the extras for the request (default: empty)
    fn extras(&mut self) -> Vec<u8> {
        vec![]
    }

    /// Return the key for the request (default: empty)
    fn key(&mut self) -> Vec<u8> {
        vec![]
    }

    /// Return the value for the request (default: empty)
    fn value(&mut self) -> Vec<u8> {
        vec![]
    }

    // TODO: cas, etc...
}

impl<R: Request> From<&mut R> for Packet {
    fn from(req: &mut R) -> Self {
        let mut packet = Packet::default();
        packet.key = req.key();
        packet.extras = req.extras();
        packet.value = req.value();
        packet.header.magic = MAGIC_REQUEST_VALUE;
        packet.header.opcode = req.opcode();
        packet.header.key_length = packet.key.len() as u16;
        packet.header.extras_length = packet.extras.len() as u8;
        packet.header.body_len =
            (packet.extras.len() + packet.key.len() + packet.value.len()) as u32;
        packet
    }
}

/// A GetRequest is used to lookup a value in memcached, given a key.
#[derive(Debug)]
pub struct GetRequest {
    pub opcode: u8,
    pub key: Vec<u8>,
}

impl GetRequest {
    /// Create a new request to get a value. Returns something even on a
    /// cache miss.
    pub fn new(key: Vec<u8>) -> Self {
        GetRequest {
            opcode: GET_OPCODE,
            key,
        }
    }

    /// Create a new request to get a value. Returns something even on a
    /// cache miss, and includes the key in the response.
    pub fn new_k(key: Vec<u8>) -> Self {
        GetRequest {
            opcode: GETK_OPCODE,
            key,
        }
    }

    /// Create a new request to get a value. Does not return anything on
    /// a cache miss.
    pub fn new_q(key: Vec<u8>) -> Self {
        GetRequest {
            opcode: GETQ_OPCODE,
            key,
        }
    }

    /// Create a new request to get a value. Does not return anything on
    /// a cache miss, and includes the key in the response.
    pub fn new_kq(key: Vec<u8>) -> Self {
        GetRequest {
            opcode: GETKQ_OPCODE,
            key,
        }
    }
}

impl Request for GetRequest {
    fn opcode(&self) -> u8 {
        self.opcode
    }

    fn key(&mut self) -> Vec<u8> {
        mem::take(&mut self.key)
    }
}

/// A request to set a key/value pair in memcached with a desired expiration.
#[derive(Debug)]
pub struct SetRequest {
    pub opcode: u8,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expire: u32,
}

impl SetRequest {
    /// Create a new request to set a value, overwriting any existing value
    /// at the specified key.
    pub fn new(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        SetRequest {
            opcode: SET_OPCODE,
            key,
            value,
            expire,
        }
    }

    /// Create a new request to set a value, overwriting any existing value
    /// at the specified key. Does not return a response except on an error.
    pub fn new_q(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        SetRequest {
            opcode: SETQ_OPCODE,
            key,
            value,
            expire,
        }
    }

    /// Create a new request to add a value, failing if an item already
    /// exists at the specified key.
    pub fn new_add(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        SetRequest {
            opcode: ADD_OPCODE,
            key,
            value,
            expire,
        }
    }

    /// Create a new request to add a value, failing if an item already
    /// exists at the specified key. Does not return a response except on an
    /// error.
    pub fn new_add_q(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        SetRequest {
            opcode: ADDQ_OPCODE,
            key,
            value,
            expire,
        }
    }

    /// Create a new request to replace a value, failing if an item does not
    /// exist at the specified key.
    pub fn new_replace(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        SetRequest {
            opcode: REPLACE_OPCODE,
            key,
            value,
            expire,
        }
    }

    /// Create a new request to replace a value, failing if an item does not
    /// exist at the specified key. Does not return a response except on an
    /// error.
    pub fn new_replace_q(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        SetRequest {
            opcode: REPLACEQ_OPCODE,
            key,
            value,
            expire,
        }
    }
}

impl Request for SetRequest {
    fn opcode(&self) -> u8 {
        self.opcode
    }

    fn key(&mut self) -> Vec<u8> {
        mem::take(&mut self.key)
    }

    fn extras(&mut self) -> Vec<u8> {
        [[0, 0, 0, 0], self.expire.to_be_bytes()].concat()
    }

    fn value(&mut self) -> Vec<u8> {
        mem::take(&mut self.value)
    }
}

/// A NoopRequest does nothing, it might be used e.g. for a keep alive.
#[derive(Debug)]
pub struct NoopRequest {
    pub opcode: u8,
}

impl NoopRequest {
    /// Create a new noop request that does nothing.
    pub fn new() -> Self {
        NoopRequest {
            opcode: NOOP_OPCODE,
        }
    }

    /// Create a new request to get the server memcached version.
    pub fn new_version() -> Self {
        NoopRequest {
            opcode: VERSION_OPCODE,
        }
    }
}

impl Request for NoopRequest {
    fn opcode(&self) -> u8 {
        self.opcode
    }
}
