use std::convert::{TryFrom, TryInto};

use super::{
    ProtocolError, Status, ADDQ_OPCODE, ADD_OPCODE, GETKQ_OPCODE, GETK_OPCODE, GET_OPCODE,
    MAGIC_REQUEST_VALUE, MAGIC_RESPONSE_VALUE, NOOP_OPCODE, REPLACEQ_OPCODE, REPLACE_OPCODE,
    SETQ_OPCODE, SET_OPCODE,
};

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub struct Header {
    pub magic: u8,
    pub opcode: u8,
    pub key_length: u16,
    pub extras_length: u8,
    pub data_type: u8,
    pub vbucket_or_status: u16,
    pub body_len: u32,
    pub opaque: u32,
    pub cas: u64,
}

impl Header {
    pub fn read_packet(self, body: &[u8]) -> Result<Packet, ProtocolError> {
        if body.len() != self.body_len as usize {
            // The body length does not match the header
            return Err(ProtocolError::BodySizeMismatch);
        }

        let (extras, body) = body.split_at(self.extras_length as usize);
        let (key, value) = body.split_at(self.key_length as usize);

        Ok(Packet {
            header: self,
            extras: extras.into(),
            key: key.into(),
            value: value.into(),
        })
    }

    pub fn read_response(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() < 24 {
            // The header must be 24 bytes
            return Err(ProtocolError::PacketTooSmall);
        }
        let magic = u8::from_be_bytes(bytes[0..1].try_into().unwrap());
        if magic != MAGIC_RESPONSE_VALUE {
            return Err(ProtocolError::InvalidMagic(magic));
        }
        Ok(Header {
            magic,
            opcode: u8::from_be_bytes(bytes[1..2].try_into().unwrap()),
            key_length: u16::from_be_bytes(bytes[2..4].try_into().unwrap()),
            extras_length: u8::from_be_bytes(bytes[4..5].try_into().unwrap()),
            data_type: u8::from_be_bytes(bytes[5..6].try_into().unwrap()),
            vbucket_or_status: u16::from_be_bytes(bytes[6..8].try_into().unwrap()),
            body_len: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
            opaque: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
            cas: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        })
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct Packet {
    pub header: Header,
    pub extras: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Packet {
    fn new_request(opcode: u8, key: Vec<u8>, extras: Vec<u8>, value: Vec<u8>) -> Self {
        let mut packet = Packet::default();
        packet.header.magic = MAGIC_REQUEST_VALUE;
        packet.header.opcode = opcode;
        packet.header.key_length = key.len() as u16;
        packet.header.extras_length = extras.len() as u8;
        packet.header.body_len = (extras.len() + key.len() + value.len()) as u32;
        packet.key = key;
        packet.extras = extras;
        packet.value = value;
        packet
    }

    pub fn get(key: Vec<u8>) -> Self {
        Packet::new_request(GET_OPCODE, key, vec![], vec![])
    }

    pub fn getk(key: Vec<u8>) -> Self {
        Packet::new_request(GETK_OPCODE, key, vec![], vec![])
    }

    pub fn getkq(key: Vec<u8>) -> Self {
        Packet::new_request(GETKQ_OPCODE, key, vec![], vec![])
    }

    pub fn set(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        let extras = [[0, 0, 0, 0], expire.to_be_bytes()].concat();
        Packet::new_request(SET_OPCODE, key, extras, value)
    }

    pub fn setq(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        let extras = [[0, 0, 0, 0], expire.to_be_bytes()].concat();
        Packet::new_request(SETQ_OPCODE, key, extras, value)
    }

    pub fn add(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        let extras = [[0, 0, 0, 0], expire.to_be_bytes()].concat();
        Packet::new_request(ADD_OPCODE, key, extras, value)
    }

    pub fn addq(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        let extras = [[0, 0, 0, 0], expire.to_be_bytes()].concat();
        Packet::new_request(ADDQ_OPCODE, key, extras, value)
    }

    pub fn replace(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        let extras = [[0, 0, 0, 0], expire.to_be_bytes()].concat();
        Packet::new_request(REPLACE_OPCODE, key, extras, value)
    }

    pub fn replaceq(key: Vec<u8>, value: Vec<u8>, expire: u32) -> Self {
        let extras = [[0, 0, 0, 0], expire.to_be_bytes()].concat();
        Packet::new_request(REPLACEQ_OPCODE, key, extras, value)
    }

    pub fn noop() -> Self {
        Packet::new_request(NOOP_OPCODE, vec![], vec![], vec![])
    }

    pub fn error_for_status(&self) -> Result<(), Status> {
        match self.header.vbucket_or_status {
            0 => Ok(()),
            it => Err(Status::from(it)),
        }
    }
}

impl Into<Vec<u8>> for Packet {
    fn into(self) -> Vec<u8> {
        vec![
            &self.header.magic.to_be_bytes()[..],
            &self.header.opcode.to_be_bytes()[..],
            &self.header.key_length.to_be_bytes()[..],
            &self.header.extras_length.to_be_bytes()[..],
            &self.header.data_type.to_be_bytes()[..],
            &self.header.vbucket_or_status.to_be_bytes()[..],
            &self.header.body_len.to_be_bytes()[..],
            &self.header.opaque.to_be_bytes()[..],
            &self.header.cas.to_be_bytes()[..],
            &self.extras[..],
            &self.key[..],
            &self.value[..],
        ]
        .concat()
    }
}

#[cfg(test)]
mod tests {
    use super::Packet;
    use crate::protocol::Header;

    #[test]
    fn test_packet_identity() {
        let header = Header {
            magic: 0x80,
            opcode: 0x0,
            key_length: 0x5,
            extras_length: 0x0,
            data_type: 0x0,
            vbucket_or_status: 0x0,
            body_len: 0x5,
            opaque: 0x0,
            cas: 0x0,
        };
        let expect_packet = Packet {
            header,
            extras: vec![],
            key: "Hello".into(),
            value: vec![],
        };
        let expect_bytes = vec![
            0x80, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x48, 0x65, 0x6c, 0x6c,
            0x6f,
        ];

        let packet_bytes: Vec<u8> = expect_packet.clone().into();
        assert_eq!(expect_bytes, packet_bytes);

        let actual_packet: Packet = header.read_packet(b"Hello").unwrap();
        assert_eq!(expect_packet, actual_packet);
    }
}
