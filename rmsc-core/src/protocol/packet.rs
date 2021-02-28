use std::convert::{TryFrom, TryInto};

use super::ProtocolError;

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

impl TryFrom<&[u8]> for Header {
    type Error = ProtocolError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 24 {
            // The header must be 24 bytes
            return Err(ProtocolError::PacketTooSmall);
        }

        Ok(Header {
            magic: u8::from_be_bytes(bytes[0..1].try_into().unwrap()),
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
    pub fn new(header: Header, body: &[u8]) -> Result<Self, ProtocolError> {
        if body.len() != header.body_len as usize {
            // The body length does not match the header
            return Err(ProtocolError::BodySizeMismatch);
        }

        let (extras, body) = body.split_at(header.extras_length as usize);
        let (key, value) = body.split_at(header.key_length as usize);

        Ok(Packet {
            header,
            extras: extras.into(),
            key: key.into(),
            value: value.into(),
        })
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

        let actual_packet: Packet = Packet::new(header, b"Hello").unwrap();
        assert_eq!(expect_packet, actual_packet);
    }
}
