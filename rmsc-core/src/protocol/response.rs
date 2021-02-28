use std::convert::TryFrom;

use super::{
    Packet, ProtocolError, ADDQ_OPCODE, ADD_OPCODE, GETKQ_OPCODE, GETK_OPCODE, GETQ_OPCODE,
    GET_OPCODE, MAGIC_RESPONSE_VALUE, NOOP_OPCODE, REPLACEQ_OPCODE, REPLACE_OPCODE, SETQ_OPCODE,
    SET_OPCODE,
};

pub trait Response: Default {
    fn has_opcode(opcode: u8) -> bool;

    fn set_key(&mut self, _key: Vec<u8>) {
        return;
    }

    fn set_value(&mut self, _value: Vec<u8>) {
        return;
    }

    fn set_extras(&mut self, _extras: Vec<u8>) {
        return;
    }

    fn from_packet(packet: Packet) -> Result<Self, ProtocolError> {
        let Packet {
            header,
            extras,
            value,
            key,
        } = packet;

        if header.magic != MAGIC_RESPONSE_VALUE {
            return Err(ProtocolError::InvalidMagic(packet.header.magic));
        }
        if !Self::has_opcode(header.opcode) {
            return Err(ProtocolError::OpCodeMismatch);
        }
        if header.vbucket_or_status != 0 {
            return Err(ProtocolError::NonZeroStatus(
                header.vbucket_or_status.into(),
            ));
        }

        let mut response = Self::default();
        response.set_extras(extras);
        response.set_value(value);
        response.set_key(key);
        Ok(response)
    }
}

#[derive(Debug, Default)]
pub struct GetResponse {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub extras: Vec<u8>,
}

impl Response for GetResponse {
    fn has_opcode(opcode: u8) -> bool {
        opcode == GET_OPCODE
            || opcode == GETK_OPCODE
            || opcode == GETQ_OPCODE
            || opcode == GETKQ_OPCODE
    }

    fn set_key(&mut self, key: Vec<u8>) {
        if !key.is_empty() {
            self.key = Some(key)
        }
    }

    fn set_value(&mut self, value: Vec<u8>) {
        if !value.is_empty() {
            self.value = Some(value)
        }
    }

    fn set_extras(&mut self, extras: Vec<u8>) {
        self.extras = extras;
    }
}

#[derive(Debug, Default)]
pub struct SetResponse {
    pub cas: u32,
}

impl Response for SetResponse {
    fn has_opcode(opcode: u8) -> bool {
        opcode == SET_OPCODE
            || opcode == SETQ_OPCODE
            || opcode == ADD_OPCODE
            || opcode == ADDQ_OPCODE
            || opcode == REPLACE_OPCODE
            || opcode == REPLACEQ_OPCODE
    }
}

#[derive(Debug, Default)]
pub struct NoopResponse {}

impl Response for NoopResponse {
    fn has_opcode(opcode: u8) -> bool {
        opcode == NOOP_OPCODE
    }
}

#[derive(Debug)]
pub struct VersionResponse {
    pub value: String,
}

impl TryFrom<Packet> for VersionResponse {
    type Error = ProtocolError;

    fn try_from(packet: Packet) -> Result<Self, Self::Error> {
        Ok(VersionResponse {
            value: std::str::from_utf8(&packet.value)
                .map_err(|_| ProtocolError::InvalidUtf8(packet.value.clone()))?
                .into(),
        })
    }
}
