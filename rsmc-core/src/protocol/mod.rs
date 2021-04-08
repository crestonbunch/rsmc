mod error;
mod packet;

pub use error::{ProtocolError, Status};
pub(crate) use packet::{Header, Packet};

const MAGIC_REQUEST_VALUE: u8 = 0x80;
const MAGIC_RESPONSE_VALUE: u8 = 0x81;

const GET_OPCODE: u8 = 0x00;
const GETK_OPCODE: u8 = 0x0c;
const GETQ_OPCODE: u8 = 0x09;
const GETKQ_OPCODE: u8 = 0x0d;

const SET_OPCODE: u8 = 0x01;
const SETQ_OPCODE: u8 = 0x11;
const ADD_OPCODE: u8 = 0x02;
const ADDQ_OPCODE: u8 = 0x12;
const REPLACE_OPCODE: u8 = 0x03;
const REPLACEQ_OPCODE: u8 = 0x13;
const DELETE_OPCODE: u8 = 0x04;

const NOOP_OPCODE: u8 = 0x0a;
const VERSION_OPCODE: u8 = 0x0b;
