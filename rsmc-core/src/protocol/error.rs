use std::{
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Status {
    UnknownStatus,
    NoError,
    KeyNotFound,
    KeyExists,
    ValueTooLarge,
    InvalidArguments,
    ItemNotStored,
    IncrDecrOnNonNumericValue,
    VbucketBelongsToAnotherServer,
    AuthenticationError,
    AuthenticationContinue,
    UnknownCommand,
    OutOfMemory,
    NotSupported,
    InternalError,
    Busy,
    TemporaryFailure,
}

impl From<u16> for Status {
    fn from(val: u16) -> Self {
        match val {
            0x00 => Status::NoError,
            0x01 => Status::KeyNotFound,
            0x02 => Status::KeyExists,
            0x03 => Status::ValueTooLarge,
            0x04 => Status::InvalidArguments,
            0x05 => Status::ItemNotStored,
            0x06 => Status::IncrDecrOnNonNumericValue,
            0x07 => Status::VbucketBelongsToAnotherServer,
            0x08 => Status::AuthenticationError,
            0x09 => Status::AuthenticationContinue,
            0x81 => Status::UnknownCommand,
            0x82 => Status::OutOfMemory,
            0x83 => Status::NotSupported,
            0x84 => Status::InternalError,
            0x85 => Status::Busy,
            0x86 => Status::TemporaryFailure,
            _ => Status::UnknownStatus,
        }
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Status::UnknownStatus => write!(f, "Unknown status"),
            Status::NoError => write!(f, "No error"),
            Status::KeyNotFound => write!(f, "Key not found"),
            Status::KeyExists => write!(f, "Key exists"),
            Status::ValueTooLarge => write!(f, "Value too large"),
            Status::InvalidArguments => write!(f, "Invalid arguments"),
            Status::ItemNotStored => write!(f, "Item not stored"),
            Status::IncrDecrOnNonNumericValue => write!(f, "Incr or decr on non-numeric value"),
            Status::VbucketBelongsToAnotherServer => write!(f, "Vbucket belongs to another server"),
            Status::AuthenticationError => write!(f, "Authentication error"),
            Status::AuthenticationContinue => write!(f, "Authentication continue"),
            Status::UnknownCommand => write!(f, "Unknown command"),
            Status::OutOfMemory => write!(f, "Out of memory"),
            Status::NotSupported => write!(f, "Not supported"),
            Status::InternalError => write!(f, "Internal error"),
            Status::Busy => write!(f, "Busy"),
            Status::TemporaryFailure => write!(f, "TemporaryFailure"),
        }
    }
}

impl StdError for Status {}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ProtocolError {
    InvalidMagic(u8),
    PacketTooSmall,
    BodySizeMismatch,
}

impl Display for ProtocolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ProtocolError::InvalidMagic(byte) => write!(f, "Invalid magic byte: {}", byte),
            ProtocolError::PacketTooSmall => write!(f, "Packet too small"),
            ProtocolError::BodySizeMismatch => write!(f, "Body size mismatch"),
        }
    }
}

impl StdError for ProtocolError {}
