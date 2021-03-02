#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub enum ProtocolError {
    InvalidMagic(u8),
    PacketTooSmall,
    BodySizeMismatch,
}
