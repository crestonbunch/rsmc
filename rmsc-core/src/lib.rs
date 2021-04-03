//! This crate provides core libraries for rsmc implementations into various
//! async runtimes. If compression is undesired, it is possible to disable the
//! `zlib` feature (on by default.)

pub mod client;
pub(crate) mod protocol;
pub(crate) mod ring;

#[cfg(feature = "zlib")]
pub mod zlib;
