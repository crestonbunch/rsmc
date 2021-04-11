use flate2::{
    write::{ZlibDecoder, ZlibEncoder},
    Compression,
};
use std::io::Write;

use crate::{
    client::{Compressor, Error},
    protocol::Packet,
};

/// The minimum number of bytes before the Zlib compressor starts
/// compressing data. About 5 times the size of a packet header.
pub const DEFAULT_MIN_BYTES: usize = 128;

/// A compressor that implements zlib compression and decompression.
#[derive(Debug, Clone, Copy)]
pub struct ZlibCompressor {
    compression: Compression,
    min_bytes: usize,
}

impl ZlibCompressor {
    /// Construct a new zlib compressor with the given compression
    /// ratio and min_bytes. Packets smaller than min_bytes will not
    /// get compressed by the Zlib compressor.
    pub fn new(compression: Compression, min_bytes: usize) -> Self {
        ZlibCompressor {
            compression,
            min_bytes,
        }
    }
}

impl Default for ZlibCompressor {
    fn default() -> Self {
        ZlibCompressor::new(Compression::default(), DEFAULT_MIN_BYTES)
    }
}

impl Compressor for ZlibCompressor {
    fn compress(&self, mut packet: Packet) -> Result<Packet, Error> {
        if packet.value.len() < self.min_bytes {
            return Ok(packet);
        }

        let mut out = vec![];
        let mut enc = ZlibEncoder::new(&mut out, self.compression);
        enc.write_all(&packet.value)?;
        enc.finish()?;

        // Update the header lengths to match the new value.
        let key_len = packet.header.key_length as u32;
        let ext_len = packet.header.extras_length as u32;
        let val_len = out.len() as u32;
        packet.header.body_len = key_len + ext_len + val_len;
        // Set a flag indicating that this data is compressed with zlib.
        // NB: extras must be non-empty to compress packets.
        packet.extras[0] = 1;
        packet.value = out;
        Ok(packet)
    }

    fn decompress(&self, mut packet: Packet) -> Result<Packet, Error> {
        if packet.extras.get(0) != Some(&1) {
            // This packet did not have the compression flag enabled.
            return Ok(packet);
        }

        let mut out = vec![];
        let mut dec = ZlibDecoder::new(&mut out);
        dec.write_all(&packet.value)?;
        dec.finish()?;

        // Update the header lengths to match the new value.
        let key_len = packet.header.key_length as u32;
        let ext_len = packet.header.extras_length as u32;
        let val_len = out.len() as u32;
        packet.header.body_len = key_len + ext_len + val_len;
        // Unset the flag indicating that this data is compressed with zlib.
        packet.extras[0] = 0;
        packet.value = out;
        Ok(packet)
    }
}

#[cfg(test)]
mod tests {
    use flate2::Compression;

    use crate::{
        client::Compressor,
        protocol::{Packet, SetExtras},
    };

    use super::ZlibCompressor;

    #[test]
    fn test_zlib() {
        let compressor = ZlibCompressor::new(Compression::new(9), 1);

        let key = b"my_test_key";
        let value = b"0000000000000000000000000000000000000000000000";
        let packet = Packet::set(&key[..], &value[..], SetExtras::new(0, 300)).unwrap();

        let compressed = compressor.compress(packet.clone()).unwrap();
        let uncompressed = compressor.decompress(compressed.clone()).unwrap();

        assert!(compressed.header.body_len < packet.header.body_len);
        assert_eq!(packet, uncompressed);
    }
}
