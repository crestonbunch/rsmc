# rsmc-tokio

This crate aims to provide a full-features memcached client for the
[Tokio async runtime](https://crates.io/crates/tokio) using rsmc-core.

This is still an early implementation, so expect some bugs and
missing features. If you find something is wrong, please open
a GitHub issue (or, even better, a PR to fix the issue!)

Expect some breaking changes before a 1.0 release.

Features:

- [x] Async
- [x] Connection pooling provided by [deadpool](https://crates.io/crates/deadpool)
- [ ] TLS support
- [x] Binary protocol support
  - [x] get, multi_get
  - [x] set, multi_set
  - [x] delete, multi_delete
  - [ ] add, replace
  - [ ] increment, decrement
- [x] Consistent hashing
  - [ ] Support for different hashing algorithms.
- [x] Compression
  - [x] Support for different compression algorithms.

## Quick start

```rust
use flate2::Compression;
use rsmc_core::{
    client::{ClientConfig, Pool},
    zlib::ZlibCompressor,
};
use rsmc_tokio::TokioConnection;


// Consistent hashing is used to distribute keys
// evenly across a memcached cluster.
let memcached_servers = vec![
    "localhost:11211",
    "localhost:11212",
    "localhost:11213",
];

// Use `ClientConfig::new_uncompressed()` if compression is not desired.
// You can disable the `zlib` feature (on by default) to disable it entirely.
let cfg = ClientConfig::new(memcached_servers, ZlibCompressor::default());

// Create a connection pool with (at most) 16 connections per server.
let pool = Pool::<TokioConnection, _>::new(cfg, 16);
let mut client = pool.get().await.unwrap();

client.set(b"hello", b"world", 300).await.unwrap();
let response: Option<Vec<u8>> = client.get(b"hello").await.unwrap(); // "world"
```
