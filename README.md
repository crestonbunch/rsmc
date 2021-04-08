# rsmc

This provides a full-featured async memcached client for multiple async
runtimes (currently only tokio, async-std to follow soon.)

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

# Usage

See child crates for more details

- [rsmc-tokio](rsmc-tokio/README.md)
