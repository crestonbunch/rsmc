use async_trait::async_trait;
use rsmc_core::client::{Connection, Error as CoreError};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub use rsmc_core::client::ClientConfig;
#[cfg(feature = "zlib")]
pub use rsmc_core::zlib::ZlibCompressor;

/// A pool of connections to memcached using tokio for async I/O and
/// the desired compression scheme. Use this to create a connection pool.
/// For example:
///
/// ```ignore
/// use rsmc_tokio::{Pool, ClientConfig};
///
/// let cfg = ClientConfig::new_uncompressed(vec!["localhost:11211".into()]);
/// let pool = Pool::new(cfg, 16);
/// ```
pub type Pool<C> = rsmc_core::client::Pool<TokioConnection, C>;

/// A TokioConnection uses the tokio runtime to form TCP connections to
/// memcached.
#[derive(Debug)]
pub struct TokioConnection {
    stream: TcpStream,
}

#[async_trait]
impl Connection for TokioConnection {
    async fn connect(url: String) -> Result<Self, CoreError> {
        let stream = TcpStream::connect(url).await?;
        Ok(TokioConnection { stream })
    }

    async fn read(&mut self, buf: &mut Vec<u8>) -> Result<usize, CoreError> {
        Ok(self.stream.read(buf).await?)
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), CoreError> {
        Ok(self.stream.write_all(data).await?)
    }
}

#[cfg(test)]
mod test {
    use flate2::Compression;
    use futures::Future;
    use rand::prelude::*;
    use std::{
        collections::HashMap,
        io::{BufRead, BufReader},
        process::{Child, Command, Stdio},
    };

    use super::*;

    #[derive(Debug)]
    struct MemcachedTester {
        names: Vec<String>,
        procs: Vec<Child>,
    }

    impl MemcachedTester {
        fn new(port: usize) -> Self {
            let name = format!("test_memcached_{}", port);
            let proc = MemcachedTester::new_proc(&name, port);

            Self {
                procs: vec![proc],
                names: vec![name],
            }
        }

        fn new_cluster(ports: Vec<usize>) -> Self {
            let (names, procs) = ports
                .into_iter()
                .enumerate()
                .map(|(i, port)| {
                    let name = format!("test_memcached_{}", i);
                    let proc = MemcachedTester::new_proc(&name, port);
                    (name, proc)
                })
                .unzip();

            Self { procs, names }
        }

        fn new_proc(name: &str, port: usize) -> Child {
            let mut proc = Command::new("docker")
                .args(&[
                    "run",
                    "--rm",
                    "-t",
                    "--name",
                    name,
                    "-p",
                    &format!("{}:11211", port),
                    "memcached",
                    "memcached",
                    "-vv",
                ])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();

            let stdout = proc.stdout.as_mut().unwrap();
            let mut reader = BufReader::new(stdout);
            let mut buf = String::new();
            reader.read_line(&mut buf).unwrap();

            proc
        }

        fn run<F: Future>(self, call: F) {
            tokio_test::block_on(call);
        }
    }

    impl Drop for MemcachedTester {
        fn drop(&mut self) {
            for name in self.names.iter() {
                Command::new("docker")
                    .args(&["stop", &name])
                    .output()
                    .unwrap();
            }

            for proc in self.procs.iter_mut() {
                proc.wait().unwrap();
            }
        }
    }

    #[test]
    fn test_connect() {
        let mut rng = rand::thread_rng();
        let random_port = rng.gen_range(10000..20000);
        MemcachedTester::new(random_port).run(async {
            let host = format!("127.0.0.1:{}", random_port);
            TokioConnection::connect(host).await.unwrap();
        })
    }

    #[test]
    fn test_end_to_end() {
        let mut rng = rand::thread_rng();
        let random_port = rng.gen_range(20000..30000);
        MemcachedTester::new(random_port).run(async {
            let host = format!("127.0.0.1:{}", random_port);
            let cfg = ClientConfig::new_uncompressed(vec![host]);
            let pool = Pool::new(cfg, 16);
            let mut client = pool.get().await.unwrap();

            assert_eq!(None, client.get(b"key").await.unwrap());
            assert_eq!(None, client.get(b"key").await.unwrap());
            assert_eq!((), client.set(b"key", b"hello", 1).await.unwrap());
            assert_eq!(Some(b"hello".to_vec()), client.get(b"key").await.unwrap());

            assert_eq!((), client.set(b"key", b"world", 1).await.unwrap());
            assert_eq!(Some(b"world".to_vec()), client.get(b"key").await.unwrap());

            let mut multi_data = HashMap::new();
            multi_data.insert(b"abc".to_vec(), b"123".to_vec());
            multi_data.insert(b"def".to_vec(), b"456".to_vec());
            client.set_multi(multi_data, 1).await.unwrap();
            let (result, _) = client
                .get_multi(vec![b"abc", b"def", b"qwop"])
                .await
                .unwrap();

            assert_eq!(b"123".to_vec(), result[&b"abc".to_vec()]);
            assert_eq!(b"456".to_vec(), result[&b"def".to_vec()]);
            assert_eq!(None, result.get(&b"qwop".to_vec()));

            assert_eq!((), client.delete(b"abc").await.unwrap());
            assert_eq!(None, client.get(b"abc").await.unwrap());
            client.delete_multi(vec![b"key", b"def"]).await.unwrap();
            assert_eq!(None, client.get(b"key").await.unwrap());
            assert_eq!(None, client.get(b"def").await.unwrap());

            assert_eq!(None, client.get(b"qwop").await.unwrap());
            assert_eq!((), client.delete(b"qwop").await.unwrap());
            assert_eq!(None, client.get(b"qwop").await.unwrap());
        });
    }

    #[test]
    fn test_cluster() {
        let rng = &mut rand::thread_rng();
        let mut random_ports = (30001..40000).collect::<Vec<_>>();
        random_ports.shuffle(rng);
        let random_ports: Vec<_> = random_ports[0..3].into();
        MemcachedTester::new_cluster(random_ports.clone()).run(async {
            let cfg = ClientConfig::new(
                random_ports
                    .into_iter()
                    .map(|port| format!("127.0.0.1:{}", port))
                    .collect(),
                ZlibCompressor::new(Compression::default(), 1),
            );
            let pool = Pool::new(cfg, 16);
            let mut client = pool.get().await.unwrap();

            assert_eq!(None, client.get(b"key").await.unwrap());
            assert_eq!(None, client.get(b"key").await.unwrap());
            assert_eq!((), client.set(b"key", b"hello", 1).await.unwrap());
            assert_eq!(Some(b"hello".to_vec()), client.get(b"key").await.unwrap());

            assert_eq!((), client.set(b"key", b"world", 1).await.unwrap());
            assert_eq!(Some(b"world".to_vec()), client.get(b"key").await.unwrap());

            let mut multi_data = HashMap::new();
            multi_data.insert(b"abc".to_vec(), b"123".to_vec());
            multi_data.insert(b"def".to_vec(), b"456".to_vec());
            client.set_multi(multi_data, 1).await.unwrap();
            let (result, _) = client
                .get_multi(vec![b"abc", b"def", b"qwop"])
                .await
                .unwrap();

            assert_eq!(b"123".to_vec(), result[&b"abc".to_vec()]);
            assert_eq!(b"456".to_vec(), result[&b"def".to_vec()]);
            assert_eq!(None, result.get(&b"qwop".to_vec()));

            assert_eq!(b"123".to_vec(), result[&b"abc".to_vec()]);
            assert_eq!(b"456".to_vec(), result[&b"def".to_vec()]);
            assert_eq!(None, result.get(&b"qwop".to_vec()));

            assert_eq!((), client.delete(b"abc").await.unwrap());
            assert_eq!(None, client.get(b"abc").await.unwrap());
            client.delete_multi(vec![b"key", b"def"]).await.unwrap();
            assert_eq!(None, client.get(b"key").await.unwrap());
            assert_eq!(None, client.get(b"def").await.unwrap());

            assert_eq!(None, client.get(b"qwop").await.unwrap());
            assert_eq!((), client.delete(b"qwop").await.unwrap());
            assert_eq!(None, client.get(b"qwop").await.unwrap());
        });
    }
}
