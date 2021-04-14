use async_trait::async_trait;
use rsmc_core::client::{Connection, Error as CoreError};
use std::{ops::DerefMut, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
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
#[derive(Debug, Clone)]
pub struct TokioConnection {
    stream: Arc<Mutex<TcpStream>>,
}

#[async_trait]
impl Connection for TokioConnection {
    async fn connect(url: String) -> Result<Self, CoreError> {
        let stream = TcpStream::connect(url).await?;
        let stream = Arc::new(Mutex::new(stream));
        Ok(TokioConnection { stream })
    }

    async fn read(&mut self, buf: &mut Vec<u8>) -> Result<usize, CoreError> {
        let mut lock = self.stream.lock().await;
        let stream = lock.deref_mut();
        Ok(stream.read(buf).await?)
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), CoreError> {
        let mut lock = self.stream.lock().await;
        let stream = lock.deref_mut();
        Ok(stream.write_all(data).await?)
    }
}

#[cfg(test)]
mod test {
    use flate2::Compression;
    use futures::Future;
    use rand::prelude::*;
    use rsmc_core::client::Compressor;
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

    async fn test_run<P: Compressor>(pool: Pool<P>) {
        let mut client = pool.get().await.unwrap();

        for (k, v) in &[
            ("key", "value"),
            ("hello", "world"),
            ("abc", "123"),
            ("dead", "beef"),
        ] {
            assert_eq!(None, client.get::<_, String>(k).await.unwrap());
            assert_eq!(None, client.get::<_, String>(k).await.unwrap());
            assert_eq!((), client.set(k, v, 1).await.unwrap());
            let expect = Some(v.to_string());
            let actual = client.get::<_, String>(k).await.unwrap();
            assert_eq!(expect, actual);

            assert_eq!((), client.delete(k).await.unwrap());
            assert_eq!(None, client.get::<_, String>(k).await.unwrap());
        }

        for map in &[
            &[("key", "value"), ("hello", "world")],
            &[("abc", "123"), ("dead", "beef")],
        ] {
            let keys = map.iter().map(|(k, _)| k.as_bytes()).collect::<Vec<_>>();
            let hash_map = map.iter().fold(HashMap::new(), |mut acc, (k, v)| {
                acc.insert(k.as_bytes(), *v);
                acc
            });

            let (result, _) = client.get_multi::<_, String>(&keys).await.unwrap();
            assert_eq!(0, result.len());

            client.set_multi(hash_map.clone(), 1).await.unwrap();

            let get_keys = [keys.clone(), vec![b"not found"]].concat();
            let (result, _) = client.get_multi::<_, String>(&get_keys).await.unwrap();
            assert_eq!(keys.len(), result.len());
            result.into_iter().for_each(|(k, v)| {
                let expect = hash_map.get(&k[..]).unwrap();
                assert_eq!(*expect, v);
            });

            client.delete_multi(&keys).await.unwrap();
            let (result, _) = client.get_multi::<_, String>(&keys).await.unwrap();
            assert_eq!(0, result.len());
        }
    }

    #[test]
    fn test_single_connection() {
        let mut rng = rand::thread_rng();
        let random_port = rng.gen_range(20000..30000);
        MemcachedTester::new(random_port).run(async {
            let host = format!("127.0.0.1:{}", random_port);
            let cfg = ClientConfig::new_uncompressed(vec![host]);
            let pool = Pool::new(cfg, 16);
            test_run(pool).await;
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
            test_run(pool).await;
        });
    }
}
