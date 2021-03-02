use std::io::ErrorKind;

use async_trait::async_trait;
use rmsc_core::client::{Connection, Error as CoreError};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

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
    use futures::Future;
    use rmsc_core::client::{ClientConfig, Pool};
    use std::{
        any::Any,
        collections::HashMap,
        io::{BufRead, BufReader, Read},
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
            let name = String::from("test_memcached");
            let mut proc = MemcachedTester::new_proc(&name, port);

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
        tokio_test::block_on(async {
            TokioConnection::connect(String::from("127.0.0.1:11211"))
                .await
                .unwrap();
        })
    }

    #[test]
    fn test_end_to_end() {
        MemcachedTester::new(11211).run(async {
            let cfg = ClientConfig::new(vec![String::from("127.0.0.1:11211")]);
            let pool = Pool::<TokioConnection>::new(cfg, 16);
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
            let mut result = client
                .get_multi(vec![b"abc", b"def", b"qwop"])
                .await
                .unwrap();

            let a = result.remove(&b"abc".to_vec()).unwrap();
            let b = result.remove(&b"def".to_vec()).unwrap();
            let c = result.remove(&b"qwop".to_vec());
            assert_eq!(b"123".to_vec(), a.unwrap());
            assert_eq!(b"456".to_vec(), b.unwrap());
            assert_eq!(true, c.is_none());
        });
    }

    #[test]
    fn test_cluster() {
        MemcachedTester::new_cluster(vec![11211, 11212, 11213]).run(async {
            let cfg = ClientConfig::new(vec![
                String::from("127.0.0.1:11211"),
                String::from("127.0.0.1:11212"),
                String::from("127.0.0.1:11213"),
            ]);
            let pool = Pool::<TokioConnection>::new(cfg, 16);
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
            let mut result = client
                .get_multi(vec![b"abc", b"def", b"qwop"])
                .await
                .unwrap();

            let a = result.remove(&b"abc".to_vec()).unwrap();
            let b = result.remove(&b"def".to_vec()).unwrap();
            let c = result.remove(&b"qwop".to_vec());
            assert_eq!(b"123".to_vec(), a.unwrap());
            assert_eq!(b"456".to_vec(), b.unwrap());
            assert_eq!(true, c.is_none());
        });
    }
}
