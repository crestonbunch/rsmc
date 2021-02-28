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
    use rmsc_core::client::{ClientConfig, Pool};

    use super::*;

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
        tokio_test::block_on(async {
            let cfg = ClientConfig::new(vec![String::from("127.0.0.1:11211")]);
            let pool = Pool::<TokioConnection>::new(cfg, 16);
            let mut client = pool.get().await.unwrap();

            assert_eq!(None, client.get(b"key").await.unwrap());
            assert_eq!(None, client.get(b"key").await.unwrap());
            assert_eq!((), client.set(b"key", b"hello", 1).await.unwrap());
            assert_eq!(Some(b"hello".to_vec()), client.get(b"key").await.unwrap());

            assert_eq!((), client.set(b"key", b"world", 1).await.unwrap());
            assert_eq!(Some(b"world".to_vec()), client.get(b"key").await.unwrap());
        })
    }
}
