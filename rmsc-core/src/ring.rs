use murmur3::murmur3_32;

use crate::client::{Connection, Error};

const DEFAULT_SIZE: usize = 360;

/// A ring manages multiple connections, using consistent hashing
/// to map a key to a connection in the ring. If a connection is
/// added or removed, then only a fraction of the keys need to
/// be reshuffled.
#[derive(Debug, Clone)]
pub struct Ring<C: Connection> {
    conns: Vec<C>,
    buckets: Vec<(u32, usize)>,
}

impl<C: Connection> Ring<C> {
    /// Create a new ring with the default size.
    pub async fn new(urls: Vec<String>) -> Result<Self, Error> {
        Ring::new_with_size(urls, DEFAULT_SIZE).await
    }

    /// Create a new ring with a custom size. The size divides the
    /// ring into buckets so that each connection owns some fraction
    /// of the buckets in the ring.
    pub async fn new_with_size(urls: Vec<String>, size: usize) -> Result<Self, Error> {
        let mut conns = vec![];
        let mut buckets = vec![];
        // In this scheme, each connection gets an equal share of the ring space.
        let share = size / urls.len();
        for (conn_index, url) in urls.into_iter().enumerate() {
            for i in 0..share {
                let k = murmur3_32(&mut url.as_bytes(), i as u32)?;
                buckets.push((k, conn_index))
            }
            conns.push(C::connect(url).await?);
        }

        buckets.sort_unstable();
        Ok(Self { conns, buckets })
    }

    /// Get the connection owning the bucket containing the given key.
    pub fn get_conn(&mut self, key: &[u8]) -> Result<&mut C, Error> {
        let conn_index = self.find_bucket(key);
        Ok(&mut self.conns[conn_index])
    }

    /// Group multiple keys and the connections that own the keys.
    pub fn get_conns<'a, 'b>(&'b mut self, keys: Vec<&'a [u8]>) -> Vec<(&'b mut C, Vec<&'a [u8]>)> {
        let pipelines = self.get_pipelines(keys);
        self.into_iter()
            .zip(pipelines)
            .filter(|(_, pipeline)| !pipeline.is_empty())
            .collect()
    }

    fn get_pipelines<'a>(&self, keys: Vec<&'a [u8]>) -> Vec<Vec<&'a [u8]>> {
        let mut out = vec![vec![]; self.conns.len()];
        for key in keys {
            let conn_index = self.find_bucket(key);
            out[conn_index].push(key);
        }
        out
    }

    fn find_bucket(&self, mut key: &[u8]) -> usize {
        // Find the position of the hash on the ring
        let ring_pos = murmur3_32(&mut key, 0).unwrap();
        // Find the bucket containing the ring position
        let bucket_search = self.buckets.binary_search_by_key(&ring_pos, |(i, _)| *i);
        let bucket_index = bucket_search.unwrap_or_else(|next_bucket| next_bucket);
        // Return the connection owning that bucket
        let (_, conn_index) = self.buckets.get(bucket_index).unwrap_or(&self.buckets[0]);
        *conn_index
    }
}

#[cfg(test)]
mod tests {
    use crate::client::{Connection, Error};
    use async_trait::async_trait;

    use super::Ring;

    #[derive(Debug, Clone)]
    struct TestConn {
        url: String,
    }

    #[async_trait]
    impl Connection for TestConn {
        async fn connect(url: String) -> Result<Self, Error> {
            Ok(TestConn { url })
        }
        async fn read(&mut self, _: &mut Vec<u8>) -> Result<usize, Error> {
            Ok(0)
        }
        async fn write(&mut self, _: &[u8]) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn test_get_conn() {
        tokio_test::block_on(async {
            let a = "localhost:11211";
            let b = "localhost:11212";
            let c = "localhost:11213";
            let urls = vec![a.to_string(), b.to_string(), c.to_string()];
            let mut ring = Ring::<TestConn>::new(urls).await.unwrap();
            assert_eq!(a, ring.get_conn(a.as_bytes()).unwrap().url);
            assert_eq!(b, ring.get_conn(b.as_bytes()).unwrap().url);
            assert_eq!(c, ring.get_conn(c.as_bytes()).unwrap().url);
            assert_eq!(c, ring.get_conn(b"").unwrap().url);
            assert_eq!(c, ring.get_conn(b"q").unwrap().url);
            assert_eq!(a, ring.get_conn(b"-").unwrap().url);
        });
    }

    #[test]
    fn test_boundary_behavior() {
        tokio_test::block_on(async {
            let urls = vec!["localhost:11211".to_string(), "localhost:11212".to_string()];
            let mut ring = Ring::<TestConn>::new_with_size(urls, 2).await.unwrap();
            assert_eq!(vec![(748582396, 1), (1636863978, 0)], ring.buckets);
            assert_eq!("localhost:11212", ring.get_conn(b"q").unwrap().url);
        });
    }
}

impl<'a, C: Connection> IntoIterator for &'a mut Ring<C> {
    type Item = &'a mut C;
    type IntoIter = std::slice::IterMut<'a, C>;

    fn into_iter(self) -> Self::IntoIter {
        self.conns[..].iter_mut()
    }
}
