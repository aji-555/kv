use std::path::Path;

use sled::{Db, IVec};

use crate::{Kvpair, Storage};

#[derive(Debug)]
pub struct SledDb(Db);

impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }

    fn get_full_key(table: &str, key: &str) -> String {
        format!("{}:{}", table, key)
    }

    fn get_table_prefix(table: &str) -> String {
        format!("{}:", table)
    }
}

/// Option<Result<T,E>> -> Result<Option<T>, E>
fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some))
}

impl Storage for SledDb {
    fn get(&self, table: &str, key: &str) -> Result<Option<crate::Value>, crate::KvError> {
        let name = SledDb::get_full_key(table, key);
        let res = self.0.get(name.as_bytes())?.map(|v| v.as_ref().try_into());
        flip(res)
    }

    fn set(
        &self,
        table: &str,
        key: &str,
        value: crate::Value,
    ) -> Result<Option<crate::Value>, crate::KvError> {
        let name = SledDb::get_full_key(table, key);
        let data: Vec<u8> = value.try_into()?;
        let res = self.0.insert(name, data)?.map(|v| v.as_ref().try_into());
        flip(res)
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, crate::KvError> {
        let name = SledDb::get_full_key(table, key);
        let res = self.0.contains_key(name)?;

        Ok(res)
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<crate::Value>, crate::KvError> {
        let name = SledDb::get_full_key(table, key);
        let res = self.0.remove(name)?.map(|v| v.as_ref().try_into());
        flip(res)
    }

    fn get_all(&self, table: &str) -> Result<Vec<crate::Kvpair>, crate::KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let res = self.0.scan_prefix(prefix).map(|v| v.into()).collect();

        Ok(res)
    }

    fn get_iter(
        &self,
        table: &str,
    ) -> Result<Box<dyn Iterator<Item = crate::Kvpair>>, crate::KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let iter = self.0.scan_prefix(prefix).into_iter().map(|v| v.into());
        Ok(Box::new(iter))
    }
}

impl From<Result<(IVec, IVec), sled::Error>> for Kvpair {
    fn from(value: Result<(IVec, IVec), sled::Error>) -> Self {
        match value {
            Ok((k, v)) => match v.as_ref().try_into() {
                Ok(x) => Kvpair::new(ivec_to_key(k.as_ref()), x),
                Err(_) => Kvpair::default(),
            },
            _ => Kvpair::default(),
        }
    }
}

fn ivec_to_key(ivec: &[u8]) -> String {
    let s = String::from_utf8_lossy(ivec);
    let mut iter = s.split(":");
    iter.next();
    iter.next().unwrap().into()
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn it_should_work() {
        let v: Option<Result<(), io::Error>> = Some(Err(io::ErrorKind::Other.into()));
        let v1 = flip(v);
        match v1 {
            Err(_) => println!("Err"),
            _ => println!("other"),
        }
    }
}
