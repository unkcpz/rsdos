use anyhow::Context;
use std::{path::PathBuf, u64};
use serde::{Serialize, Deserialize};
use bincode;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PackEntry {
    pub raw_size: u64,  // uncompressed size
    pub compressed: bool,
    pub size: u64,  // real size occupyed in pack
    pub offset: u64,
    pub pack_id: u64,
}

/// Create db
pub fn create(db: &PathBuf) -> anyhow::Result<()> {
    // Create the table if it doesn't already exist
    let config = sled::Config::default()
        .path(db)
        .cache_capacity(10_000_000_000)
        .flush_every_ms(Some(1000));

    config.open()?;

    Ok(())
}

/// Counting number of packed objects and ``total_size`` if they were loose objects (`raw_size`)
pub fn stats(db: &sled::Db) -> anyhow::Result<(u64, u64)> {
    let mut count = 0;
    let mut total_size = 0;
    for ret in db.iter() {
        let (_, value) = ret?;
        let pn: PackEntry = bincode::deserialize(&value).expect("failed to deserialize pack obj");
        total_size += pn.raw_size;
        count += 1;
    }
    Ok((count, total_size))
}

pub fn insert(db: &sled::Db, hashkey: &str, pn: &PackEntry) -> anyhow::Result<()> {
    let value = bincode::serialize(pn).with_context(|| format!("failed to serialize {pn:?} to bincode"))?;
    db.insert(hashkey, value).with_context(|| "insert failed")?;
    Ok(())
}

// XXX: sub from select_multiple which only query once
pub fn select(db: &sled::Db, hashkey: &str) -> anyhow::Result<Option<PackEntry>> {
    let value = db.get(hashkey)?;
    if let Some(value) = value {
        let pn: PackEntry = bincode::deserialize(&value).expect("failed so deserialize pack obj");
        Ok(Some(pn))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::container::PACKS_DB;

    use super::*;

    #[test]
    fn db_create() {
        let dir = TempDir::new().unwrap();
        let db = dir.as_ref().join(PACKS_DB);
        create(&db).unwrap();

        assert!(db.exists());
    }

    #[test]
    fn db_insert_and_select() {
        let dir = TempDir::new().unwrap();
        let db = sled::open(dir.as_ref().join(PACKS_DB)).unwrap();
        let pn = PackEntry {
            raw_size: 1,
            compressed: false,
            size: 2,
            offset: 200,
            pack_id: 5,
        };
        
        let hashkey = "random";
        insert(&db, hashkey, &pn).unwrap();
        
        let got_pn = select(&db, hashkey).unwrap().unwrap();

        assert_eq!(got_pn, pn);
    }

    #[test]
    fn db_insert_and_stat() {
        let dir = TempDir::new().unwrap();
        let db = sled::open(dir.as_ref().join(PACKS_DB)).unwrap();
        let pn = PackEntry {
            raw_size: 2,
            compressed: false,
            size: 2,
            offset: 200,
            pack_id: 5,
        };
        
        insert(&db, "key1", &pn).unwrap();
        insert(&db, "key2", &pn).unwrap();
        
        let (count, tsize) = stats(&db).unwrap();

        assert_eq!(count, 2);
        assert_eq!(tsize, 4);
    }
}
