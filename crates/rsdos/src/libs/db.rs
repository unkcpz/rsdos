use anyhow::Context;
use rusqlite::{params, Connection, OptionalExtension};
use std::{path::PathBuf, u64};

#[derive(Debug, Clone)]
pub struct PackEntry {
    pub hashkey: String,
    pub compressed: bool,
    pub size: u64,
    pub offset: u64,
    pub length: u64,
    pub pack_id: u64,
}

pub fn create(db: &PathBuf) -> anyhow::Result<()> {
    // Create the table if it doesn't already exist
    let conn = Connection::open(db).with_context(|| "create db")?;
    conn.execute_batch(
        "PRAGMA journal_mode = wal;",
    )
    .expect("PRAGMA");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS db_object (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    hashkey VARCHAR NOT NULL,
                    compressed BOOLEAN NOT NULL,
                    size INTEGER NOT NULL,
                    offset INTEGER NOT NULL,
                    length INTEGER NOT NULL,
                    pack_id INTEGER NOT NULL
                )",
        [],
    )?;

    conn.execute(
        "CREATE UNIQUE INDEX ix_db_object_hashkey ON db_object (hashkey)",
        [],
    )
    .with_context(|| "execute create SQL")?;

    Ok(())
}

/// Counting number of packed objects and ``total_size`` if they were loose objects
pub fn stats(db: &PathBuf) -> anyhow::Result<(u64, u64)> {
    let conn = Connection::open(db)
        .with_context(|| format!("Open db {} for auditing", db.to_string_lossy()))?;
    let mut stmt = conn.prepare("SELECT size FROM db_object")?;
    let rows = stmt
        .query([])
        .with_context(|| "query size of objects")?
        .mapped(|row| row.get::<_, u64>(0));

    let mut count = 0;
    let mut total_size = 0;
    for size in rows {
        total_size += size?;
        count += 1;
    }
    Ok((count, total_size))
}

pub fn insert_packin(conn: &Connection, packin: &PackEntry) -> anyhow::Result<()> {
    // NOTE: I use SQL: `INSERT OR IGNORE` to deal with duplicate keys
    let mut stmt = conn.prepare_cached("INSERT OR IGNORE INTO db_object (hashkey, compressed, size, offset, length, pack_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;
    stmt.execute(
            params![packin.hashkey, packin.compressed, packin.size, packin.offset, packin.length, packin.pack_id])
        .with_context(|| format!("insert {packin:?} to db"))?;

    Ok(())
}

pub fn insert(conn: &Connection, hashkey: &str, compressed: bool, size: u64, offset: u64, length: u64, pack_id: u64) -> anyhow::Result<()> {
    // NOTE: I use SQL: `INSERT OR IGNORE` to deal with duplicate keys
    let mut stmt = conn.prepare_cached("INSERT OR IGNORE INTO db_object (hashkey, compressed, size, offset, length, pack_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;
    stmt.execute(
            params![hashkey, compressed, size, offset, length, pack_id])
        .with_context(|| "insert to db")?;

    Ok(())
}


// XXX: sub from select_multiple which only query once
pub fn select(conn: &Connection, hash_hex: &str) -> anyhow::Result<Option<PackEntry>> {
    let mut stmt = conn.prepare_cached("SELECT hashkey, compressed, size, offset, length, pack_id FROM db_object WHERE hashkey = ?1")?;
    let entry = stmt.query_row(params![hash_hex], |row| {
        Ok(PackEntry {
            hashkey: row.get(0)?,
            compressed: row.get(1)?,
            size: row.get(2)?,
            offset: row.get(3)?,
            length: row.get(4)?,
            pack_id: row.get(5)?,
        })
    }).optional()?;

    Ok(entry)
}

