use anyhow::{Context, Ok};
use rusqlite::Connection;
use std::{path::PathBuf, u64};

pub const PACKS_DB: &str = "packs.idx";

pub fn create(db: &PathBuf) -> anyhow::Result<()> {
    // Create the table if it doesn't already exist
    let conn = Connection::open(db).with_context(|| "create db")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS db_object (
                    id INTEGER NOT NULL,
                    hashkey VARCHAR NOT NULL,
                    compressed BOOLEAN NOT NULL,
                    size INTEGER NOT NULL,
                    offset INTEGER NOT NULL,
                    length INTEGER NOT NULL,
                    pack_id INTEGER NOT NULL,
                    PRIMARY KEY (id)
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

pub fn stats(db: &PathBuf) -> anyhow::Result<(u64, u64)> {
    let conn = Connection::open(db)
        .with_context(|| format!("Open db {} for audit", db.to_string_lossy()))?;
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
