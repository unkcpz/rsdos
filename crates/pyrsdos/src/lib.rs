use std::{
    collections::HashMap,
    fs::{self},
    io::{self, Cursor, Seek},
    path::PathBuf,
};

use pyo3::{exceptions::PyValueError, prelude::*};
use pyo3_file::PyFileLikeObject;
use rsdos::{
    add_file::{copy_by_chunk, stream_to_loose, stream_to_packs, HashWriter, StoreType},
    object::stream_from_packs_multi,
    status,
    utils::Dir,
    Config, Container, Object,
};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};

#[pyclass(name = "_Container")]
struct PyContainer {
    inner: Container,
}

#[pymethods]
impl PyContainer {
    #[new]
    fn new(folder: PathBuf) -> Self {
        Self {
            inner: Container::new(folder),
        }
    }

    fn get_folder(&self) -> PathBuf {
        self.inner.path.clone()
    }

    #[pyo3(signature = (pack_size_target=4 * 1024 * 1024))]
    fn init_container(&self, pack_size_target: u64) -> PyResult<()> {
        let config = Config::new(pack_size_target);
        self.inner.initialize(&config)?;
        Ok(())
    }

    #[getter]
    fn is_initialised(&self) -> PyResult<bool> {
        Ok(self.inner.validate().is_ok())
    }

    fn stream_to_loose(&self, stream: Py<PyAny>) -> PyResult<(u64, String)> {
        let mut file_like = PyFileLikeObject::with_requirements(stream, true, false, false, false)?;

        stream_to_loose(&mut file_like, &self.inner)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn stream_to_packs(&self, stream: Py<PyAny>) -> PyResult<(u64, String)> {
        let mut file_like = PyFileLikeObject::with_requirements(stream, true, false, false, false)?;

        stream_to_packs(&mut file_like, &self.inner)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    // XXX: this function is almost re-implemented from `rsdos::stream_to_packs_multi` in order to
    // optimize directly in the pyo3 wrapper to see the effect on performance optimization.
    fn stream_to_packs_multi(&self, sources: Vec<Py<PyAny>>) -> PyResult<Vec<String>> {
        let mut results = Vec::with_capacity(sources.len());
        let packs = self.inner.packs()?;
        let mut conn = Connection::open(self.inner.packs_db()?).unwrap();

        let mut current_pack_id: u64 = 0;
        if !Dir(&packs).is_empty().unwrap() {
            for entry in packs.read_dir()? {
                let path = entry?.path();
                if let Some(filename) = path.file_name() {
                    let n = filename.to_string_lossy();
                    let n = n.parse()?;
                    current_pack_id = std::cmp::max(current_pack_id, n);
                }
            }
        }
        // If size of current pack exceed the single pack limit, create next pack
        let p = Dir(&packs).at_path(&format!("{current_pack_id}"));
        let mut fpack = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(p)?;

        let mut fpack = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(Dir(&packs).at_path(&format!("{current_pack_id}")))?;

        let pack_size_target = self.inner.config()?.pack_size_target;

        let tx = conn.transaction().unwrap();
        {
            let mut stmt = tx.prepare_cached("INSERT OR IGNORE INTO db_object (hashkey, compressed, size, offset, length, pack_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)").unwrap();

            for stream in sources {
                // results.push(self.stream_to_packs(file)?);
                let mut file_like =
                    PyFileLikeObject::with_requirements(stream, true, false, false, false)?;

                // Use new pack if size of the current pack reach or exceed the threshold limit
                // TODO: some overhead here in file metadata check
                let offset = if fpack.metadata()?.len() >= pack_size_target {
                    current_pack_id += 1;
                    0
                } else {
                    fpack.seek(io::SeekFrom::End(0))?
                };

                // TODO: need to check if new pack file needed. Create one if needed based on the
                // TODO: Large overhead to create hasher and hashwrite for every stream, it can be
                // reused.
                let hasher = Sha256::new();
                let mut hwriter = HashWriter::new(&fpack, hasher);

                // Pack chunk size: 64 MiB TODO: make it configurable??
                let chunk_size = 65_536;
                let bytes_copied = copy_by_chunk(&mut file_like, &mut hwriter, chunk_size)?;

                let hash = hwriter.hasher.finalize();
                let hash_hex = hex::encode(hash);

                stmt.execute(params![
                    &hash_hex,
                    false,
                    bytes_copied as u64,
                    offset,
                    bytes_copied as u64,
                    current_pack_id
                ])
                .unwrap();
                // .with_context(|| "insert to db")?;

                results.push(hash_hex);
            }
        }
        tx.commit().unwrap();

        Ok(results)
    }

    // This is 2 times fast than write to writer from py world since there is no overhead to cross
    // boundary for every py object.
    fn get_loose_objects_content(&self, hashkeys: Vec<String>) -> HashMap<String, Option<Vec<u8>>> {
        let mut buf = Vec::new();
        hashkeys
            .iter()
            .map(|hashkey| {
                let content =
                    match Object::from_hash(hashkey, &self.inner, &StoreType::Loose).unwrap() {
                        Some(mut obj) => {
                            buf.clear();
                            let mut cursor = Cursor::new(&mut buf);

                            std::io::copy(&mut obj.reader, &mut cursor).unwrap();
                            Some(buf.clone())
                        }
                        _ => None,
                    };
                (hashkey.to_owned(), content)
            })
            .collect()
    }

    fn write_stream_from_loose(&self, hash: &str, py_filelike: Py<PyAny>) -> PyResult<()> {
        Stream::write_from_loose(&self.inner, hash, py_filelike)
    }

    fn write_stream_from_packs(&self, hash: &str, py_filelike: Py<PyAny>) -> PyResult<()> {
        Stream::write_from_packs(&self.inner, hash, py_filelike)
    }

    // XXX: Vec<u8> -> ByteStr ?
    fn stream_from_packs_multi(&self, hashkeys: Vec<String>) -> PyResult<HashMap<String, Vec<u8>>> {
        let mut objs = stream_from_packs_multi(&self.inner, &hashkeys)?;
        let mut buf = Vec::new();
        let res = objs
            .iter_mut()
            .map(|obj| {
                let hashkey = &obj.hashkey;
                buf.clear();
                let mut cursor = Cursor::new(&mut buf);
                std::io::copy(&mut obj.reader, &mut cursor).unwrap();
                (hashkey.to_owned(), buf.clone())

                // NOTE: a bit overhead to copy from buf to buf, in principle can directly take from the memory
                // let cursor = &mut obj.reader;
                // let buf = cursor.get_mut();
                // (hashkey.to_owned(), std::mem::take(buf))
            })
            .collect();

        Ok(res)
    }

    // XXX: combine with get_n_objs and return dicts
    fn get_total_size(&self) -> PyResult<u64> {
        let info = status::stat(&self.inner)?;
        Ok(info.size.loose)
    }

    fn get_n_objs(&self) -> PyResult<u64> {
        let info = status::stat(&self.inner)?;
        Ok(info.count.loose)
    }
}

#[derive(Debug)]
#[pyclass]
struct Stream;

impl Stream {
    fn write_from_loose(cnt: &Container, hash: &str, py_filelike: Py<PyAny>) -> PyResult<()> {
        if let Some(mut obj) = rsdos::Object::from_hash(hash, cnt, &StoreType::Loose)? {
            match PyFileLikeObject::with_requirements(py_filelike, true, false, false, false) {
                Ok(mut fl) => {
                    // copy from reader to writer
                    std::io::copy(&mut obj.reader, &mut fl)?;
                    fl.rewind().unwrap();
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Err(PyErr::new::<PyValueError, _>(format!("{hash} not found")))
        }
    }

    fn write_from_packs(cnt: &Container, hash: &str, py_filelike: Py<PyAny>) -> PyResult<()> {
        if let Some(mut obj) = rsdos::Object::from_hash(hash, cnt, &StoreType::Packs)? {
            match PyFileLikeObject::with_requirements(py_filelike, true, false, false, false) {
                Ok(mut fl) => {
                    // copy from reader to writer
                    std::io::copy(&mut obj.reader, &mut fl)?;
                    fl.rewind().unwrap();
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Err(PyErr::new::<PyValueError, _>(format!("{hash} not found")))
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "rsdos")]
fn pyrsdos(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContainer>()?;
    Ok(())
}
