use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufReader, Cursor, Seek, Take},
    path::PathBuf,
};

use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};
use pyo3_file::PyFileLikeObject;
use rsdos::{
    add_file::{
        stream_to_loose, stream_to_packs, HashWriter, StoreType, _stream_to_packs, copy_by_chunk,
    },
    status,
    utils::Dir,
    Config, Container, Object,
};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::io::Read;

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

    fn get_object_content(&self, hashkey: &str) -> PyResult<Vec<u8>> {
        match Object::from_hash(hashkey, &self.inner, &StoreType::Loose)? {
            Some(mut obj) => {
                let mut buf = Vec::new();
                let mut cursor = Cursor::new(&mut buf);

                std::io::copy(&mut obj.reader, &mut cursor)?;
                Ok(buf)
            }
            _ => Err(PyValueError::new_err(format!(
                "hash key {hashkey} is not found"
            ))),
        }
    }

    // TODO: a bit faster if I do raw rust wrapper but not enough: 8ms -> 7ms
    fn get_objects_content(&self, hashkeys: Vec<String>) -> PyResult<HashMap<String, Vec<u8>>> {
        let mut buf = Vec::new();
        let d = hashkeys
            .iter()
            .map(|hashkey| {
                // let content = self.get_object_content(k).unwrap();
                let content =
                    match Object::from_hash(hashkey, &self.inner, &StoreType::Loose).unwrap() {
                        Some(mut obj) => {
                            buf.clear();
                            let mut cursor = Cursor::new(&mut buf);

                            std::io::copy(&mut obj.reader, &mut cursor).unwrap();
                            buf.clone()
                        }
                        _ => todo!(),
                    };
                (hashkey.to_string(), content)
            })
            .collect();

        Ok(d)
    }

    // TODO: try here return an iterator, use it in get_objects_content, see if it is getting fast

    // XXX: return an Object struct???
    fn stream_from_loose(
        &self,
        py: Python,
        obj_hash: &str,
    ) -> PyResult<Option<Py<PyLooseStreamObject>>> {
        let obj_path = self
            .inner
            .loose()?
            .join(format!("{}/{}", &obj_hash[..2], &obj_hash[2..]));
        if obj_path.exists() {
            let file_like = PyLooseStreamObject::new(obj_path.to_str().unwrap().to_string())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;

            Ok(Some(Py::new(py, file_like)?))
        } else {
            Ok(None)
        }
    }

    fn stream_from_packs(
        &self,
        py: Python,
        obj_hash: &str,
    ) -> PyResult<Option<Py<PyPacksStreamObject>>> {
        let obj = rsdos::Object::from_hash(obj_hash, &self.inner, &StoreType::Packs)?;
        if let Some(obj) = obj {
            let file_like = PyPacksStreamObject::new(obj)?;
            Ok(Some(Py::new(py, file_like)?))
        } else {
            Ok(None)
        }
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

#[pyclass]
struct PyPacksStreamObject {
    inner: Object<Take<BufReader<File>>>,
}

#[pymethods]
impl PyPacksStreamObject {
    fn read(&mut self, py: Python) -> PyResult<Py<PyBytes>> {
        let mut buf = vec![0; self.inner.expected_size];
        let n = self.inner.reader.read(&mut buf).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to read file: {}", e))
        })?;
        Ok(PyBytes::new_bound(py, &buf[..n]).into())
    }
}

impl PyPacksStreamObject {
    fn new(obj: Object<Take<BufReader<File>>>) -> PyResult<Self> {
        Ok(PyPacksStreamObject { inner: obj })
    }
}

// NOTE: this is re-implement of rsdos::Object without generic (which is for any Reader)
// since pyO3 need non-opaque to wrapped to python class.
#[pyclass]
struct PyLooseStreamObject {
    inner: fs::File,
    size: u64,
}

#[pymethods]
impl PyLooseStreamObject {
    #[new]
    fn new(filename: String) -> PyResult<Self> {
        let file = fs::File::open(filename).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to open file: {}", e))
        })?;
        let size = file.metadata()?.len();
        Ok(PyLooseStreamObject { inner: file, size })
    }

    fn read(&mut self, py: Python) -> PyResult<Py<PyBytes>> {
        let mut buf = vec![0; self.size as usize];
        let n = self.inner.read(&mut buf).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to read file: {}", e))
        })?;
        Ok(PyBytes::new_bound(py, &buf[..n]).into())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "rsdos")]
fn pyrsdos(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContainer>()?;
    Ok(())
}
