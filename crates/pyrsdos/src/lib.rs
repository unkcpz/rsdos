use std::{
    collections::HashMap,
    io::{Cursor, Read, Seek},
    path::PathBuf,
    str::FromStr,
};

use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};
use pyo3_file::PyFileLikeObject;
use rsdos::{
    container::{Compression, PACKS_DB},
    db,
    io::{ByteString, ReaderMaker},
    Config, Container,
};

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

    fn _init_db(&self) -> PyResult<()> {
        let db = self.inner.path.join(PACKS_DB);
        db::create(&db)?;

        Ok(())
    }

    fn get_folder(&self) -> PathBuf {
        self.inner.path.clone()
    }

    #[pyo3(signature = (pack_size_target=4 * 1024 * 1024, compression_algorithm="zlib:+1"))]
    fn init_container(&self, pack_size_target: u64, compression_algorithm: &str) -> PyResult<()> {
        let config = Config::new(pack_size_target, compression_algorithm);
        self.inner.initialize(&config)?;
        Ok(())
    }

    #[getter]
    fn is_initialised(&self) -> bool {
        self.inner.valid().is_ok()
    }

    fn insert_to_loose(&self, stream: Py<PyAny>) -> PyResult<(u64, String)> {
        let file_like = PyFileLikeObject::with_requirements(stream, true, false, false, false)?;
        let stream = Stream { fl: file_like };

        rsdos::io_loose::insert(stream, &self.inner)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn insert_to_packs(&self, stream: Py<PyAny>) -> PyResult<(u64, String)> {
        let file_like = PyFileLikeObject::with_requirements(stream, true, false, false, false)?;
        let stream = Stream { fl: file_like };

        rsdos::io_packs::insert(stream, &self.inner)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn insert_many_to_packs(
        &self,
        py: Python,
        sources: Vec<Py<PyBytes>>,
    ) -> PyResult<Vec<(u64, String)>> {
        let sources = sources.iter().map(|s| {
            let b = s.bind(py);
            b.as_bytes().to_vec()
        });

        rsdos::io_packs::insert_many(sources, &self.inner)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    // This is 2 times fast than write to writer from py world since there is no overhead to cross
    // boundary for every py object.
    fn extract_many_from_loose(&self, hashkeys: Vec<String>) -> HashMap<String, Option<ByteString>> {
        let mut buf = Vec::new();
        hashkeys
            .iter()
            .map(|hashkey| {
                let content = match rsdos::io_loose::extract(hashkey, &self.inner).unwrap() {
                    Some(obj) => {
                        buf.clear();
                        // XXX: no need of using Cursor should use buffer reader
                        let mut cursor = Cursor::new(&mut buf);
                        let mut rdr = obj.make_reader().unwrap();

                        std::io::copy(&mut rdr, &mut cursor).unwrap();
                        Some(buf.clone())
                    }
                    _ => None,
                };
                (hashkey.to_owned(), content)
            })
            .collect()
    }

    fn pack_all_loose(&self, compress_mode: &str) -> PyResult<()> {
        // NOTE: compress_mode passed to here are: "no", "yes", "keep", "auto".
        // In legacy dos, "keep" is equivelant to "no" when pack from loose.
        let compression = match compress_mode {
            "no" | "keep" => Compression::from_str("none").unwrap(),
            "yes" => {
                let algo = self
                    .inner
                    .config()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                    .compression_algorithm;
                Compression::from_str(&algo)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?
            }
            _ => {
                todo!()
            }
        };
        rsdos::maintain::_pack_loose_internal(&self.inner, &compression)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))
    }

    fn extract_many_from_packs(
        &self,
        hashkeys: Vec<String>,
    ) -> PyResult<HashMap<String, ByteString>> {
        let objs = rsdos::io_packs::extract_many(&hashkeys, &self.inner)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;

        let res = objs
            .map(|obj| {
                let hashkey = &obj.id;
                let b = match obj.to_bytes() {
                    Ok(b) => b,
                    _ => b"".to_vec(), // Will this happened? should I just panic??
                };
                (hashkey.to_owned(), b)
            })
            .collect();

        Ok(res)
    }

    fn write_stream_from_loose(&self, hash: &str, py_filelike: Py<PyAny>) -> PyResult<()> {
        Stream::write_from_loose(&self.inner, hash, py_filelike)
    }

    fn write_stream_from_packs(&self, hash: &str, py_filelike: Py<PyAny>) -> PyResult<()> {
        Stream::write_from_packs(&self.inner, hash, py_filelike)
    }


    // XXX: combine with get_n_objs and return dicts
    fn get_total_size(&self) -> PyResult<u64> {
        let info = rsdos::cli::stat(&self.inner)?;
        Ok(info.size.loose)
    }

    fn get_n_objs(&self) -> PyResult<u64> {
        let info = rsdos::cli::stat(&self.inner)?;
        Ok(info.count.loose)
    }
}

#[derive(Debug)]
#[pyclass]
struct Stream {
    fl: PyFileLikeObject,
}

impl Stream {
    fn write_from_loose(cnt: &Container, hash: &str, py_filelike: Py<PyAny>) -> PyResult<()> {
        if let Some(obj) = rsdos::io_loose::extract(hash, cnt)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?
        {
            match PyFileLikeObject::with_requirements(py_filelike, true, false, false, false) {
                Ok(mut fl) => {
                    // copy from reader to writer
                    let mut rdr = obj
                        .make_reader()
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
                    std::io::copy(&mut rdr, &mut fl)?;
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
        if let Some(obj) = rsdos::io_packs::extract(hash, cnt)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?
        {
            match PyFileLikeObject::with_requirements(py_filelike, true, false, false, false) {
                Ok(mut fl) => {
                    // copy from reader to writer
                    let mut rdr = obj
                        .make_reader()
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
                    std::io::copy(&mut rdr, &mut fl)?;
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

impl ReaderMaker for Stream {
    fn make_reader(&self) -> Result<impl Read, rsdos::Error> {
        Ok(self.fl.clone())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "rsdos")]
fn pyrsdos(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContainer>()?;
    Ok(())
}
