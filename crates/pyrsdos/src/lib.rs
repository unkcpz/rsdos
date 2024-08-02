use std::{
    collections::HashMap,
    io::{Cursor, Seek},
    path::PathBuf,
};

use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};
use pyo3_file::PyFileLikeObject;
use rsdos::{
    add_file::{stream_to_loose, stream_to_packs, StoreType},
    object::stream_from_packs_multi,
    status,
    Config, Container, Object,
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
    fn is_initialised(&self) -> bool {
        self.inner.validate().is_ok()
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

    fn stream_to_packs_multi(
        &self,
        py: Python,
        sources: Vec<Py<PyBytes>>,
    ) -> PyResult<Vec<String>> {
        let mut ll = Vec::with_capacity(sources.len());
        for source in sources {
            let b = source.bind(py);
            let cursor = Cursor::new(b.as_bytes().to_vec());
            ll.push(cursor);
        }

        let mut_refs: Vec<&mut Cursor<Vec<u8>>> = ll.iter_mut().collect();
         
        let results = rsdos::add_file::stream_to_packs_multi(mut_refs, &self.inner)?;
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
