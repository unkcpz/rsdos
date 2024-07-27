use std::{collections::HashMap, fs, io::Cursor, path::PathBuf};

use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};
use pyo3_file::PyFileLikeObject;
use rsdos::{add_file::stream_to_loose, status, Config, Container, Object};
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

    #[pyo3(signature = (pack_size_target=4))]
    fn init_container(&self, pack_size_target: u64) -> PyResult<()> {
        let config = Config::new(pack_size_target);
        Ok(self.inner.initialize(&config)?)
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

    fn get_object_content(&self, hashkey: &str) -> PyResult<Vec<u8>> {
        match Object::from_hash(hashkey, &self.inner)? {
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
        let d = hashkeys
            .iter()
            .map(|hashkey| {
                // let content = self.get_object_content(k).unwrap();
                let content = match Object::from_hash(hashkey, &self.inner).unwrap() {
                    Some(mut obj) => {
                        let mut buf = Vec::new();
                        let mut cursor = Cursor::new(&mut buf);

                        std::io::copy(&mut obj.reader, &mut cursor).unwrap();
                        buf
                    }
                    _ => todo!(),
                };
                (hashkey.to_string(), content)
            })
            .collect();

        // println!("{map:?}", map);
        Ok(d)
    }

    // TODO: try here return an iterator, use it in get_objects_content, see if it is getting fast

    // XXX: return an Object struct???
    fn stream_from_loose(&self, py: Python, obj_hash: &str) -> PyResult<Py<PyStreamObject>> {
        let obj_path = self
            .inner
            .loose()?
            .join(format!("{}/{}", &obj_hash[..2], &obj_hash[2..]));
        if obj_path.exists() {
            let file_like = PyStreamObject::new(obj_path.to_str().unwrap().to_string())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;

            Ok(Py::new(py, file_like)?)
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(
                "object not exist".to_string(),
            ))
        }
    }

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
struct PyStreamObject {
    inner: fs::File,
    size: u64,
}

#[pymethods]
impl PyStreamObject {
    #[new]
    fn new(filename: String) -> PyResult<Self> {
        let file = fs::File::open(filename).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to open file: {}", e))
        })?;
        let size = file.metadata()?.len();
        Ok(PyStreamObject { inner: file, size })
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
