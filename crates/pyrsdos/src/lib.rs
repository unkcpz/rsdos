use std::{collections::HashMap, io::Cursor, path::PathBuf};

use pyo3::{exceptions::PyValueError, prelude::*};
use pyo3_file::PyFileLikeObject;
use rsdos::{add_file::stream_to_loose, Config, Container, Object};

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

    // fn add_object(&self, content: &[u8]) -> PyResult<String> {
    //     let mut reader = Cursor::new(content);
    //     let (_, hash_hex) = stream_to_loose(&mut reader, &self.inner)?;
    //     Ok(hash_hex)
    // }

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

    fn stream_to_loose(&self, stream: Py<PyAny>) -> PyResult<(u64, String)> {
        let mut file_like = PyFileLikeObject::with_requirements(stream, true, false, false, false)?;

        stream_to_loose(&mut file_like, &self.inner)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "rsdos")]
fn pyrsdos(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContainer>()?;
    Ok(())
}
