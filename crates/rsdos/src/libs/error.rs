use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum Error {
    #[error("std::io error")]
    StdIO(#[from] std::io::Error),
    #[error("Could not open data at '{}'", .path.display())]
    IoOpen {
        source: std::io::Error,
        path: PathBuf,
    },
    #[error("Could not write data at '{}'", .path.display())]
    IoWrite {
        source: std::io::Error,
        path: PathBuf,
    },
    #[error("Could not create directory at '{}'", .path.display())]
    CreateDirectory {
        source: std::io::Error,
        path: PathBuf,
    },

    // Container erors
    #[error("Refusing to initialize in non-empty directory as '{}'", .path.display())]
    DirectoryNotEmpty { path: PathBuf },
    #[error("Could not obtain the container directory at {}", .path.display())]
    UnableObtainDir { path: PathBuf },
    #[error("Uninitialized container directory at {}", .path.display())]
    Uninitialized { path: PathBuf },
    #[error("Could not read the container config file at {}", .path.display())]
    ConfigFileRead {
        source: std::io::Error,
        path: PathBuf,
    },
    #[error("Could not reach {}: {cause}", .path.display())]
    StoreComponentError { path: PathBuf, cause: String },

    // io modele errors
    #[error("Unexpected size in copy: expect {} got {}", .expected, .got)]
    UnexpectedCopySize { expected: u64, got: u64 },
    #[error("Unable to copy by chunk")]
    ChunkCopyError { source: std::io::Error },
}
