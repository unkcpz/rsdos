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
    #[error("Refusing to initialize in non-empty directory as '{}' in folder", .path.display())]
    DirectoryNotEmpty { path: PathBuf },
    #[error("Could not obtain the container directory at {}", .path.display())]
    UnableObtainDir { path: PathBuf },
    #[error("Uninitialized container directory at {}", .path.display())]
    Uninitialized { path: PathBuf },
    #[error("Could not read the container config file at {}: {}", .path.display(), .source)]
    ConfigFileError {
        source: std::io::Error,
        path: PathBuf,
    },
    #[error("Could not reach {}: {cause}", .path.display())]
    StoreComponentError { path: PathBuf, cause: String },
    #[error("Could not parst {} to compression algorithm", .s)]
    ParseCompressionError { s: String },

    // io module errors
    #[error("Unexpected size in copy: expect {} got {}", .expected, .got)]
    UnexpectedCopySize { expected: u64, got: u64 },
    #[error("Unable to copy by chunk")]
    ChunkCopyError { source: std::io::Error },
    #[error("Unable to parse pack file name {}", .n)]
    ParsePackFilenameError { source: std::num::ParseIntError, n: String},
    #[error("Unexpected checksum, expected: '{}' got: '{}'", .expected, .got)]
    IntegrityError { expected: String, got: String},

    // db module erors
    #[error("sled error")]
    SledError(#[from] sled::Error),
    #[error("Could not select from DB")]
    SledSelectError { source: sled::Error },
    #[error("Could not insert to DB")]
    SledInsertError { source: sled::Error },
}
