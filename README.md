# disk-objectstore-rs

[`disk-objectstore`](https://github.com/aiidateam/disk-objectstore) reimplemented in Rust.

## Progress

- [x] Init command
- [x] Status command and test on large dos
- [ ] AddFiles and then can start prepare tests cases
- [ ] pyo3 bindings and get object iter bind
- [ ] 1st benchmark with python dos on read behaviors
- [ ] optimize
- [ ] validate
- [ ] backup
- [ ] 2nd benchmark on optimize/validate/backup ...

## Design

### File operation timeout

Since I/O operiation is synchroues, operiations on large files will block thread. 
No matter whether we use multithread (through `tokio::task::spawn_blocking` which is issuing a blocking call in general), we put a timeout to close the handler.

- Default timeout: `10s`.
- Provide API to pass the timeout and can pass the default value as global variable.

### Blocking IO

There is `tokio/fs` [1] that slap an asynchronous IO but Linux doesn't have non-blocking file I/O so it is blocking a thread somewhere anyway.
Tokio will use ordinary blocking file operations behind the scenes by using `spawn_blocking` treadpool to run in background.

Thus comes to the design, using one thread as default and spawning thread only when the global set for the async is turned on.
Because of that, it is also consider to add timeout to the file operations. 

[1] https://docs.rs/tokio/latest/tokio/fs/index.html  

## Installation

- [ ] cargo binstall
- [ ] cargo instatll
- [ ] curl
- [ ] python library and bin
- [ ] apt/pacman/brew
