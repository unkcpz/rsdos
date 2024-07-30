# rsdos

(R)u(s)ty implementation of [`(d)isk-(o)bject(s)tore`](https://github.com/aiidateam/disk-objectstore).

## Progress

- [x] Init command
- [x] Status command and test on large dos
- [x] AddFiles and then can start prepare tests cases
- [x] Prepare test using stream to loose and test on init/status/add-files
- [x] Read APIs: has_objects, get_object_hash, get_folder, get_object_stream, get_objects_stream_and_meta, list_all_objects
- [x] Container as an struct
- [x] pyo3 bindings and get object iter bind
- [x] 1st benchmark with python dos on loose read and write behaviors
- [x] Pack write
- [ ] Pack read
- [ ] Solve HashWriter overhead by using reference instead of mem alloc for each entry visiting
- [ ] loose -> Pack
- [ ] 2nd benchmark on loose-P pack and Pack read/write
- [ ] benchmark on loose -> Pack without compress
- [ ] compression
- [ ] benchmark on pack with compress
- [ ] optimize
- [ ] validate
- [ ] backup
- [ ] benchmark on optimize/validate/backup ...
- [ ] own rust benchmark on detail performance tuning.

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

## Performance note

- When add duplicate file, if add a file that has same content, will skip the move operation. 

### Time scales to be noticed (2009)
https://surana.wordpress.com/2009/01/01/numbers-everyone-should-know/

- L1 cache reference 0.5 ns
- Branch mispredict 5 ns
- L2 cache reference 7 ns
- Mutex lock/unlock 100 ns
- Main memory reference 100 ns
- Compress 1K bytes with Zippy 10,000 ns
- Send 2K bytes over 1 Gbps network 20,000 ns
- Read 1 MB sequentially from memory 250,000 ns
- Round trip within same datacenter 500,000 ns
- Disk seek 10,000,000 ns
- Read 1 MB sequentially from network 10,000,000 ns
- Read 1 MB sequentially from disk 30,000,000 ns
- Send packet CA->Netherlands->CA 150,000,000 ns

### Improvement

- close db session is managed by the scope. in legacy dos need to close with container manually.
- how git manage repack: https://github.blog/open-source/git/scaling-monorepo-maintenance/#geometric-repacking
- gixoxide: https://github.com/Byron/gitoxide/blob/1a979221793a63cfc092e7e0c64854f8182cfaf0/etc/discovery/odb.md?plain=1#L172 

### Why legacy-dos is slow

#### Python 

....

#### More

- When read from hashkey, store the handler and meta which require allocation, and increase the cache miss.

## API discrepancy with legacy dos

- useless `close()`, since rust manage drop after outof scope
- some exceptions are quite redundent such as `FileeNotFoundError`, and `NotInitialized` Error which are take care by the anyhow to propogate up already.
- getter for `loose_prefix_len` and `pack_size_targer` is passing through `Config` in rsdos, no willing to support expose API to container.
