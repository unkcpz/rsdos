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
- [x] Pack read
- [x] Solve HashWriter overhead by using reference instead of mem alloc for each entry visiting
- [x] benchmark on packs read/write
- [x] profiling on packs read (db?, io?) see flamegraph of `batch_packs_read.rs` and it shows db is the bottleneck.
- [x] packs correctly on adding new packs file
- [ ] test on pack id searching
- [ ] loose -> Pack
- [ ] 2nd benchmark on loose->pack
- [ ] benchmark on loose -> Pack without compress
- [ ] compression
- [ ] benchmark on pack with compress
- [ ] optimize
- [ ] validate
- [ ] backup
- [ ] benchmark on optimize/validate/backup ...
- [ ] own rust benchmark on detail performance tuning.
- [ ] hide direct write to packs and shading with same loose structure
- [ ] Use `sled` as k-v DB backend which should have better performance than sqlite.
- [ ] `io_uring`

## Design

### File operation timeout

Since I/O operiation is synchroues, operiations on large files will block thread. 
No matter whether I use multithread (through `tokio::task::spawn_blocking` which is issuing a blocking call in general), I put a timeout to close the handler.

- Default timeout: `10s`.
- Provide API to pass the timeout and can pass the default value as global variable.

### Blocking IO

There is `tokio/fs` [1] that slap an asynchronous IO but Linux doesn't have non-blocking file I/O so it is blocking a thread somewhere anyway.
Tokio will use ordinary blocking file operations behind the scenes by using `spawn_blocking` treadpool to run in background.

Thus comes to the design, using one thread as default and spawning thread only when the global set for the async is turned on.
Because of that, it is also consider to add timeout to the file operations. 

[1] https://docs.rs/tokio/latest/tokio/fs/index.html  

### PyO3 at boundary

When wrapping rust interface to python lib, the interfaces require explicit types, which means the traits can not be used in the types that mapping at the boundary.
These are the `streams` that need to pass into and fetched from container defined in the rust implementation.
The stream is a file-like instance that can either can `read` when it used in the flow that goes into the container, or it can `write` when it used in the flow that being fetch from the container.
Typically in the python world it is `BinaryIO`, `StringIO` etc.

The write into CNT part is straitforward, since streams can be wrapped as `PyFileLikeObject` (a `PyObject` provided by `pyo3-file`) with read permission.
It then becomes rust `Reader`. 
As for the read from CNT part, the rust implementation use object hash to lookup in the container and return an `Object<R>` which can be from loose or packs storages. 
However, the `Object<R>` is a generic type defined with any type of reader. 
If I want to directly return this object to the python world, I need to make it an explicit type and turn out to be very complex in the type: `Object<Take<BufReader<File>>>` as the time I writing this note.
To simplify it, I use `PyFileLikeObject` with write to map any file-like instance from python world to a `Writer`.
This design at the same time makes the boundary looks symmetry in turns of read and write operations.


## Installation

- [ ] cargo binstall
- [ ] cargo instatll
- [ ] curl
- [ ] python library and bin
- [ ] apt/pacman/brew

## Performance notes

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

### Improvement ideas

- close db session is managed by the scope. in legacy dos need to close with container manually.
- how git manage repack: https://github.blog/open-source/git/scaling-monorepo-maintenance/#geometric-repacking
- gixoxide: https://github.com/Byron/gitoxide/blob/1a979221793a63cfc092e7e0c64854f8182cfaf0/etc/discovery/odb.md?plain=1#L172 
- using `io_uring` for heavy io in an system async way.

### Why legacy-dos is slow

#### Python 

....

#### More

- When read from hashkey, store the handler and meta which require allocation, and increase the cache miss.

## API discrepancy with legacy dos

- useless `close()`, since rust manage drop after outof scope
- some exceptions are quite redundent such as `FileeNotFoundError`, and `NotInitialized` Error which are take care by the anyhow to propogate up already.
- getter for `loose_prefix_len` and `pack_size_targer` is passing through `Config` in rsdos, no willing to support expose API to container.
