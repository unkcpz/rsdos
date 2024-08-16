# rsdos

An efficient (r)u(s)ty  [`(d)isk-(o)bject(s)tore`](https://github.com/aiidateam/disk-objectstore).

## Installation

- [ ] cargo binstall
- [ ] cargo instatll
- [ ] curl
- [ ] python library and bin
- [ ] apt/pacman/brew

## Usage

TODO:

## Design

### APIs

#### Rust

- `insert` and `insert_many` for insert single or many objects to container.
- `extract` and `extract_many` for extract single or many objects from container.
- `insert_many` and `extract_many` should both using iterator as input/output since the number of objects can be huge. Meanwhile using iterator helps with buffer management on rw large amount of files.
- For the container, it should implement `insert`, `extract`, `insert_many` and `extract_many`. That requires loose has `insert_many` implemented from `insert` and be the method for the container.
- Since insert/extract can interact with either loose or packed store, I use enum-based strategy.
- naming convention are `loose` and `packed`. To compatible with legacy dos, if legacy container exist, `packs` is also valid.
- `pack` is the operation to move objects from loose to packed store. It calling `insert_many` to packed store since no overhead on DB openning/closing. 
- `repack` is on packed store and do the `pack` again using `sandbox` folder.
- Besides the `pack` and `repack` cases above, `insert_many` to packed store should not exposed to normal user. 
- To make `Container` a generic type, things that implement `insert`, `extract`, `insert_many` and `extract_many` should be a Container no matter it is local or not. 
- hashkey servers two purpose: 1. as the id of the object stored, this need to use sha256 to avoid duplicate 2. as the checksum to see if the lazy object read is valid, for this purpose can use cheap checksum.
- For the Packed objects, `raw_size` is the uncompressed size while `size` is the compressed size occupied the packed file, this different from legacy dos which `length` is the compressed size occupied in packed file.
- Only support one compression library, for V1 that is zlib, for V2 use zstd.
- [when it is worth to compress?](https://developer.att.com/video-optimizer/docs/best-practices/text-file-compression)

#### Py wrapper

I think open container as context manager is a bad idea in legacy dos. Because the drop part is calling db.close() which only required for packs rw. 
In principle the context manager should always used since otherwise DB is not gracefully tear down and cause memory leak. 
Rust will take care of drop in scope so I do not need to put any drop codes for container in py wrapper.
After initialize the container, the object is straightforward to use and every IO has its own connection to DB.
Since I am using `sled` as embeded DB, it is even safe to be used in a non-blocking condition (not tested but in principle if we trust `sled`). 

When interact with container, client side (user) have no knowledge on where the objects are stored it can be in loose or packed. 
Therefore, when calling `insert` or `insert_many` from python wrapper it always goes to loose. 
When calling `extract` or `extract_many` it will check loose first and then packed store to get the object(s). 
The `pack` operation will trigger the move from loose to packed store and result into the objects are distrubuted in two places.

### Estimate whether to compress
- [when it is worth to compress?](https://developer.att.com/video-optimizer/docs/best-practices/text-file-compression)
- see [bet on compression](https://github.com/facebook/zstd/issues/3793#issuecomment-1765095341)
- see how [btrfs use pre-compression-heuristics](https://btrfs.readthedocs.io/en/latest/Compression.html#pre-compression-heuristics)

In the reader maker, I put a method to the trait named `worth_compress` that return (`SmallContent`, `MaybeBinary`, `ZFile([u8; 4])`, `MaybeLargeText`).
By default it return `MaybeLargeText` so should be compressed by default if compression turned on. 
I use the metric metioned in the "att" article above to decide whether I'll compress it or not.

Here is the decision making flow:
- If something wrong when parsing the maybe format, just regard it "worth to compress" (e.g. `MaybeLargeText`).
- If it is a file (`SmallContent`) < 850 bytes don't compress. (file metadata)
- Read 2 header bytes if it is a zilb or a zstd(which is 4 bytes in header) (`ZFile([u8; 4])`), don't compress. (this will be override if recompress was on and different compression algorithm is assigned.)
- Read 512 bytes and check if it is a binary (`MaybeBinary`) (by checking null bytes which is a heuristic for it is a binary data) 
- none of above is true, regard it as "worth to compress!" (`MabyLargeText`)

This avoid to run actuall compress which bring overhead.

### Migration

- The loose is the same, `packs` need to rename to `packed`.
- Using zstd therefore read as zlib write as zstd.
- The `config.json` will contain more information so use default to fill the missing field.
- The packed DB is the most important thing to migrate, all elements are read out and injected into the new embeded DB backend. (need to be careful about `size`, `raw_size` definition w.r.t to the legacy dos)
- To do the migration, function as CLI command is provided. I also need to provide python wrapper so it can call from AiiDA.

### `io_uring`

TODO:

### File operation timeout

**Deprecated design decision**: see `io_uring`

Since I/O operiation is synchroues, operiations on large files will block thread. 
No matter whether I use multithread (through `tokio::task::spawn_blocking` which is issuing a blocking call in general), I put a timeout to close the handler.

- Default timeout: `10s`.
- Provide API to pass the timeout and can pass the default value as global variable.

### Blocking IO

**Deprecated design decision**: see `io_uring`

There is `tokio/fs` [1] that slap an asynchronous IO but Linux doesn't have non-blocking file I/O so it is blocking a thread somewhere anyway.
Tokio will use ordinary blocking file operations behind the scenes by using `spawn_blocking` treadpool to run in background.

~~Thus comes to the design, using one thread as default and spawning thread only when the global set for the async is turned on.~~
~~Because of that, it is also consider to add timeout to the file operations.~~

I am considering using `io-uring` so should not having blocking IO in the end.

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



## Performance notes

- When add duplicate file, if add a file that has same content, will skip the move operation. 
- `zstd` is faster than zlib: https://github.com/facebook/zstd?tab=readme-ov-file#benchmarks

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

- useless `close()`, since rust manage drop after out of scope
- some exceptions are quite redundent such as `FileNotFoundError`, and `NotInitialized` Error which are take care by the anyhow to propogate up already.
- getter for `loose_prefix_len` and `pack_size_target` is passing through `Config` in rsdos, no willing to support expose API to container.

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
- [x] loose -> Pack
- [x] benchmark on loose -> Pack without compress (more than 3x times faster)
- [x] API redesign to make it ergonamic and idiomatic Rust [#7](https://github.com/unkcpz/rsdos/pull/7)
- [x] compression (zlib) [#8](https://github.com/unkcpz/rsdos/pull/8)
- [x] benchmark on pack with compress [#9](https://github.com/unkcpz/rsdos/pull/9)
- [x] estimate on the input stream format and decide whether pack. [#9](https://github.com/unkcpz/rsdos/pull/9)
- [ ] (v2) Use `sled` as k-v DB backend which should have better performance than sqlite [#1](https://github.com/unkcpz/rsdos/pull/1) 
- [ ] (v2) `io_uring`
- [ ] (v2) switch to using zstd instead of zlib
- [ ] Memory footprint tracking when packing, since rsdos use iterator it should be memory efficient.
- [ ] Dependency injection mode to attach progress bar to long run functions (py exposed interface as well)
- [ ] docs as library
- [ ] repack
- [ ] optimize
- [ ] validate
- [ ] backup
- [ ] benchmark on optimize/validate/backup ...
- [ ] own rust benchmark on detail performance tuning.
- [ ] (v2) Compress on adding to loose as git not just during packs. Header definition required.
- [ ] (v2) hide direct write to packs and shading with the same loose structure
- [ ] generic Container interface that can extent to host data in online storage (trait Container with insert/extract methods)
- [ ] Add mutex to the pack write, panic when other thread is writing. (or io_uring take care of async?)
- [ ] (v2) Rename packs -> packed
- [ ] migation plan and CLI tool
- [ ] Explicit using buffer reader/writer to replace copy_by_chunk, need to symmetry use buf on reader and write for insert/extract. I need to decide in which timing to wrap reader as a BufReader, in `ReaderMaker` or in copy???
- [ ] (v3) integrate with OpenDAL for unified interface
