## Design

This section summarizes the architecture and guiding principles of RSDOS.

### APIs

#### Rust API

1. **Insertion**  
   - `insert` (single stream object)  
   - `insert_many` (multiple stream objects via an iterator)

   Both methods store data into the “container.” Iterators help manage buffers when dealing with a large number of files, reducing memory overhead.

2. **Extraction**  
   - `extract` (single object)  
   - `extract_many` (multiple objects via an iterator)

   Both methods read data from the container, checking loose storage first, then packed storage.

3. **Container Abstraction**  
   - The container should implement `insert`, `insert_many`, `extract`, and `extract_many` regardless of its underlying storage (loose or packed).  
   - Internally, an `enum` strategy distinguishes between loose and packed storage.

4. **Naming and Legacy Compatibility**  
   - “loose” and “packed” are the primary terms; `packs` remains valid for compatibility with legacy disk-objectstore.  

5. **Packing**  
   - `pack` moves objects from loose to packed storage. It uses `insert_many` for efficiency and avoids repeated DB open/close overhead.  
   - `repack` on packed storage re-packs objects (vacuuming old data with incremental pack IDs).

6. **Hash Keys**  
   - Act as both unique IDs (using SHA-256 to avoid duplicates) and checksums to validate object integrity.  
   - A cheaper checksum can also be used to verify data integrity for already-identified objects.

7. **Compression**  
   - Supports both **zlib** and **zstd** (default).  
   - Metadata: `raw_size` is the uncompressed size; `size` is the compressed size in a packed file.

#### Python Wrapper

- The Python API does not expose a context manager for containers because Rust will handle resource cleanup automatically.  
- Each I/O call uses its own connection to the embedded DB (`sled` in v2), allowing safe operations—even in non-blocking contexts (though this is untested).  
- From Python, `insert` and `insert_many` always write to loose storage; `extract` and `extract_many` search both loose and packed.  
- `pack` moves objects from loose to packed, meaning objects might reside in both places afterward.

#### Illustration

Below is a conceptual illustration of how bytes flow across Python and Rust boundaries:

![cross boundaries](./misc/rsdos-design.svg)

### Estimating Whether to Compress

RSDOS uses heuristics to decide if data is worth compressing, following recommendations from:
- [When is it worth compressing?](https://developer.att.com/video-optimizer/docs/best-practices/text-file-compression)  
- [A discussion on compression trade-offs](https://github.com/facebook/zstd/issues/3793#issuecomment-1765095341)
- [Btrfs pre-compression heuristics](https://btrfs.readthedocs.io/en/latest/Compression.html#pre-compression-heuristics)

The rough decision flow is:

1. If a file is very small (e.g., < 850 bytes), do not compress.  
2. If the file already appears to be zlib/zstd-compressed (by reading the header bytes), do not compress (unless forced to recompress).  
3. Check the first 512 bytes. If they contain many null bytes (likely binary), treat them as `MaybeBinary`.  
4. Otherwise, treat them as large text (`MaybeLargeText`) and compress if compression is enabled.

When any parsing or heuristic fails, default to “worth compressing.”

### Migration

1. **Loose Storage** remains the same. A directory named `packs` is also recognized as `packed`.
2. **Compression**:  
   - Legacy reads with zlib, new writes with zstd.  
   - On migration, you can re-insert everything into the new store to convert to zstd if desired.
3. **Config**: `config.json` now includes extra fields; missing items use defaults.
4. **Packed DB**:  
   - Migrating from a legacy store requires reading all objects from the old database, then reinserting them into the new embedded DB.  
   - Carefully handle the difference between `size` (compressed size) vs. `raw_size` (uncompressed size).

A dedicated CLI command will assist with migrations and bridging to Python-based AiiDA tools.

### `io_uring`

*(Planned for v2)*

The goal is to use **io_uring** for non-blocking, efficient I/O on supported Linux kernels, thus removing the need for blocking thread pools.

### File Operation Timeout

**Deprecated (see `io_uring` above)**

Originally, timeouts were planned for large file operations to prevent blocking. With **io_uring**, blocking becomes less of an issue. Hence, the timeout design has been deprecated.

### Blocking I/O

**Deprecated (see `io_uring` above)**

While `tokio/fs` simulates asynchronous file I/O, it internally uses blocking system calls (with a thread pool). The shift to **io_uring** will address true asynchronous file I/O at the system level.

### PyO3 at the Boundary

When exposing Rust implementations to Python via PyO3:

- **Python → Rust (Insertion)**  
  Wrap Python file-like objects (`BinaryIO`, `StringIO`, etc.) in a `PyFileLikeObject` to create a Rust `Reader`.

- **Rust → Python (Extraction)**  
  Reading from RSDOS returns a generic `Object<R>` (loose or packed). For simplicity, it is converted back to a `PyFileLikeObject` for Python.

These conversions ensure a smooth streaming interface on both sides.

## Performance Notes

- **Deduplication**: Files with identical content share a single storage instance (thanks to hash-based IDs).  
- **Compression**: Zstd typically outperforms zlib.  
- **Loose vs. Packed**: Loose is faster for small inserts; packing is more efficient for batch storage.

### Why Legacy-dos Is Slower

- Excessive allocations for metadata on each read.  
- Manual resource management (e.g., container close calls).  
- Less efficient DB or compression approach in some cases.

## API Discrepancies with Legacy

- No explicit `close()` in RSDOS; Rust’s drop behavior handles cleanup automatically.  
- Certain legacy exceptions (`FileNotFoundError`, `NotInitializedError`) are replaced by standard Rust error propagation.  
- Configuration parameters (e.g., `loose_prefix_len`, `pack_size_target`) live in `Config` rather than container methods.
