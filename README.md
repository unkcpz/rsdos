# RSDOS

[![rust-test](https://img.shields.io/github/actions/workflow/status/unkcpz/rsdos/ci-rust.yml?label=rust-test)](https://github.com/unkcpz/rsdos/actions/workflows/ci-rust.yml)
[![python-test](https://img.shields.io/github/actions/workflow/status/unkcpz/rsdos/ci-python.yml?label=python-test)](https://github.com/unkcpz/rsdos/actions/workflows/ci-python.yml)

RSDOS - ([R]u[S]ty [D]isk-[O]bject[S]tore), is a **fast**, **server-less**, **rust-native** disk object store for dataset management.

It handles huge datasets without breaking a sweat—whether if you’re juggling thousands of tiny files or streaming multi-gigabyte blobs. 
It’s **not** designed as a backup solution, but rather for storing millions of files in a compact and manageable way.

It packs data intelligently to maximize disk usage, deduplicates content via SHA-256 hashing.
The tool appling on-the-fly compression (`zstd` as default or `zlib`) whenever it’s beneficial—no manual tuning required. 
I keep I/O straightforward with streaming-based insert and extract methods so you don’t flood your RAM when dealing with large files. 

Thanks to Rust’s memory safety guarantees, RSDOS delivers great performance without the usual headaches or subtle bugs.
If you’re integrating with Python, that’s covered through pyo3 bindings.

More design details can be found at [**design notes**](https://github.com/unkcpz/rsdos/blob/main/Design.md)

## Installation

You can install **RSDOS** using various methods. Pick whichever approach suits your workflow or distribution:

### Cargo install

To build from source (requires Rust and Cargo):

```bash
cargo install rsdos
```

This compiles RSDOS locally and places the `rsdos` binary in your Cargo bin directory (often `~/.cargo/bin`).

### Curl (Manual Download)

For systems without Rust installed, or if you prefer manual downloads:

1. Visit the [Releases page](https://github.com/unkcpz/rsdos/releases) to find a precompiled binary for your system.
2. Download via `curl`, for example:
   ```bash
   curl -LO https://github.com/unkcpz/rsdos/releases/download/vX.Y.Z/rsdos-x86_64-unknown-linux-musl.tar.gz
   ```
3. Unpack and move the binary into your PATH:
   ```bash
   tar xvf rsdos-x86_64-unknown-linux-musl.tar.gz
   sudo mv rsdos /usr/local/bin/
   ```
4. Test the installation:
   ```bash
   rsdos --help
   ```

### Python Library (PyPI)

If you need the Python API or want to use RSDOS via Python scripts or Jupyter notebooks, you can install the Python wrapper:

```bash
pip install rsdos
```

(This also provides an `rsdos` CLI command if the package is set up accordingly.)

<!-- ### System Package (Apt / Pacman / Brew) -->
<!---->
<!-- *(Planned; not yet available.)* -->
<!---->
<!-- - **Debian/Ubuntu (apt)**   -->
<!--   ```bash -->
<!--   sudo apt-get update -->
<!--   sudo apt-get install rsdos -->
<!--   ``` -->
<!---->
<!-- - **Arch Linux (pacman)**   -->
<!--   ```bash -->
<!--   sudo yay -S rsdos -->
<!--   ``` -->
<!---->
<!-- - **macOS (Homebrew)**   -->
<!--   ```bash -->
<!--   brew update -->
<!--   brew install rsdos -->
<!--   ``` -->

### Minimum Supported Rust Version 

- MSRV: **1.78**

## Usage

Once installed, confirm everything is working by running:

```bash
rsdos --version
```

### CLI tool

Manage your large file datasets through CLI:

- Initialize a new container in the current directory

```bash
rsdos init --pack-size=512 --compression=zstd
# [info] Container initialized at ./container
```

- Add files as loose objects

```bash
rsdos add-files --to loose ./mydata1.txt ./mydata2.bin
# abc123... - mydata1.txt: 1.2 MB
# def456... - mydata2.bin: 3.4 MB
```

- Pack all loose objects for efficient storage

```bash
rsdos optimize pack
# [info] Packed 2 loose objects into pack file #1
```

- Display container status

```bash
rsdos status
# [container]
# Location = ./container
# Id = 0123456789abcdef
# ZipAlgo = zstd
#
# [container.count]
# Loose = 0
# Packs = 1
# Pack Files = 1
#
# [container.size]
# Loose = 0 B
# Packs = 4.6 MB
# Packs Files = 4.6 MB
```

### Python binding

Here’s a quick-start guide for the Python API, showcasing core operations:

```python
from rsdos import Container, CompressMode

# 1. Create a new container (or open an existing one) at a specified path:
cnt = Container("/path/to/container")

# 2. Initialize the container with desired settings
cnt.init_container(
    clear=False,
    pack_size_target=4 * 1024 * 1024 * 1024,  # 4 GB pack size target
    loose_prefix_len=2,
    hash_type="sha256",
    compression_algorithm="zlib+1",  # zlib with level +1
)

# 3. Add objects in loose storage
num_files = 10
content_list = [b"ExampleData" + str(i).encode("utf-8") for i in range(num_files)]
hashkeys = []
for content in content_list:
    hkey = cnt.add_object(content)
    hashkeys.append(hkey)

# 4. Pack all loose objects for optimal storage
cnt.pack_all_loose(CompressMode.YES)

# 5. Retrieve the content of the first file
retrieved_data = cnt.get_object_content(hashkeys[0])
print("Retrieved:", retrieved_data)
```

#### Additional Tips

- Heuristics: RSDOS automatically decides whether to compress data based on size and content type (e.g., text vs. binary). You can override this with the compress parameter.
- Large Repositories: For very large sets of files, consider batch insertion (add_objects_to_pack) and periodic calls to pack_all_loose for best performance.
- Streaming Approach: When handling files that exceed available memory, always use the streaming methods (add_streamed_object, get_object_stream).

Batch Insertion

```python
files_data = [b"file1", b"file2", b"file3"]
hashkeys = cnt.add_objects_to_pack(
    content_list=files_data,
    compress=True
)
print("Inserted files:", hashkeys)
```

Streaming to and from Files

```python
import io

# Write from a file
with open("large_file.bin", "rb") as infile:
    stream_hash = cnt.add_streamed_object(infile)
    print("Stored large file, hash:", stream_hash)

# Read back into a file-like object
with cnt.get_object_stream(stream_hash) as instream:
    if instream:
        with open("restored_file.bin", "wb") as outfile:
            outfile.write(instream.read())
    else:
        print("Object not found in container.")
```

## Disclaimer

- `RSDOS` is heavily inspired by aiidateam/disk-objectstore, this reimplementation aims to explore alternative design and performance optimizations.

## Progress

- [x] **Init command**  
- [x] **Status command** (tested on large disk-objectstore)  
- [x] **Add files** (insert objects to loose storage)  
- [x] **Stream-based reading** (has_objects, get_object_hash, list_all_objects, etc.)  
- [x] **Container struct**  
- [x] **PyO3 bindings**  
- [x] **Benchmarking** (loose read/write, packed read/write)  
- [x] **Pack** (write)  
- [x] **Repack** (planned after initial design)  
- [x] **Compression** (zlib & zstd)  
- [x] **Heuristics for compression**  
- [ ] **Repack** (finalize vacuuming logic)  
- [ ] **Migration** (tools & Python wrapper for AiiDA)  
- [ ] **Memory footprint tracking**  
- [ ] **Progress bar** for long-running operations  
- [ ] **Documentation** (library docs & examples)  
- [ ] **Validate**, **Optimize**, **Backup**  
- [ ] **Thread safety** (pack write synchronization)  
- [ ] **Use `sled` as a K/V DB** (v2)  
- [ ] **Implement `io_uring`** (v2)  
- [ ] **Compression at loose stage** (v2)  
- [ ] **Refactor legacy `packs` → `packed`** (v2)  
- [ ] **OpenDAL integration** (v3)  
- [ ] **Generic container interfaces** (v3)

