# RSDOS

An efficient, **(R)u(S)ty** [**(D)isk-(O)bject(S)tore**](https://github.com/aiidateam/disk-objectstore).


## Installation

Planned installation methods include:

- [ ] **cargo binstall**
- [ ] **cargo install**
- [ ] **curl**
- [ ] **Python library** (providing both Python API and CLI)
- [ ] **Apt / Pacman / Brew** (system packages)

<!-- --- -->
<!---->
<!-- ## Installation -->
<!---->
<!-- You can install **RSDOS** using various methods. Pick whichever approach suits your workflow or distribution: -->
<!---->
<!-- ### 1. Cargo binstall -->
<!---->
<!-- If you have [cargo-binstall](https://github.com/cargo-bins/cargo-binstall) installed: -->
<!-- ```bash -->
<!-- cargo binstall rsdos -->
<!-- ``` -->
<!-- This automatically fetches and installs prebuilt binaries for your platform (if available). -->
<!---->
<!-- ### 2. Cargo install -->
<!---->
<!-- To build from source (requires Rust and Cargo): -->
<!-- ```bash -->
<!-- cargo install rsdos -->
<!-- ``` -->
<!-- This compiles RSDOS locally and places the `rsdos` binary in your Cargo bin directory (often `~/.cargo/bin`). -->
<!---->
<!-- ### 3. Curl (Manual Download) -->
<!---->
<!-- For systems without Rust installed, or if you prefer manual downloads: -->
<!---->
<!-- 1. Visit the [Releases page](https://github.com/unkcpz/rsdos/releases) to find a precompiled binary for your system. -->
<!-- 2. Download via `curl`, for example: -->
<!--    ```bash -->
<!--    curl -LO https://github.com/unkcpz/rsdos/releases/download/vX.Y.Z/rsdos-x86_64-unknown-linux-musl.tar.gz -->
<!--    ``` -->
<!-- 3. Unpack and move the binary into your PATH: -->
<!--    ```bash -->
<!--    tar xvf rsdos-x86_64-unknown-linux-musl.tar.gz -->
<!--    sudo mv rsdos /usr/local/bin/ -->
<!--    ``` -->
<!-- 4. Test the installation: -->
<!--    ```bash -->
<!--    rsdos --help -->
<!--    ``` -->
<!---->
<!-- ### 4. Python Library (PyPI) -->
<!---->
<!-- If you need the Python API or want to use RSDOS via Python scripts or Jupyter notebooks, you can install the Python wrapper: -->
<!---->
<!-- ```bash -->
<!-- pip install rsdos -->
<!-- ``` -->
<!---->
<!-- (This also provides an `rsdos` CLI command if the package is set up accordingly.) -->
<!---->
<!-- ### 5. System Package (Apt / Pacman / Brew) -->
<!---->
<!-- *(Planned; not yet available. Check back for official package links.)* -->
<!---->
<!-- - **Debian/Ubuntu (apt)**   -->
<!--   ```bash -->
<!--   sudo apt-get update -->
<!--   sudo apt-get install rsdos -->
<!--   ``` -->
<!---->
<!-- - **Arch Linux (pacman)**   -->
<!--   ```bash -->
<!--   sudo pacman -S rsdos -->
<!--   ``` -->
<!---->
<!-- - **macOS (Homebrew)**   -->
<!--   ```bash -->
<!--   brew update -->
<!--   brew install rsdos -->
<!--   ``` -->
<!---->
<!-- Once installed, confirm everything is working by running: -->
<!---->
<!-- ```bash -->
<!-- rsdos --version -->
<!-- ``` -->

### MSRV

**Minimum Supported Rust Version (MSRV): 1.78**

## Usage

*TODO: Provide usage examples and CLI commands.*

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

