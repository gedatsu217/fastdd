## Overview

fastdd is a command-line utility, similar to `dd`, for high-speed file and device copying, built on `io_uring`.

## Installation

This tool depends on the `io_uring` feature of the Linux kernel. 

1.  Clone the source code:
    ```bash
    git clone git@github.com:gedatsu217/fastdd.git
    cd fastdd
    ```

2.  Build the project using the Rust toolchain:
    ```bash
    cargo build --release
    ```

3.  The executable will be available at `target/release/fastdd`.

## Usage
```
fastdd --if [input_file] --of [output_file]
```
### Options
```
--bs: Block size.
--count: Number of blocks to copy. If not specified, the entire file will be copied.
--is: Input file seek offset in blocks.
--os: Output file seek offset in blocks.
--ring_size: Size of the io_uring ring. If not specified, a default size will be used.
--num_buffers: Number of buffers to use. If not specified, a default number will be used.
--progress: Display periodic progress updates.
```