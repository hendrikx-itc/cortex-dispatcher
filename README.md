# Cortex

[![Crates.io](https://img.shields.io/crates/v/cortex-dispatcher.svg)](https://crates.io/crates/cortex-dispatcher)
[![Crates.io](https://img.shields.io/crates/l/cortex-dispatcher.svg)](https://crates.io/crates/cortex-dispatcher)

Cortex is system for efficiently collecting and distributing files with a
choice of multiple protocols

# Installation

If you're already using Rust, Cortex can be installed with `cargo`:

```
$ cargo install cortex-dispatcher
```

# Development

## Running Cortex Dispatcher

Running a debug build against the Docker based development stack:

```
RUST_BACKTRACE=1 RUST_LOG=cortex_dispatcher=debug,actix_web=debug cargo run --bin cortex-dispatcher -- --config dev-stack/cortex-dispatcher.yml
```

## Running Cortex Sftp Scanner

Running a debug build against the Docker based development stack:

```
RUST_BACKTRACE=1 RUST_LOG=cortex_sftp_scanner=debug,actix_web=debug cargo run --bin cortex-sftp-scanner -- --config dev-stack/cortex-sftp-scanner.yml
```
