FROM ubuntu:18.04

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.56.1

VOLUME ["/root/.cargo"]

RUN apt-get update && apt-get install -y \
    wget \
# GCC and friends
    build-essential \
# OpenSSL development files
    libssl-dev \
# For finding the OpenSSL development files
    pkg-config \
# For publishing resulting packages
    openssh-client

RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    url="https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init"; \
    wget "$url"; \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --default-toolchain $RUST_VERSION; \
    rm rustup-init; \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
    cargo install cargo-deb; \
    rustup --version; \
    cargo --version; \
    rustc --version;
