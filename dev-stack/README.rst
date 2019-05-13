Cortex Development Stack
========================

This repository contains supporting development configuration for the code in the Cortex repository:

https://gitlab.hendrikx-itc.nl/hitc/cortex-dispatcher

Start Supporting Services
-------------------------

We use Docker containers to run the supporting services for Cortex. Start the
services in the foreground using docker-compose::

   $ docker-compose up


Loading Database Schema
-----------------------

For now, the PostgreSQL database schema for Cortex needs to be loaded manually from the SQL files in the Cortex repository. There are currently 2 parts: One for the Cortex Dispatcher and one for the Sftp Scanner.


Running Cortex Dispatcher
-------------------------

Prerequisites: libssl-dev::

   $ sudo apt install libssl-dev

Build and start a local development version with debug logging::

   $ RUST_LOG=cortex_dispatcher=debug cargo run --bin cortex-dispatcher -- --config cortex-dispatcher.yml


Running Cortex Sftp Scanner
---------------------------

Build and start a local development version with debug logging::

   $ RUST_LOG=cortex_sftp_scanner=debug cargo run --bin cortex-sftp-scanner -- --config cortex-sftp-scanner.yml
