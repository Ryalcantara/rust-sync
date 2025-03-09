# RustSync 🔄

**SQL Server to MySQL Database Synchronization Tool**

## Introduction 📚

RustSync is a database synchronization utility developed in Rust that facilitates data transfer between Microsoft SQL Server and MySQL databases. This tool is specifically designed to connect a local SQL Server instance with an online MySQL database hosted online. (⌐■_■)

## Key Features ✨

- 🚀 High-performance data synchronization utilizing Rust's efficiency
- 🔐 Secure credential management via environment variables
- 🪟 Integrated Windows Authentication support for SQL Server
- ⚠️ Comprehensive error handling with the anyhow crate
- ⚡ Asynchronous I/O operations powered by Tokio

## Technical Overview 🔍

This application establishes connections to both SQL Server and MySQL databases concurrently, allowing for efficient data transfer between the two systems. It leverages Rust's strong type system and memory safety guarantees to ensure reliable operation. ᕦ(ò_óˇ)ᕤ

## Installation and Configuration 🛠️

1. Clone the repository to your local environment
2. Create a configuration file (.env) with the necessary database connection parameters
3. Execute the application using `cargo run`

## Future Development Plans 🔮

- 🔄 Implementation of comprehensive data synchronization algorithms
- 📦 Addition of batch processing capabilities for large datasets
- 🛡️ Enhancement of error recovery mechanisms
- 📝 Integration of structured logging functionality
- ⏱️ Development of scheduled synchronization features

---

A personal project developed to facilitate efficient database synchronization between disparate database management systems. (￣▽￣)ノ