# Contributing to Apiary

Thank you for your interest in contributing to Apiary! This guide will help you get started.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How to Contribute](#how-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Features](#suggesting-features)
  - [Submitting Pull Requests](#submitting-pull-requests)
- [Development Setup](#development-setup)
  - [Prerequisites](#prerequisites)
  - [Building](#building)
  - [Running Tests](#running-tests)
- [Code Style & Conventions](#code-style--conventions)
  - [Rust](#rust)
  - [Python](#python)
- [Project Structure](#project-structure)
- [CI Checks](#ci-checks)
- [License](#license)

## Code of Conduct

Please be respectful and constructive in all interactions. We are committed to providing a welcoming and inclusive environment for everyone.

## How to Contribute

### Reporting Bugs

If you find a bug, please open an issue and include:

1. A clear, descriptive title
2. Steps to reproduce the behavior
3. Expected behavior vs. actual behavior
4. Your environment (OS, Rust version, Python version)
5. Any relevant logs or error messages

### Suggesting Features

Feature requests are welcome! Please open an issue and describe:

1. The problem you're trying to solve
2. Your proposed solution
3. Any alternatives you've considered

### Submitting Pull Requests

1. Fork the repository and create your branch from `main`
2. Make your changes following the [code style guidelines](#code-style--conventions)
3. Add or update tests as appropriate
4. Ensure all [CI checks](#ci-checks) pass
5. Open a pull request with a clear description of your changes

## Development Setup

### Prerequisites

- **Rust 1.78+**: Install via [rustup](https://rustup.rs/)
- **Python 3.9+**: Required for the Python SDK
- **maturin**: Install with `pip install maturin`

### Building

```bash
# Clone the repository
git clone https://github.com/ApiaryData/apiary.git
cd apiary

# Build all Rust crates
cargo build --workspace

# Build the Python package
maturin develop
```

### Running Tests

```bash
# Run all Rust tests
cargo test --workspace

# Run Python acceptance tests
python tests/test_step1_acceptance.py
```

## Code Style & Conventions

### Rust

- **Formatting**: Use `rustfmt` — run `cargo fmt --all` before committing
- **Linting**: Use `clippy` — run `cargo clippy --workspace --all-targets -- -D warnings`
- **Error handling**: Use `thiserror` for error types
- **Logging**: Use `tracing` for logging (not `println!` or `log`)
- **Documentation**: All public APIs must have rustdoc comments
- **Tests**: Unit tests go in `#[cfg(test)]` modules within each source file; integration tests go in `tests/`

### Python

- **Naming**: Use `snake_case` for all Python-facing APIs
- **Type hints**: Include type annotations for public functions

## Project Structure

```
apiary/
├── crates/
│   ├── apiary-core/       # Core types and traits
│   ├── apiary-storage/    # Storage backends (local, S3)
│   ├── apiary-runtime/    # Node runtime and bee pool
│   ├── apiary-query/      # DataFusion SQL engine integration
│   ├── apiary-python/     # PyO3 Python bindings
│   └── apiary-cli/        # Command-line interface
├── python/                # Python package source
├── docs/                  # Documentation
│   └── architecture/      # Design documentation
├── tests/                 # Acceptance tests
├── deploy/                # Docker Compose configs
├── scripts/               # Benchmark and utility scripts
├── benchmarks/            # Performance benchmarks
└── pyproject.toml         # Maturin build configuration
```

## CI Checks

All pull requests must pass the following checks before merging:

| Check | Command |
|-------|---------|
| Format | `cargo fmt --all -- --check` |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` |
| Test | `cargo test --workspace` |

The CI pipeline also builds release binaries, Python wheels, and a Docker image.

## License

By contributing to Apiary, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
