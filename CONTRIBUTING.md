# Contributing to R-Kafka

Thank you for your interest in contributing to R-Kafka! This document provides guidelines and information for contributors.

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## How to Contribute

### Reporting Bugs

1. Check existing [Issues](../../issues) to avoid duplicates
2. Use the Bug Report template if available
3. Include: Rust version, OS, steps to reproduce, expected vs actual behavior, logs if applicable

### Suggesting Features

1. Open an issue with the `enhancement` label
2. Describe the use case and expected behavior
3. For protocol compatibility features, reference the relevant Kafka KIP (Kafka Improvement Proposal)

### Pull Requests

1. Fork the repository and create a branch from `main`
2. Follow the coding conventions (see below)
3. Add tests for new functionality
4. Ensure `cargo fmt`, `cargo clippy`, and `cargo test` pass
5. Write a clear commit message following [Conventional Commits](https://www.conventionalcommits.org/)
6. Open a PR and fill in the description template

## Development Setup

```bash
# Clone
git clone https://github.com/RegalPine/R-Kafka.git
cd r-kafka

# Build
cargo build

# Run tests
cargo test --workspace

# Format & lint
cargo fmt --all
cargo clippy --workspace -- -D warnings
```

## Coding Conventions

### Rust Style

- Follow standard `rustfmt` configuration
- Use `cargo clippy` with warnings as errors
- Prefer explicit error types over `unwrap()`/`expect()` in library code
- Document public APIs with `///` doc comments
- Include `#[cfg(test)]` unit tests in the same file as the implementation

### Architecture

- Each crate (`rk-*`) should have clear boundaries and minimal cross-dependencies
- Protocol types belong in `rk-protocol`
- Storage types belong in `rk-storage`
- Shared types belong in `rk-core`
- API handlers belong in `rk-broker`

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(protocol): add DescribeTopics API handler (API Key 70)
fix(storage): correct CRC32C offset calculation in RecordBatch
perf(network): optimize zero-copy fetch with splice syscall
docs(readme): update quick start guide
test(broker): add roundtrip test for Produce v10
```

### Testing

- **Unit tests**: In-file `#[cfg(test)]` modules
- **Integration tests**: In `crates/*/tests/` or `crates/rk-server/tests/`
- **Protocol compatibility**: Use `tests/testdata/` binary samples when available
- **Benchmarks**: In `crates/rk-server/tests/benchmark.rs`

## Areas Needing Help

- Protocol interoperability testing with real Kafka clients
- Performance benchmarking and optimization
- Documentation improvements
- Additional API version support
- Cross-platform testing (Linux, macOS)
- Packaging (Docker, Homebrew, cargo install)

## License

By contributing, you agree that your contributions will be licensed under the Apache License, Version 2.0.
