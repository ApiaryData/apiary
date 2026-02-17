# apiary-cli

Command-line interface for the [Apiary](https://github.com/ApiaryData/apiary) distributed data processing framework.

## Overview

`apiary-cli` provides a command-line tool for managing Apiary nodes and running queries. The CLI is currently a placeholder for v1; the primary interface is the [Python SDK](../apiary-python/README.md).

### Planned Commands

```
apiary start [--storage <uri>]   Start a local Apiary node
apiary sql "SELECT ..."          Run a SQL query
apiary shell                     Open a Python REPL with Apiary pre-configured
```

## Building

```bash
cargo build -p apiary-cli
```

## License

Apache License 2.0 — see [LICENSE](../../LICENSE) for details.

Part of the [Apiary](https://github.com/ApiaryData/apiary) project.
