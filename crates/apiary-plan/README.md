# apiary-query

DataFusion-based SQL query engine for the [Apiary](https://github.com/ApiaryData/apiary) distributed data processing framework.

## Overview

`apiary-query` wraps [Apache DataFusion](https://datafusion.apache.org/) to provide SQL query capabilities over Apiary's Parquet-based storage:

- **ApiaryQueryContext** — Wraps DataFusion's `SessionContext` with Apiary namespace resolution (`hive.box.frame`)
- **Custom SQL commands** — `USE hive.box`, `SHOW HIVES`, `SHOW BOXES`, `SHOW FRAMES`, and `DESCRIBE frame`
- **Cell pruning** — Pushes WHERE predicates down to skip Parquet cells using column-level statistics
- **Projection pushdown** — Reads only the columns needed by the query
- **Distributed queries** — Cache-aware query planning that assigns cells to nodes based on locality and capacity

## Usage

```rust
use apiary_query::ApiaryQueryContext;

let ctx = ApiaryQueryContext::new(storage, registry).await?;

// Standard SQL
let results = ctx.sql("SELECT region, SUM(amount) FROM sales GROUP BY region").await?;

// Custom commands
ctx.sql("USE my_hive.my_box").await?;
ctx.sql("SHOW FRAMES").await?;
ctx.sql("DESCRIBE my_frame").await?;
```

## Supported SQL

- `SELECT` with projections, filters, joins, and subqueries
- Aggregations: `GROUP BY`, `AVG`, `SUM`, `COUNT`, `MIN`, `MAX`
- `USE hive.box` to set the active namespace
- `SHOW HIVES`, `SHOW BOXES`, `SHOW FRAMES` for discovery
- `DESCRIBE frame` for schema inspection
- `DELETE` and `UPDATE` are blocked with clear error messages (append-only model)

## License

Apache License 2.0 — see [LICENSE](../../LICENSE) for details.

Part of the [Apiary](https://github.com/ApiaryData/apiary) project.
