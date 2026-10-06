# SQL Reference

Apiary uses [Apache DataFusion](https://datafusion.apache.org/) as its SQL engine. Queries run over Parquet cells with automatic pruning and projection pushdown.

## Table References

Tables use a three-part name: `hive.box.frame`.

```sql
SELECT * FROM warehouse.sales.orders;
```

Set context with `USE` to shorten references:

```sql
USE HIVE warehouse;
USE BOX sales;
SELECT * FROM orders;
```

Two-part names also work after `USE HIVE`:

```sql
USE HIVE warehouse;
SELECT * FROM sales.orders;
```

## SELECT

Standard SQL SELECT with full expression support.

```sql
SELECT order_id, customer, amount
FROM warehouse.sales.orders
WHERE amount > 100
ORDER BY amount DESC
LIMIT 10;
```

### WHERE

```sql
SELECT * FROM warehouse.sales.orders
WHERE region = 'us' AND amount >= 50.0;
```

WHERE predicates on partition columns trigger **cell pruning** — only matching Parquet files are read.

### GROUP BY and Aggregates

Supported aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.

```sql
SELECT customer, COUNT(*) AS order_count, SUM(amount) AS total
FROM warehouse.sales.orders
GROUP BY customer;
```

```sql
SELECT region, AVG(amount) AS avg_amount
FROM warehouse.sales.orders
GROUP BY region
HAVING AVG(amount) > 100;
```

### ORDER BY

```sql
SELECT * FROM warehouse.sales.orders
ORDER BY amount DESC, customer ASC;
```

### LIMIT

```sql
SELECT * FROM warehouse.sales.orders
LIMIT 25;
```

### JOIN

```sql
SELECT o.order_id, o.amount, c.name
FROM warehouse.sales.orders o
JOIN warehouse.sales.customers c ON o.customer_id = c.id;
```

## Custom Commands

### USE HIVE

Set the current hive context. Subsequent queries can omit the hive prefix.

```sql
USE HIVE warehouse;
```

### USE BOX

Set the current box context. Requires a hive to be set first.

```sql
USE HIVE warehouse;
USE BOX sales;
```

### SHOW HIVES

List all hives in the apiary.

```sql
SHOW HIVES;
-- Returns: name
--          warehouse
--          analytics
```

### SHOW BOXES

List all boxes in a hive.

```sql
SHOW BOXES IN warehouse;
```

### SHOW FRAMES

List all frames in a box.

```sql
SHOW FRAMES IN warehouse.sales;
```

### DESCRIBE

Show schema, partitions, cell count, row count, and size for a frame.

```sql
DESCRIBE warehouse.sales.orders;
-- Returns: column_name | data_type | partition | ...
--          order_id    | int64     | false     |
--          customer    | utf8      | false     |
--          amount      | float64   | false     |
--          region      | utf8      | true      |
```

## Blocked Operations

Apiary does not support DML via SQL. These statements return clear error messages directing you to the Python SDK:

| Statement | Alternative |
|-----------|-------------|
| `INSERT`  | `write_to_frame()` |
| `UPDATE`  | `overwrite_frame()` |
| `DELETE`  | `overwrite_frame()` (replace all data) |
| `CREATE TABLE` | `create_frame()` |
| `DROP TABLE`   | Not yet supported |
| `ALTER TABLE`  | Not yet supported |
| `COPY`  | Not supported; query results are returned to the caller |
| `SET`, `RESET` | Not supported; the Node configures its own query session |

## Query Execution

1. **Parse** — DataFusion parses the SQL.
2. **Resolve** — Table names are resolved through the registry to a Frame's Delta table, using the current hive and box for short names. Names are matched exactly first, then case-insensitively.
3. **Plan** — DataFusion builds a physical plan with projection pushdown, partition pruning and file skipping from Delta statistics. Scans are lazy: no Frame is loaded into memory first.
4. **Fit joins to a Bee** — A hash join stays only if its build side is known to fit a Bee's memory; otherwise it becomes a sort-merge join, which spills to disk.
5. **Execute** — Bees run the plan under the Node's shared memory pool; operators that exceed it spill to the Node's spill directory.
6. **Return** — Results are serialized as Arrow IPC bytes.

All queries on a Node share one long-lived session, so they run concurrently, and `USE HIVE` / `USE BOX` apply to every caller of that Node.

## Examples

```sql
-- Top 5 customers by revenue
USE HIVE warehouse;
USE BOX sales;
SELECT customer, SUM(amount) AS revenue
FROM orders
GROUP BY customer
ORDER BY revenue DESC
LIMIT 5;

-- Count orders per region
SELECT region, COUNT(*) AS cnt
FROM warehouse.sales.orders
GROUP BY region;

-- Inspect a frame
DESCRIBE warehouse.sales.orders;

-- Browse the namespace
SHOW HIVES;
SHOW BOXES IN warehouse;
SHOW FRAMES IN warehouse.sales;
```
