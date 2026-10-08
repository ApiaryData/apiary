# Running a node: the `apiary` binary and its entrances

To run several Nodes as one colony (membership, discovery, relays, the shared drive), see [Networking](networking.md).

`apiary node run --config apiary.toml` runs one Node as a process: the comb, the
crop, ripening and harvest, and the entrances that let other programs reach it.
The embedded Python class (`Apiary`) runs the same Node inside your process.

```bash
apiary node check --config apiary.toml     # validate a config file
apiary node run   --config apiary.toml     # run until interrupted
apiary sql "SELECT count(*) FROM farm.field.readings" --url http://127.0.0.1:50051
```

SIGTERM and Ctrl-C stop the entrances, then the Node, which deposits its crop
before it exits.

## Configuration

A TOML file. Only `[node] storage` is required; every other setting takes the
Node's default. Unknown keys are errors.

```toml
[node]
storage = "local:///mnt/drive/apiary"     # the site's comb: local://<path> or s3://bucket/prefix
cache_dir = "/var/lib/apiary"             # the crop and set-aside deposits, on this Node's own disk
harvest = "s3://site-harvest/apiary"      # where capped Cells go (needs conditional writes)
deposit_interval_secs = 10
crop_max_mb = 64
crop_sync = true
retention_secs = 2592000                  # unset keeps harvested Cells on the drive for ever
# also: harvest_interval_secs, harvest_batch_mb, cap_interval_secs, cap_max_age_secs,
#       clear_interval_secs, clear_grace_secs

[flight]
listen = "127.0.0.1:50051"                # loopback by default: there is no user authentication yet
token_env = "APIARY_TOKEN"                # an optional shared bearer token (or token = "...")

[mqtt]
host = "broker.local"
port = 1883
client_id = "apiary-pi-01"
batch_rows = 1000                         # a batch is deposited at this many rows...
batch_interval = 500                      # ...or when its oldest message is this many ms old
idle_flush = 10                         # ...or when no message has arrived for this many ms (0 = off)
[[mqtt.subscriptions]]
topic = "plant/+/readings"
frame = "factory.line1.readings"          # hive.box.frame
```

The Flight entrance has one shared bearer token and no per-user access control.
It listens on the loopback address unless you say otherwise, and the Node warns
if you open it to the network without a token. Use it on a trusted network until
join tokens arrive with the networking phase.

## The Flight SQL entrance

Any Flight SQL client can query a Node and deposit into it: ADBC, JDBC, DBeaver,
`pyarrow.flight`, or `apiary.connect()` in Python.

- **Queries** are Flight SQL statements. A result's schema metadata carries
  `apiary.rows.crop` and `apiary.rows.comb`, the rows the query read from each
  stage. A query runs when its `FlightInfo` is requested, and its result waits
  (a minute, once) for the `DoGet` that reads it.
- **Deposits** are Flight SQL bulk ingests: the catalog is the hive, the schema is
  the box, the table is the frame. They append to an existing frame; a deposit
  does not create frames, replace data or take a transaction.
- **Browsing:** `GetCatalogs`, `GetDbSchemas`, `GetTables` (with the frame's
  columns, `_stage` last) and `GetSqlInfo` answer from the registry.
- **Custom actions** do what Flight SQL has no verb for. The body is JSON:

  | Action | Body |
  |---|---|
  | `apiary.create_hive` | `{"name"}` |
  | `apiary.create_box` | `{"hive", "name"}` |
  | `apiary.create_frame` | `{"hive", "box", "name", "schema": {column: type}, "partition_by": []}` |
  | `apiary.set_recipe` | `{"hive", "box", "frame", "sort_by": [], "dedup_by": []}` |
  | `apiary.flush_crop` | `{}` |

## The MQTT entrance

A Node subscribes to topic filters and deposits what arrives. Each subscription
feeds one frame. A message is JSON: an object, or an array of objects, whose
fields are the frame's columns (a field a message omits is null).

Messages are decoded one at a time, gathered per frame into batches, and admitted
through the Guard. A message is acknowledged to the broker only after its batch
has landed in the crop, so a Node that dies mid-batch is redelivered what it had
not secured. That is at-least-once: the crop does not deduplicate, and a frame
with a dedup key removes repeats when it ripens.

A broker holds only so many unacknowledged messages in flight to a subscriber (Mosquitto 20 by default), and a message is acknowledged only after it is deposited, so `idle_flush` deposits whenever the stream pauses; without it a stream of small messages would wait out `batch_interval` on every window. Messages that carry several rows go faster still.

## Guards and set-aside

A Guard checks every deposit against its frame's schema before it lands. A batch
is admitted when every column it carries is one the frame has, with a type that can
be cast to the frame's; columns it omits are filled with nulls if the frame allows.
Refused:

- a column the frame does not have (a field is never silently dropped),
- a missing required column,
- a type that cannot be cast, or values that do not fit (`"abc"` for an integer),
- a batch over 256 MB in memory.

What happens to a refused deposit depends on who sent it. A **caller** (Flight,
Python) gets the reason as an error and keeps its data. A **stream** (MQTT) has
nobody to tell, so the deposit is **set aside** in `<cache_dir>/set_aside/`: the
payload in one file and a `.json` sidecar with the frame, the source, the reason
and the time, for an operator to inspect and replay.

Evolving a frame to accept new columns is not supported yet: a deposit with a new
column is refused until the frame is recreated.
