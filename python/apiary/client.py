"""A client for a running Apiary node, over Arrow Flight SQL.

    import apiary

    ap = apiary.connect("grpc://pi-01:50051")
    ap.create_hive("factory")
    ap.create_box("factory", "line1")
    ap.create_frame("factory", "line1", "readings", {"id": "int64", "temp": "float64"})

    ap.ingest("factory", "line1", "readings", table)          # a pyarrow Table
    ap.sql("SELECT avg(temp) FROM factory.line1.readings")    # a pyarrow Table

It needs ``pyarrow`` (``pip install pyarrow``). The node it talks to is started
with ``apiary node run --config apiary.toml``.
"""

import json
import struct

try:
    import pyarrow as pa
    import pyarrow.flight as flight
except ImportError as exc:  # pragma: no cover - exercised only without pyarrow
    raise ImportError(
        "apiary.connect() needs pyarrow: pip install pyarrow"
    ) from exc

__all__ = ["ApiaryError", "Client", "connect"]

_SQL_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql."


class ApiaryError(RuntimeError):
    """A node refused or failed a request. The message says why."""


# -- the little protobuf the Flight SQL commands need ------------------------


def _varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _field_bytes(number, payload):
    return _varint((number << 3) | 2) + _varint(len(payload)) + payload


def _field_string(number, text):
    return _field_bytes(number, text.encode("utf-8"))


def _any(message_name, payload):
    """A protobuf ``Any`` holding a Flight SQL command."""
    return _field_string(1, _SQL_PREFIX + message_name) + _field_bytes(2, payload)


def _statement_query(query):
    return _any("CommandStatementQuery", _field_string(1, query))


def _statement_ingest(hive, box_name, frame):
    # table_definition_options { if_exists = APPEND }, then the table's three names.
    options = _varint((2 << 3) | 0) + _varint(2)
    payload = (
        _field_bytes(1, options)
        + _field_string(2, frame)
        + _field_string(3, box_name)
        + _field_string(4, hive)
    )
    return _any("CommandStatementIngest", payload)


def _record_count(app_metadata):
    """``DoPutUpdateResult.record_count`` (field 1, a varint)."""
    data = bytes(app_metadata)
    if not data or data[0] != (1 << 3):
        return 0
    value, shift = 0, 0
    for byte in data[1:]:
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    return value


def _location(url):
    for prefix, replacement in (("http://", "grpc://"), ("https://", "grpc+tls://")):
        if url.startswith(prefix):
            return replacement + url[len(prefix):]
    return url


def _wrap(error):
    """An ApiaryError saying what the node said.

    pyarrow raises ArrowInvalid, ArrowKeyError and the like for some gRPC
    statuses and FlightError subclasses for others; callers see one type.
    """
    detail = getattr(error, "message", None) or str(error)
    return ApiaryError(detail)


# -- the client --------------------------------------------------------------


class Client:
    """A connection to one node's entrance. Make one with :func:`connect`."""

    def __init__(self, url, token=None):
        self._client = flight.FlightClient(_location(url))
        headers = []
        if token:
            headers.append((b"authorization", f"Bearer {token}".encode()))
        self._options = flight.FlightCallOptions(headers=headers)

    # Queries ------------------------------------------------------------

    def sql(self, query):
        """Run a SQL query; returns a ``pyarrow.Table``.

        The schema's metadata carries ``apiary.rows.crop`` and ``apiary.rows.comb``,
        the rows the query read from each stage.
        """
        try:
            descriptor = flight.FlightDescriptor.for_command(_statement_query(query))
            info = self._client.get_flight_info(descriptor, self._options)
            tables = [
                self._client.do_get(endpoint.ticket, self._options).read_all()
                for endpoint in info.endpoints
            ]
        except (flight.FlightError, pa.ArrowException) as e:
            raise _wrap(e) from None
        if not tables:
            return pa.Table.from_batches([], schema=info.schema)
        return pa.concat_tables(tables) if len(tables) > 1 else tables[0]

    # Deposits -----------------------------------------------------------

    def ingest(self, hive, box_name, frame, table):
        """Deposit a ``pyarrow.Table`` into a frame; returns the rows landed.

        The batch is checked against the frame's schema first. One that does not
        fit is refused with the reason and nothing lands.
        """
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])
        try:
            descriptor = flight.FlightDescriptor.for_command(
                _statement_ingest(hive, box_name, frame)
            )
            writer, reader = self._client.do_put(descriptor, table.schema, self._options)
            writer.write_table(table)
            writer.done_writing()
            result = reader.read()
            writer.close()
        except (flight.FlightError, pa.ArrowException) as e:
            raise _wrap(e) from None
        return _record_count(result) if result is not None else 0

    # Namespace ----------------------------------------------------------

    def create_hive(self, name):
        """Create a hive (harmless if it exists)."""
        self._act("apiary.create_hive", {"name": name})

    def create_box(self, hive, name):
        """Create a box in a hive."""
        self._act("apiary.create_box", {"hive": hive, "name": name})

    def create_frame(self, hive, box_name, name, schema, partition_by=None):
        """Create a frame. ``schema`` is a dict of column name to type."""
        self._act(
            "apiary.create_frame",
            {
                "hive": hive,
                "box": box_name,
                "name": name,
                "schema": schema,
                "partition_by": list(partition_by or []),
            },
        )

    def set_recipe(self, hive, box_name, frame, sort_by=None, dedup_by=None):
        """Set how a frame ripens: its sort key and its deduplication key."""
        self._act(
            "apiary.set_recipe",
            {
                "hive": hive,
                "box": box_name,
                "frame": frame,
                "sort_by": list(sort_by or []),
                "dedup_by": list(dedup_by or []),
            },
        )

    def flush_crop(self):
        """Deposit the node's crop into the comb now; returns what moved."""
        return self._act("apiary.flush_crop", {})

    def _act(self, kind, body):
        try:
            action = flight.Action(kind, json.dumps(body).encode("utf-8"))
            results = list(self._client.do_action(action, self._options))
        except (flight.FlightError, pa.ArrowException) as e:
            raise _wrap(e) from None
        return json.loads(results[0].body.to_pybytes()) if results else {}

    # Lifecycle ----------------------------------------------------------

    def close(self):
        """Close the connection."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def connect(url, token=None):
    """Connect to a node's entrance, e.g. ``connect("grpc://pi-01:50051")``.

    ``token`` is the shared bearer token, if the node requires one.
    """
    return Client(url, token)
