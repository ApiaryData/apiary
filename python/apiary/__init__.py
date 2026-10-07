"""Apiary — a distributed data processing framework inspired by bee colony intelligence.

Usage, embedded in your process:

    from apiary import Apiary

    ap = Apiary("production")
    ap.start()
    print(ap.status())
    ap.shutdown()

Usage, against a node started with ``apiary node run`` (needs pyarrow):

    import apiary

    ap = apiary.connect("grpc://pi-01:50051")
    print(ap.sql("SELECT count(*) FROM factory.line1.readings"))
"""

from apiary.apiary import Apiary

#: The embedded node: the same class as ``Apiary``.
embedded = Apiary

__all__ = ["Apiary", "connect", "embedded"]


def connect(url, token=None):
    """Connect to a running node's entrance, e.g. ``connect("grpc://pi-01:50051")``.

    Returns a client with ``sql``, ``ingest``, ``create_hive``, ``create_box``,
    ``create_frame``, ``set_recipe`` and ``flush_crop``. ``token`` is the shared
    bearer token, if the node requires one. Needs ``pyarrow``.
    """
    from apiary.client import connect as _connect

    return _connect(url, token)
