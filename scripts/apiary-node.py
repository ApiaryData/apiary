#!/usr/bin/env python3
"""Apiary node runner — starts a node and keeps it alive.

Environment variables
---------------------
APIARY_NAME           Apiary instance name          (default: production)
APIARY_STORAGE_URL    Object-storage URL            (default: solo/local mode)
AWS_ACCESS_KEY_ID     S3 / MinIO access key
AWS_SECRET_ACCESS_KEY S3 / MinIO secret key
AWS_ENDPOINT_URL      Custom S3 endpoint (MinIO)
AWS_REGION            AWS region                    (default: us-east-1)
RUST_LOG              Log verbosity                 (default: info)
"""

import os
import signal
import sys
import time


def main():
    # Lazy import so the script fails fast with a clear message when the
    # package is missing.
    try:
        from apiary import Apiary  # noqa: WPS433
    except ImportError:
        print(
            "ERROR: The 'apiary' Python package is not installed.\n"
            "Install it with:  pip install apiary-data   (or maturin develop)",
            file=sys.stderr,
        )
        sys.exit(1)

    name = os.environ.get("APIARY_NAME", "production")
    storage_url = os.environ.get("APIARY_STORAGE_URL")

    kwargs = {"name": name}
    if storage_url:
        kwargs["storage"] = storage_url

    ap = Apiary(**kwargs)
    ap.start()

    status = ap.status()
    print(
        f"Apiary node started — {status['cores']} cores, "
        f"{status['memory_gb']:.2f} GB",
        flush=True,
    )

    # Graceful shutdown on SIGINT / SIGTERM
    def _shutdown(sig, _frame):
        signame = signal.Signals(sig).name
        print(f"\nReceived {signame}, shutting down …", flush=True)
        ap.shutdown()
        print("Node stopped.", flush=True)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Main loop — periodically log swarm status
    try:
        while True:
            time.sleep(60)
            try:
                swarm = ap.swarm_status()
                print(
                    f"Swarm: {swarm['alive']} alive, "
                    f"{swarm['total_bees']} bees, "
                    f"{swarm['total_memory_gb']:.2f} GB",
                    flush=True,
                )
            except Exception as exc:
                print(f"Warning: swarm_status failed: {exc}", flush=True)
    except Exception as exc:
        print(f"Fatal: {exc}", file=sys.stderr, flush=True)
        ap.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()
