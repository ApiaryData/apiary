"""Apiary — a distributed data processing framework inspired by bee colony intelligence.

Usage:
    from apiary import Apiary

    ap = Apiary("production")
    ap.start()
    print(ap.status())
    ap.shutdown()
"""

from apiary.apiary import Apiary

__all__ = ["Apiary"]
