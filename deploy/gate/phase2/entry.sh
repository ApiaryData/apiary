#!/bin/sh
# Start a node: route through its home router if it has one, optionally under a
# fake clock (a Pi that booted without network time), then run apiary.
set -e
if [ -n "$GATEWAY" ]; then
  ip route replace default via "$GATEWAY"
fi
# Nodes on the open network cannot reach other sites' private subnets: on a real
# network those addresses are not routable. (Docker's host would route them.)
for net in $BLACKHOLE; do
  ip route add blackhole "$net"
done
if [ -n "$FAKETIME" ]; then
  lib=$(find /usr/lib -name 'libfaketime.so.1' | head -n1)
  export LD_PRELOAD="$lib"
  # Only the wall clock is wrong; timers keep real time.
  export FAKETIME_DONT_FAKE_MONOTONIC=1
  echo "running under a fake clock: $FAKETIME"
fi
exec apiary node run --config /etc/apiary/apiary.toml
