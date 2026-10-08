# Phase 2 gate: networking, membership and the comb host

`deploy/gate/phase2/run_gate.py` on a laptop under Docker Desktop: seven Nodes in a topology of
containers with real NAT, a relay, WAN latency and a Pi booted at 1970 (see
[the topology](../../../deploy/gate/phase2/README.md)). Both runs passed **28 of 28** checks.

| Run | Pod's NAT | Transcript |
|---|---|---|
| symmetric | `MASQUERADE --random`, so direct paths cannot cross it | [`gate-symmetric-nat.txt`](gate-symmetric-nat.txt) |
| cone | ordinary `MASQUERADE` | [`gate-cone-nat.txt`](gate-cone-nat.txt), [`gate-cone-nat.json`](gate-cone-nat.json) |

With a TLS relay (`RELAY_TLS=1`, QUIC address discovery) the same gate passes **28 of 28**
again, in two runs:

| Run | Pod's NAT | Transcript |
|---|---|---|
| TLS relay, cone | ordinary `MASQUERADE` | [`gate-tls-cone-nat.txt`](gate-tls-cone-nat.txt) |
| TLS relay, symmetric | `MASQUERADE --random` | [`gate-tls-symmetric-nat.txt`](gate-tls-symmetric-nat.txt) |

**With the TLS relay and a cone NAT, the pod reaches every Pi directly**: the two NATed sites
punch through. With a symmetric NAT they stay on the relay, as they should. (The runs above
this table used a plain-HTTP relay.)

Paths as each Node saw them (the same in both runs):

```
             cloud  dockerhost     k8s-pod     pi-host        pi-1        pi-2     pi-late
cloud            -      direct      direct      direct      direct      direct      direct
dockerhost  direct           -      direct      direct      direct      direct      direct
k8s-pod     direct      direct           -     relayed     relayed     relayed     relayed
pi-host     direct      direct     relayed           -      direct      direct      direct
pi-1        direct      direct     relayed      direct           -      direct      direct
pi-2        direct      direct     relayed      direct      direct           -      direct
pi-late     direct      direct     relayed      direct      direct      direct           -
```

- **Within the Pi site every pair is direct**, and each Pi counts exactly the other Pis as its site.
- **Pis behind their router reach the public nodes directly** (they dial out).
- **With the plain-HTTP relay, the pod behind a NAT reaches the Pis through the relay**, in
  both runs: two NATed sites cannot learn each other's mapped addresses, so even a cone NAT
  stays relayed. The TLS relay above fixes that for cone NATs.
- **Revocation:** one push to one Node (the cloud VM) cut the revoked key off from all six
  others within seconds, and the revoked Node was told why.
- **The Pi booted at 1970** joined all six peers (QUIC authentication does not look at
  certificate dates), flagged every peer's token as newer than its clock, took deposits into
  its crop, refused to commit with "Clock not synchronised", and shipped its rows once
  restarted with the right time.
- **The drive:** two Pis deposited through the host; all three Pis read all 200 rows; the
  Parquet files were on the host's disk and none on the clients.

## What the drive costs

`drive_bench` (`crates/apiary-net/examples/drive_bench.rs`, raw numbers in [`drive.json`](drive.json)):
a client Node reaching the host's drive over real QUIC on loopback, in a container limited to
2 CPUs and 2 GB, against the same work on a local directory.

| | Local | Through the drive |
|---|---:|---:|
| head, 1 KiB (median of 300) | 0.03 ms | 0.25 ms |
| get, 1 KiB | 0.05 ms | 0.47 ms |
| put, 1 KiB | 0.13 ms | 0.37 ms |
| create-if-absent, 1 KiB (what a Delta commit rests on) | 0.09 ms | 0.24 ms |
| put 64 MiB | 86 ms (781 MB/s) | 144 ms (465 MB/s) |
| get 64 MiB | 47 ms (1,433 MB/s) | 387 ms (173 MB/s) |
| Delta commit of 1,000 rows | 12.5 ms | 15.7 ms |
| scan and aggregate, 2.04 million rows (10 MB) | 16 ms | 38 ms |

Reaching the drive through its host adds about a quarter to half a millisecond to each small
operation and about 3 ms to a commit. Bulk reads are the slowest part, at 173 MB/s: more than a
gigabit link can carry (about 110 MB/s), so on a Pi's LAN the wire is the limit, not this. A scan
costs about twice a local one at this size, because each Parquet read crosses the colony.

Loopback has no network latency and a laptop CPU, so these are a floor: a real LAN adds its
round trips to every operation, and a Pi 4 will do the QUIC encryption more slowly. Reads are not
streamed into the query engine in parallel ranges yet, so a wide scan over many files pays the
per-request cost serially.

## Cross-compiling for the Pi

The full `apiary` binary (iroh, the relay server, Delta, DataFusion) cross-compiles for
`aarch64-unknown-linux-gnu` in release mode (246 MB unstripped), and runs under ARM64
emulation: it reports `aarch64`, makes keys and signs tokens. That was the main build risk
of this phase; CI's aarch64 job builds the same package.

## What this run found

The gate found three real problems, all fixed in this phase:

1. **A Node only served requests on connections it had accepted.** When two Nodes found each
   other at once and the host's dial became the client's route to the host, the host never
   served the client's requests, and the client's registry read hung. The loopback tests could
   not see it, because their clients always dialled. Every admitted connection is now served,
   whoever dialled it, with a regression test.
2. **Delta's log reader needs a listing in key order.** A local file system lists in directory
   order, which made concurrent commits through the drive fail with "expected contiguous
   commit files". The host sorts its listings.
3. **The emulation was dishonest at first.** Docker's host routed between the bridge networks,
   so the "NAT" was bypassed and every path looked direct. Fixed in the topology (see its
   README), which is also why a first run showed site labels contradicted by 0.3 ms links.

## Caveats

A laptop under Docker, not a Raspberry Pi, a real router, or a real WAN. The 12 ms of WAN
latency is a stand-in. Direct paths between two NATed sites are untested for the reason above.
The Kubernetes manifest was not run on a cluster.
