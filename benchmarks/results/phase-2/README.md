# Phase 2 gate: networking, membership and the comb host

`deploy/gate/phase2/run_gate.py` on a laptop under Docker Desktop: seven Nodes in a topology of
containers with real NAT, a relay, WAN latency and a Pi booted at 1970 (see
[the topology](../../../deploy/gate/phase2/README.md)). Both runs passed **28 of 28** checks.

| Run | Pod's NAT | Transcript |
|---|---|---|
| symmetric | `MASQUERADE --random`, so direct paths cannot cross it | [`gate-symmetric-nat.txt`](gate-symmetric-nat.txt) |
| cone | ordinary `MASQUERADE` | [`gate-cone-nat.txt`](gate-cone-nat.txt), [`gate-cone-nat.json`](gate-cone-nat.json) |

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
- **The pod behind a NAT reaches the Pis through the relay**, in both runs. With a plain-HTTP
  relay two NATed sites cannot learn each other's mapped addresses, so even a cone NAT stays
  relayed: direct NAT-to-NAT paths need the relay's QUIC address discovery, which needs TLS.
  That is not done. Everything connects; two NATed sites are just slower.
- **Revocation:** one push to one Node (the cloud VM) cut the revoked key off from all six
  others within seconds, and the revoked Node was told why.
- **The Pi booted at 1970** joined all six peers (QUIC authentication does not look at
  certificate dates), flagged every peer's token as newer than its clock, took deposits into
  its crop, refused to commit with "Clock not synchronised", and shipped its rows once
  restarted with the right time.
- **The drive:** two Pis deposited through the host; all three Pis read all 200 rows; the
  Parquet files were on the host's disk and none on the clients.

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
