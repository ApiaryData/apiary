# Networking: membership, transport, discovery and the comb host

A site is a handful of Nodes (three to ten Pis, say) on one network. A colony is the
set of Nodes with fast links to each other. Apiary connects them over QUIC, dialled by
public key, with no VPN, overlay or extra daemon: the one `apiary` binary does the
transport, the discovery, the relay if you want one, and the serving of the comb's drive.

This page explains how Nodes know who may join, how they find each other, how a Node
without the drive uses it, and what to expect when things go wrong. The commands are
in [Running a node](node.md).

## Identity and membership

- **A Node's id is its public key.** Each Node makes an ed25519 key on first start and
  keeps it in a file (in Kubernetes, a Secret), so a restarted or rescheduled Node is the
  same Node. `apiary node id --config apiary.toml` prints it.
- **The Apiary has one signing key**, held by the Beekeeper and kept off the Nodes.
  `apiary key generate --out apiary.key` makes it; every Node is given only the public half
  (`apiary_public_key` in `[net]`).
- **A Node joins with a token** signed by that key. A token names the Apiary, the colony,
  an expiry and what the Node may do (`run`, `ingest`, `read`), and may carry bootstrap
  peers, a relay URL, and a single Node id it is bound to.

  ```bash
  apiary token issue --key apiary.key --apiary factory --colony line1 \
      --caps run,ingest --days 365 --relay http://relay.example:3340 \
      --bootstrap <node id>@192.168.1.10:7000 --bootstrap <other node id>@
  apiary token inspect <token> --public <apiary public key>
  ```

- **Every connection starts with a handshake** in which each end shows its token. The
  transport has already proved that the peer holds the key it connected as, so a token
  needs no second signature. A peer without a valid token is refused with the reason
  (a bad signature, the wrong Apiary, expired, bound to another Node, revoked) and never
  reaches any service.
- **Comb-store credentials never travel in a token.**

### Revocation

The Beekeeper keeps one cumulative list of revoked Node ids and token ids and signs it with
the Apiary key, numbering each version.

```bash
apiary revoke --key apiary.key --list revocations.json --node <node id> \
    --push http://some-node:50051
```

Nodes pass the list to each other on every connection, in both directions, and adopt any
list that is newer than theirs and correctly signed. So handing it to one Node is enough: it
adopts the list, closes any connection to the revoked key and passes the list on to every
peer. A site that was offline when the key was revoked learns of it the moment it
reconnects. The list is kept on disk, so a restart does not forget it. A revoked node is told
why when it tries to rejoin.

### Clocks

A site may be offline for days and a Pi may boot with no idea of the time, so:

- A Node never refuses a peer's token for expiry while its own clock is **behind the token's
  issue time**: the clock is plainly wrong, not the token. The peer is admitted and flagged
  (`clock_suspect` in `apiary net status`). A peer with a good clock still checks expiry on
  its side, so an expired token does not get far.
- **A Node with no trustworthy clock keeps ingesting but will not commit.** Delta commit
  timestamps are wall time, so every path that commits (deposit, direct write, overwrite,
  capping, harvest, clearing) refuses while the clock reads before 2025, or before the time
  the Node's own token was issued. The crop needs no wall time, so deposits keep landing in
  it and ship once the clock is right. The refusal says why.
- QUIC authentication uses the Node keys and does not look at certificate dates, so a Node
  with a wrong clock connects normally.

## Transport

Nodes talk over [iroh](https://iroh.computer): QUIC, dialled by public key, direct when
possible and through a relay when not. One connection per peer and protocol, with three
protocols separated by ALPN:

| Protocol | Carries |
|---|---|
| gossip | dance-floor entries and probes (later phases) |
| exchange | Arrow batches between stages (later phases) |
| control | requests and responses: the drive, probes |

Apiary does not use iroh's public infrastructure: no address-lookup service, no default
relay. The transport sits behind Apiary's own `Transport` trait, so it can be replaced
without touching the layers above, and the simulator can run the whole stack over an
in-memory network.

### Relays

A Node behind a NAT that nobody can dial needs a relay: a server both sides can reach, which
forwards their encrypted traffic. It sees only ciphertext, since the connection is
encrypted end to end with the Nodes' own keys. Any Node with a public address can run one:

```toml
[net]
serve_relay = "0.0.0.0:3340"          # in a Node on a cloud VM
relays = ["http://relay.example.com:3340"]   # in every Node that should use it
```

or `apiary relay run --listen 0.0.0.0:3340` for a relay with no Node attached. That relay
speaks plain HTTP: safe for the traffic it carries, for the reason above, and enough for
Nodes to reach each other, but it forwards only. Nodes behind NATs stay on it.

**A relay with TLS and address discovery** lets two Nodes behind ordinary NATs find a direct
path. Each Node asks the relay (over QUIC, on UDP 7842) what address its NAT gave it, the
two exchange those addresses through the relay, and both send at once so each NAT lets the
other in. QUIC needs TLS, so this relay needs a certificate and every Node must trust it:

```bash
apiary relay cert --out ./relay --name relay.example.com --name 203.0.113.7   # self-signed
apiary relay run --cert ./relay/relay.pem --key ./relay/relay.key             # or in a Node, below
```

```toml
[net]
relays = ["https://relay.example.com:3341"]   # in every Node
relay_ca = "/etc/apiary/relay.pem"            # trust a private relay's certificate
# relay_quic_port = 7842                      # where address discovery answers

[net.serve_relay_tls]                          # in the Node that runs the relay
https = "0.0.0.0:3341"
quic = "0.0.0.0:7842"
cert = "/etc/apiary/relay.pem"
key = "/etc/apiary/relay.key"
```

A certificate from a public CA needs no `relay_ca`. Open TCP 3341 and UDP 7842 to the relay.

**What to expect.** Nodes behind NATs reach everything they can dial (any node with a public
address, and each other on the same LAN) directly. Between two NATed sites:

- plain relay: through the relay;
- TLS relay, ordinary (cone) NATs: a direct path, found within seconds (the gate checks it);
- TLS relay, a symmetric NAT on either side: through the relay, as before. A symmetric NAT
  gives a new port per destination, so the address the Node learned is not the one its peer
  would reach.

Two things to know. A Node whose clock is badly wrong cannot verify the relay's certificate,
so it cannot use a TLS relay (it still reaches Nodes it can dial, and the clock gate already
stops it committing). And several Nodes behind one NAT should bind different `udp_port`s:
on the same port the router remaps one of them per destination, which looks like a symmetric
NAT and defeats punching.

Keep relayed paths out of anything that moves a lot of data; the site rule below does.

## Discovery

Sources only say where peers might be. They never vouch for them: the Discoverer dials what
they find, and membership decides who is admitted. A forged or stale announcement costs one
refused dial, with backoff.

| Where | Source | Config |
|---|---|---|
| First contact | bootstrap peers, from the join token or `[net] bootstrap` | a peer named only by id is dialled through the relay |
| A LAN | multicast DNS: id, colony and port | `mdns = true` (default) |
| Kubernetes | a headless Service's DNS name resolves to every pod | `[[net.dns_peers]]` (the pods' ids are known; DNS fills in addresses) |
| Anywhere | the comb: each Node writes `floor/<node>`, and any Node that can read the store finds the rest | `rendezvous = true` (default) |

## Sites and colonies

A colony is set by a Node's token. A **site** is judged: each Node's config can declare a
`site` label, and each Node measures the peers it talks to (round-trip time, throughput,
direct or relayed).

- **A declared label wins.** Where a label is missing, a direct link at or under 5 ms counts
  as the same site and a relayed or over-25 ms link counts as another.
- **Measurement flags contradictions** instead of overruling: two Nodes with the same label
  across a relay, or different labels on a sub-millisecond direct link, are reported in
  `apiary net status` as a contradiction.

## The comb host and its drive

A hive has one comb, on a drive plugged into one Node: the comb host. The role follows the
hardware. Every other Node reaches the drive through the host, over the colony's QUIC
connections, with no extra storage software.

```toml
# On the Pi the drive is plugged into:
[node]
storage = "local:///mnt/drive/apiary"
[net]
serve_comb = true

# On every other Node:
[node]
storage = "apiary-drive://<the host's node id>/"
```

The host serves its directory as an object store. A client Node's `delta-rs`, query engine,
registry and harvest all read and write through it, with the code above unchanged. What makes
that safe is the one rule every commit rests on, **create-if-absent**: the next Delta log
entry is created only if it does not exist, and the host's own file system makes that atomic,
so whichever Node creates it first wins. Eight Nodes racing for one entry produce exactly one
winner.

- **Permissions.** A token needs `run` or `ingest` to change the comb and any capability to
  read it. A `read` token cannot write.
- **Listings** are sorted by the host (Delta's log reader assumes the key order S3 gives; a
  local file system does not) and `list_with_offset` is answered by the host, so only the log
  tail crosses the network.
- **When the host is down**, Nodes keep ingesting into their crops. Deposits fail with a
  clear error and succeed when a host is back (the drive can be moved to another Pi, which
  becomes the host when it starts). While the host is down, the loss window stretches back
  to each Node's last deposit.
- **Network file shares are not used for the comb**, because whether create-if-absent holds
  over them depends on server and client settings. A NAS that serves S3 with conditional
  writes can stand in for the host and the drive together.
- Puts are buffered in memory on both ends (a Cell is at most one standard size), and a
  request that gets no answer from the host fails after a minute instead of hanging.

## Looking at it

```bash
apiary net status --url http://127.0.0.1:50051
```

shows the Node, the colony and site, whether it serves the comb, the revocation list version,
each admitted peer (colony, path, round-trip time, verdict, capabilities, whether its clock
looked wrong), and each peer discovery has found but that has not joined, with why.

## Configuration reference

All under `[net]`; a Node with no `[net]` section runs alone, as before.

| Key | Meaning |
|---|---|
| `apiary`, `apiary_public_key` | The Apiary's name and the key that signs its tokens (required) |
| `token` / `token_file` / `token_env` | This Node's join token (one of them is required) |
| `key_file` | Where the Node's key lives (default `<cache_dir>/net/node.key`) |
| `site` | This Node's site label |
| `udp_port` | The UDP port for QUIC (0 picks one; pin it behind a firewall or port mapping) |
| `relays`, `serve_relay`, `serve_relay_tls`, `relay_ca`, `relay_quic_port` | Relay servers to use; run one here (plain, or with TLS and address discovery); trust a private relay's certificate |
| `external_addrs` | Addresses to advertise besides those found (a published port) |
| `mdns`, `rendezvous` | Discovery sources (both default on) |
| `bootstrap`, `dns_peers` | Peers to dial first; peers found by hostname |
| `revocations_file` | Where the revocation list is kept |
| `serve_comb` | This Node has the drive and serves it |
| `discovery_interval_secs`, `measure_interval_secs` | How often to look for peers and measure them |

## Deployment

| Platform | How |
|---|---|
| Raspberry Pis on a LAN | One binary per Pi under systemd; mDNS finds the rest; one Pi has the drive |
| Docker or Compose | A container per host; publish one UDP port or use host networking; a volume for the key and cache |
| Kubernetes | [A StatefulSet](../deploy/k8s/README.md), the key in a Secret, a headless Service; the pods dial out |
| Cloud VMs | The binary or a container, the token in user data; a VM with a public address is the natural relay |
| Pi site plus cloud | Two colonies in one Apiary; the cloud VM runs the relay |

## What has been checked, and what has not

`deploy/gate/phase2` builds a topology of containers with real NAT (a home router that drops
unsolicited inbound traffic, a symmetric NAT, a relay on the open network, WAN latency, a Pi
booted at 1970) and checks the phase 2 gate: the colonies form, pairs within a site connect
directly, every cross-site pair connects directly or through the relay (directly, by hole punching,
across two ordinary NATs when the relay has TLS), a revoked key is
refused everywhere from one push to one Node, a Pi booted without network time joins and
ingests but will not commit until its clock is right, and every Node in a site commits to the
drive through its host.

Not checked: real Pis behind a real home router, a real Kubernetes cluster (the manifest is
validated for structure and its config is parsed by the real parser, but not run), a cloud
VM, and NATs from real routers (the gate's are Linux's; some consumer routers are stricter).
