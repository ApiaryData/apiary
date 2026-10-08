# The phase 2 gate topology

```
                      wan 10.20.0.0/24  (the open network; WAN latency on every router)
   ┌──────────────────────┬──────────────────────┬─────────────────────┬─────────────────┐
 cloud                dockerhost             nat-home              nat-lab
 relay + node         node, public           MASQUERADE            symmetric NAT
 10.20.0.20           10.20.0.30             drops unsolicited     (or a cone NAT with
                                              inbound               LAB_NAT=MASQ)
                                                 │                     │
                                       home 10.10.0.0/24        lab 10.30.0.0/24
                                  pi-host  pi-1  pi-2  pi-late     k8s-pod
                                  (the drive)            (clock at 1970)
```

Run it:

```bash
python deploy/gate/phase2/run_gate.py              # builds the image, runs, tears down
python deploy/gate/phase2/run_gate.py --skip-build --keep
LAB_NAT=MASQ python deploy/gate/phase2/run_gate.py --skip-build
RELAY_TLS=1 LAB_NAT=MASQ python deploy/gate/phase2/run_gate.py --skip-build   # TLS relay: NAT-to-NAT hole punching
```

`RELAY_TLS=1` runs the relay with TLS and QUIC address discovery (the gate makes the
certificate and every Node trusts it). With `LAB_NAT=MASQ` the pod and the Pis then reach
each other directly; with the default symmetric NAT they stay relayed. `pi-late` (1970
clock) cannot verify the relay's certificate, so it and the pod do not meet in this mode.

It needs Docker with permission to give containers `NET_ADMIN`. It makes the Apiary key,
Node keys, tokens and configs with the `apiary` CLI, as a Beekeeper would, starts every
Node, and checks the gate: the colonies form with nothing but the one binary; every pair
within the Pi site connects directly; every cross-site pair connects directly or through the
relay; a revoked key is refused everywhere from one push to one Node; a Pi booted without
network time joins and ingests but will not commit until its clock is right; every Pi in the
site commits to the drive through its host.

**What makes the topology honest.** Docker's host happily routes between its bridge
networks, which would let the "private" subnets reach each other and bypass the NAT, so:
each router sends everything out its WAN side and drops private destinations there (the
open network does not route a site's subnets), the public nodes blackhole the private
subnets, and the routers add WAN latency with `tc netem`. Without these, every path looked
direct at 0.3 ms and the site judge rightly flagged the contradictions.

**What it does not cover.** Real hardware, a real home router, a real cluster, real WAN
conditions. The "Kubernetes pod" is a container dialling out of its own NAT, which is what a
pod does; the StatefulSet manifest is in `deploy/k8s`.
