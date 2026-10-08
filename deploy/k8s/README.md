# Running Apiary on Kubernetes

One site is a **StatefulSet**, one pod per Node. What makes a pod a Node, and a
rescheduled pod the *same* Node, is its key, which lives in a Secret.

```bash
# On your own machine, where the Apiary key is kept (it never goes to the cluster):
apiary key generate --out apiary.key                 # once per Apiary
deploy/k8s/render.sh --apiary factory --colony line1 --replicas 3 \
    --apiary-key apiary.key --image ghcr.io/you/apiary:latest \
    [--relay https://relay.example.com] > site.yaml
kubectl apply -f site.yaml                           # contains keys: do not commit it
```

What it makes:

| Object | Role |
|---|---|
| Secret `apiary-keys` | One key per pod (`apiary-0.key`, ...). A pod's key is its Node id. |
| Secret `apiary-token` | The join token, signed by the Apiary key, naming the colony and every pod's id. |
| ConfigMap `apiary-config` | The Node config. Each pod fills in its own storage and key file at start. |
| Headless Service `apiary` | Its name resolves to every pod's address. The Nodes find each other through it (`[[net.dns_peers]]`), with no registry and no multicast. |
| StatefulSet `apiary` | The Nodes. `emptyDir` for the crop and cache, and a `PersistentVolumeClaim` for the comb. |

**The drive.** Pod `apiary-0` mounts the PersistentVolume and serves it as the comb
host; the other pods reach it through its Node id (`apiary-drive://<id>/`), as Pis
reach the drive on their comb host. The volume must be `ReadWriteOnce` block or
local storage: Delta commits need an atomic create-if-absent, which the host's own
file system gives and a network file share may not.

**Reaching other sites.** Pods dial out, so a pod can join a colony elsewhere
without any inbound rule. For the other direction, expose UDP 7000 with a `Service`
of type `LoadBalancer` or `NodePort` and add its address under `external_addrs`, or
point the Nodes at a relay (`--relay`); a pod behind a NAT that nobody can dial
directly is reached through the relay.

**Scaling.** Change `replicas` and re-run `render.sh` (the new pods need keys and a
token that names them), then apply. The operator that does this on its own comes
with the later phases; removing a pod without losing data means draining it first.

**Status.** `kubectl exec apiary-0 -- apiary net status` lists the peers, the paths
(direct or relayed) and how each peer's site was judged.

This manifest is validated for structure and its config is checked by the real
parser (`apiary node check`); it has not been run on a cluster. The gate in
`deploy/gate/phase2` runs the same Node configuration in a container topology.
