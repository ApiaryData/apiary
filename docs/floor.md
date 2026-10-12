# The dance floor

`apiary-floor` is the shared board a colony's Nodes read and post to: who is here and
how loaded they are now, which work is worth doing and who has claimed it. It is gossiped, so no Node asks another for anything,
and everything on it fades, so nobody has to clean it.

## Entries

An entry has a key, a version, a lifetime and a body. The kinds are **presence** (a Node's
sign of life and load), **dance** (an advertisement of a stage's Patches) and **soft claim**
(a Bee's claim on one Patch); [foraging](foraging.md) says how the last two are used.

- The **key** is a kind, a target (empty for a Node's own presence, a query and stage for
  a dance, a Patch for a claim),
  the author's Node id and the author's Bee. One author owns a key. Only the author
  posts under it.
- Each post carries a higher **version** than the last. Merging keeps, per key, the
  highest version, and ties between different bodies go to the larger serialisation, so
  every Node picks the same one.
- The **lifetime** (at most 60 s) travels as what remains of it, not as a deadline. The
  receiver turns it into a deadline on its own monotonic clock. A Pi booted without
  network time reads the floor the same as any other Node.
- **Withdrawing** an entry posts a tombstone that lives as long as the longest entry can
  (60 s), so an older copy still on its way round the mesh cannot bring it back.
- An author that restarts publishes above the version of its key it can still see, because
  its old entries come back to it from its peers.

Merging is commutative, associative and idempotent: entries can arrive in any order,
twice, or by way of several Nodes, and every copy converges. `InMemoryFloor` holds one
copy and is the whole floor of a solitary Node. The `Floor` trait is the interface the
rest of the engine sees (`publish`, `withdraw`, `get`, `entries`, `sample`). `sample`
picks up to *k* live entries at random from the Node's own copy, which is how Followers
will choose dances.

## Gossip

`GossipFloor` keeps a copy in step with the colony over the gossip protocol:

- Every round (250 ms, jittered by up to a fifth either way from a seeded generator) a Node
  pushes to each peer in its colony what has changed on its copy since the last push to
  that peer. The peer merges it and answers with what it holds that is newer than the digest
  in the push.
- The first push to a peer and every fifth after it carry a digest of everything the
  sender holds, which repairs what a lost message or a reconnection left behind.
- A change from any source (a post or a merge) counts as a change, so entries pass through
  Nodes that can reach each other even when the two ends cannot. A full mesh makes the
  floor converge in one round; it is not required for it to converge.
- Only one push per peer is in flight, with a two-second limit, so a silent peer never
  holds up the others. A Node that has nothing new to say and no digest due says nothing.
- A change is sent at once (after a few milliseconds, to gather what else is about to
  change) instead of at the next round, because a claim the colony hears of late is a Patch
  two Bees may run.
- Only Nodes in the colony's own token are gossiped with and accepted from.

The floor does not dial. Whoever makes the mesh (discovery in a real Node, the test
harness in the simulator) keeps peers admitted, and gossip uses them. After a partition
heals, a redial is what brings a peer back.

## What the simulator checks

`crates/apiary-observe/tests/floor.rs`, on the simulated network with real Meshes and a
TLS relay:

- five Nodes (a cloud machine, three Pis behind a cone NAT, a pod) all converge, and a
  converged floor goes quiet;
- a Node that dies stops posting, and every other copy forgets it within one lifetime;
- two sides of a partition each hear their own news and not the other's, and agree again
  after the heal;
- entries cross Nodes that never connect to each other;
- a withdrawn entry is gone everywhere and stays gone;
- a run replays exactly from its seed.

## Not yet

- Heat and the later entry kinds arrive with the steps that use them.
- The V1 heartbeat files still feed the world view (`swarm_status`). A Node in a colony
  posts its presence on the floor, which is how it knows whether it has peers; the
  heartbeat-over-storage logic becomes `StoreFloor`, the fallback when peers are
  unreachable, in phase 10.
- Gossip is a full-mesh push-pull. Partial views and SWIM-style membership are phase 10.
