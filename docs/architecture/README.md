# V1 architecture (historical)

These documents describe **Apiary V1**: storage-only coordination through object
storage, a custom JSON ledger, heartbeat files, and no node-to-node traffic.
V1 is what the code implements today, and the design below replaces it in phases.

The current design is [`docs/design/apiary-biomimetic-design.md`](../design/apiary-biomimetic-design.md).
Its build order (section 12) is the migration path, and each step has a gate that
must pass before the next begins.

Where these V1 documents and the design disagree, the design wins. Notably, V1's
"no gossip, no Flight, no Raft, no node-to-node communication" rules, its custom
ledger, and its `_queries/` object-storage query path are all superseded. The
`_queries/` path has already been removed from the code.
