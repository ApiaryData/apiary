# Foraging: a query run by a colony

A query on a Node with no peers runs the way it always has, whole, on one Bee. In a
colony it can be cut into **Patches** that any Bee on any Node runs. Nothing assigns the
Patches: Bees follow **dances** on the [dance floor](floor.md), claim what they find
and hand batches to each other. This page is how that works. The mechanisms are in
`apiary-plan` (stages, Patches, the exchange, model functions), `apiary-forage` (who does
what) and `apiary-runtime` (the Node's side).

## From query to Patches

DataFusion plans a query as a tree of operators over partitions. Everywhere the tree moves
rows between partitions (a hash or round-robin repartition, or a merge into one partition)
it can be cut. The part below a cut is a **stage**; the part above reads the stage's
output through a `StageInputExec`. Every operator left inside a stage maps partition *i* of
its inputs to partition *i* of its output, so each partition of a stage is an independent
**Patch**.

- A stage is kept as its boundary plan node (the repartition or merge). A Patch runs the
  boundary's input for its partition and does what the boundary does with the rows: splits
  them by hash or round robin into output partitions, or passes them on whole.
- The stages are written with `datafusion-proto` to `plans/<query>` in the comb store, so
  any Node can run any Patch and the query outlives the Node that planned it.
- Leaves that cannot travel need a codec. A Frame scan is a `FrameScanExec` carrying its
  table version, columns, filters and the files each partition reads; the Node that runs
  the Patch rebuilds the scan through the comb, so two Nodes given the same spec read the
  same rows. Rows held in one Node's memory (a crop read) are a `HeldExec`, and the stages
  that read them are pinned to the Node that planned the query.
- A Patch runs a fresh copy of its stage's plan. DataFusion shares scan and join state
  between the partitions of one plan instance, so reusing an instance would make a second
  attempt at a Patch see no rows.

A query planned and run whole gives the same rows as the same query cut into Patches
(`ApiaryQueryContext::sql_staged` runs one on a single Node and is the reference).

## The exchange

Each Patch's output goes to an `Exchange`. The stage below reads, for each of its
partitions, the matching output of every Patch above.

- **One winner.** Each attempt writes its outputs under its own name and then creates the
  Patch's completion record, which can be created only once. The attempt that creates it
  wins; the other discards its work. A Patch is committed exactly once with no coordination.
- **Finished queries.** A query that has been answered leaves a marker, so an attempt still
  running cannot complete a Patch of it.
- **The comb store** (`StoreExchange`, under `outputs/`) works between any two Nodes.
- **QUIC** (`NetExchange`): the output is also kept in memory on its producer and served
  over the exchange protocol, so a consumer in the colony gets it in one hop. If the
  producer is gone, slow, or evicted it, the consumer reads the store, which has it.

## Who does what

| Role | Stimulus | Work |
|---|---|---|
| Scout | a query to plan; a query planned here due a look; a Patch running far too long | plan; advertise stages; ask for stragglers to be run again |
| Forager | Patches of a query planned here that can run now | pull the next one (stages that read rows only this Node holds first) |
| Follower | dances with Patches left | follow one dance, claim a Patch of its group, run it |
| Receiver | the inputs of a query's result stage are done | run it and answer the caller |

- **Small queries stay put.** The Scout that plans a query runs it whole, inline, if it
  reads at most `local_bytes` (64 MB by default). A dance cannot recruit anyone before such
  a query is finished, and handing batches between stages through the comb store would
  cost more than it saves. A small query in a colony takes the time it takes alone.
- **A dance** advertises a stage's Patches. Its lifetime is its circuits, so its
  remaining life *is* its strength, and a dead dancer's dance fades with no one deleting it.
  A Scout posts a flat dance (one circuit) that says only that there is work. A Forager that
  finishes a Patch measures its profitability, rows per Bee-second, against the Node's own
  pace; she starts a dance of ⌈k·π/π_baseline⌉ circuits (at most 12), renews it one step
  lower with each Patch she finishes, and dances only when profitability has been patchy
  (a coefficient of variation above 0.25).
- **Following.** A Follower follows one dance, chosen at random in proportion to its
  strength over all the dances on her Node's floor copy (a uniform sample of 64 if there are
  more). She does not sort or compare them. `Mode::Pull` is the baseline for the
  comparison: every dance counts the same.
- **Claims.** She posts a soft claim and starts at once. Each Node takes Patches from its
  own point on the ring of the group and its Bees take neighbouring ones, so the first wave
  of Bees on different Nodes, reacting before gossip tells any of them of the others'
  claims, seldom lands two on one Patch. Before she starts she checks the comb store for the
  Patch's completion record, because a Done marker on the floor fades and a Node forgets
  what it learned of a query.
- **Duplicates.** Two Bees can still claim one Patch, and the first to finish wins. A Node
  whose recent attempts were wasted above 4% waits a gossip round after claiming and yields
  to the lower Node and Bee id. Gossip pushes news at once, not at the next round.
- **Stragglers.** Every claim on the floor carries its group's size and how long it has
  run, so any Scout can see a Patch that one Bee has been running more than six times as
  long as the group's median, and advertise it. A second Bee runs it, never a third, and
  whichever attempt finishes first is committed.
- **Answering.** The Receiver trusts the floor's Done markers to know a stage looks done and
  checks the comb store before running the result stage.

## Standing queries and model functions

A **standing query** is a rollup registered once (`register_standing`): each time a batch
lands for its source Frame, the Node queues a Patch that runs the query over that batch
(readable as the table `batch`) and ingests the result into the target Frame. It is a
Forager's work, one the Node's Bees come back to with every batch. The target holds partial
aggregates, sums and counts, combined when it is queried. Queries are kept in the comb
store (`standing/`), so they survive a restart. The queue is bounded; a Node that cannot
keep up counts the batches it skipped until backpressure arrives with the tremble signals.

`predict('model', feature, ...)` runs a model over SQL features. Models are files in the
comb store (`models/<name>/`), fetched by whichever Node runs a Patch that calls them, and
fetched again when a newer version is registered (`register_model`). The `linear` kind (a
weight per feature and a bias) is built in; the `onnx` kind runs any ONNX model through
ONNX Runtime (feature `onnx`, which loads the runtime's shared library when first needed,
so a build needs no copy of it). `scripts/onnx-test.sh` runs the backend against a real
runtime in a container.

## What the simulator checks

`crates/apiary-observe/tests/forage.rs` runs real Bees, a real gossiped floor and real
foraging with Patches that take virtual time:

- a rollup submitted at a Pi, with a cloud machine, a pod and another Pi in the colony,
  is finished by all of them, and every Patch is committed once;
- on a mixed, overloaded workload (cheap rollups, ad hoc SQL, dear inference), dances
  produce about 45% more rows than pull on every seed;
- duplicate work is under 1% when Patches outlast gossip, and yielding to the lower id
  wastes less when they do not;
- a straggler is run again; a Node lost mid-query does not lose the query; a stage pinned
  to one Node runs only there; a small query is run whole and recruits nobody;
- outputs travel between Nodes over the exchange protocol, with the store read only when a
  producer is gone;
- a run replays exactly from its seed.

`crates/apiary-runtime/tests/colony_query.rs` does the same with two real Nodes over one
comb: aggregates, ordered limits and joins give the rows they give run whole, Patches run on
both, batches cross over the exchange protocol, and a model registered on one Node is
fetched and run by the other.

## Not yet

- Results of queries run across the colony do not report the rows each stage gave them
  (`_stage` counts); the solitary path does.
- A Node whose Bees are all busy delays the answer to its own callers: the Receiver is a
  Bee on the Node that planned the query.
- A query sees the crop of the Node that planned it, and not the crops of other Nodes
  (the stage that reads the crop is pinned to the planning Node). Each Node listing its
  undeposited ranges on the floor, so a query sees every crop in the colony, is not built.
- Cross-site foraging, hard claims and WAN budgets are phase 9.
