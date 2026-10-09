# The colony: Bees, roles and temperature

A Node's Bees are tasks, one per core, from `apiary-colony`. Each holds one role at a
time and reconsiders it only between Patches. Nothing assigns a Bee its role: the Node
says what is calling and what to do when a Bee answers (`Duties`), and the Bee chooses.

## Roles and what calls them

| Role | Stimulus | Work in a solitary Node |
|---|---|---|
| Forager | queries waiting | run the query, under the Bee's share of the Node's memory |
| Ripener | the crop is full or old; nectar is ready to cap; harvest is due | deposit, cap, harvest |
| Undertaker | clearing is due | delete files no table version needs |
| Scout | the Node's picture of its comb is stale | survey the comb for nectar ready to cap |
| Follower, Receiver, Guard | none yet | dances (phase 5), tremble signals and the handoff wait (phase 6); the entrance still validates inline |

A stimulus of 1 means a task of that kind is waiting.

## The choice

A Bee engages role *j* with probability `s² / (s² + θ²)`, for the stimulus *s* it sees
and its own threshold θ for that role.

- Thresholds are drawn per Bee from a log-normal spread seeded by Node id and Bee index.
  `colony_diversity` (σ, default 0.5) is the spread; zero makes every Bee the same.
- Doing a role lowers its threshold and neglect raises it, within bounds, so specialists
  emerge without configuration.
- The forager stimulus is damped by the share of Bees foraging, `s / (1 + α F̂)`.
- A Bee may not change role within its dwell time (2 s) while the role it holds is still
  wanted. A Bee idling in a role nobody needs is free to leave it.
- A new Bee's first Patch is calibration: it measures its scan rate and its latency to the
  comb store.

## Node temperature

`T = max(busy Bees / Bees, memory reserved / pool, claimed-but-unstarted Patches / limit,
SoC temperature / throttle point)`. On a Raspberry Pi the SoC reading comes from
`/sys/class/thermal`. Each Bee draws its own cooling threshold θ_cool from the same
spread; above it the Bee stops claiming, and it resumes below θ_cool less a hysteresis. Bees
therefore drop out one at a time as a Node heats, and the load curve bends rather than
saws on and off.

Work nobody has claimed yet is a stimulus, not a heat: counting it would stop the very
Bees that would drain it.

## Memory

Each Bee's Patch runs under a `CappedPool`, a share of the Node's DataFusion memory pool:
operators that would take more than the share are refused, and spill. Until a query is
split into one Patch per partition (phase 5), a Patch is a whole query over `cores`
partitions, so a Bee's share is that many partitions' worth.

## The comb: ripening yields to users, and commits are budgeted

- **Capping** runs as Ripener work. Delta treats a compaction that changes no data as
  transparent to concurrent writers, so capping and a user's overwrite carry a common
  application id (`REWRITE_FENCE`): whichever commits second conflicts. Capping then
  gives way, and an overwrite rebuilds from the table capping left and tries again.
  Without the fence, capping first and an overwrite built from the older table would both
  succeed, and the capped copy of the old rows would outlive the overwrite.
- **Commit budget.** Each Frame's log takes at most `commit_budget_per_min` commits
  (default 30), deposits and capping together. A Frame out of budget keeps its rows in the
  crop and the next commit is larger.

## What the simulator shows

- `jones.rs`: with the same arrivals, a diverse colony (σ = 0.5) holds the Node's
  temperature steadier than a uniform one (σ = 0), on every seed tried. The price is a
  somewhat longer queue.
- `ripening.rs`: across 60 seeds with the overwrite landing at every point of the cap,
  the Frame ends holding exactly the user's rows; and a Frame streamed to at ten batches a
  second, with a deposit cadence far faster than its budget, never sees more than the
  budget's commits in any minute, loses no rows, and would commit several times as often
  without the budget.
