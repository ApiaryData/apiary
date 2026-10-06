# Apiary: A Biomimetic Data Processing Engine in Rust

Oct 6, 2026 · @Ust Oldfield

## Abstract

Apiary is rebuilt from a clean slate as a Rust data processing engine in which no node leads and every scheduling, backpressure and data-placement decision comes from a documented honeybee (*Apis mellifera*) mechanism. Apache DataFusion executes queries and Delta Lake stores data; the colony decides who runs what, where data sits, and when to slow down.

It is built first for edge and IoT sites: three to ten Raspberry Pis at a factory or shop that ingest sensor data, run SQL and models over it, keep working through days without an uplink, and keep their data on a cluster-attached external drive. Capped data is harvested to a cloud object store, where Databricks or a similar platform refines it into products for customers. The same engine serves equally as that edge tier and as a standalone alternative to cloud platforms.

The engine coordinates through two media, as a colony does. The comb is durable: Delta tables on the site's cluster-attached drive, committed by conditional writes that any node may attempt and only one can win. The dance floor is ephemeral: dances, tremble dances and stop signals pass between nodes by gossip and fade unless renewed. Five mechanisms carry the design: response thresholds with individual variation allocate Bees to roles; the waggle dance recruits idle Bees to work in proportion to its payoff; the tremble dance and stop signal give backpressure and load shedding; comb pattern formation places hot, warm and cold data with no tiering policy; and quorum sensing makes the colony's few irrevocable decisions. Nectar ripening and capping give the write path its lifecycle.

Nodes reach each other by public key over QUIC, directly, through NAT or via a relay, with no extra software to install. A colony is one network site. Emergence runs inside each colony and, more slowly, between colonies, where every payoff is discounted by the cost of the WAN; when signals between sites go stale, work falls back to planned fragments that return reduced results.

Correctness never rests on the biology. Delta's commit protocol, idempotent stage outputs and fencing tokens carry it, and the colony mechanisms govern performance only. Every mechanism names its biological source, its control law and the simulation test that shows it behaves like the colony.

## Design decisions

These decisions were taken on 6 October 2026, and where earlier drafts assumed otherwise, they win.

| Question | Decision | What it changes | § |
| --- | --- | --- | --- |
| First users | Edge and IoT sites: factories and retail | Ingest, offline survival and small sites set the priorities | Feasibility |
| Typical site | 3–10 Raspberry Pis holding gigabytes | Full-mesh gossip within a site; a site keeps its own shipped data cached | Networking, 6 |
| Workload | A mix from day one: streaming ingest with rollups, ad hoc SQL, ML features and inference | Standing queries and model functions are first-class | 7 |
| Relationship to Databricks | An edge tier feeding Databricks and a standalone alternative, equally | Delta Lake stays; Databricks is the refinery for harvested data | 6 |
| Where data lives | First on the Node that ingested it, then on the site's cluster-attached external drive, then harvested to a cloud object store | Crop on the Node, comb on the drive, harvest in the cloud | 4, 7 |
| Visibility before shipping | Queryable at once, flagged as not yet shipped | A `_stage` column (crop, comb, harvested) on every Frame and a count per stage in every result | 7 |
| A Node lost before shipping | Accept a short loss window | Crops deposit to the drive over the LAN every cadence, so the window stays short even during an outage | 7 |
| Local store | A cluster-attached external drive at every site | The site's comb lives on the drive; the cloud holds only harvested data | 6 |
| Offline periods | Days offline must be survivable | A site keeps ingesting, querying and ripening offline, and ships oldest first when the link returns | 8 |
| Networking default | Built in (iroh), no extra software | No overlay mode; relay mode is built into the Apiary binary | Networking |
| Cross-site emergence | Built in, with the planned path as fallback | Inter-colony dances discounted by WAN cost; a planned split when signals go stale | 5 |
| The cloud object store | Harvested honey: taken out of the hive before it is refined | Only capped data is harvested, and the site keeps enough on its drive to work through an outage | 4, 6 |
| Refining | Databricks or a similar platform turns harvested data into products for customers | Apiary ripens and caps; refining into products happens downstream of the harvest | 4, 6 |

## Background

The clean slate keeps the paper's goal and namespace, takes its lessons from V1, and rebuilds most of the machinery. The goal stands: one engine for batch, interactive, ingestion and iterative work, running on anything from a single Raspberry Pi to a cloud cluster, with edge and cloud nodes as peers.

The namespace stays because it is real hive anatomy and maps level for level onto Unity Catalog's three-level namespace: an Apiary holds Hives (catalogues), a Hive holds Boxes (schemas), a Box holds Frames (tables), and each Frame holds comb.

| Concern | Apiary paper | Clean-slate redesign | Why |
| --- | --- | --- | --- |
| Coordination | Gossip plus Raft | Gossiped dance floor, with the object store as fallback medium | No leader; ephemeral signals stay off the store |
| Commits | Raft-ordered Ledger | Delta Lake log via `delta-rs`, conditional put | Leaderless single winner; tables readable by Spark and Databricks |
| Query engine | DataFusion | DataFusion, with stages scheduled by the colony | Mature SQL, leaving the biology to scheduling |
| Data format | Per node: Arrow IPC, Parquet or CSV | Any format at the entrance; Parquet in the comb | Delta needs Parquet, and any Bee can then read any Cell |
| Data exchange | Not specified | Arrow IPC over QUIC between Nodes, the object store as fallback; Flight SQL at the entrance | Direct handoff when reachable, stigmergy when not |
| Python | PyO3 bindings | PyO3: an embedded single-node colony, and a client | A Pi user gets an in-process engine |
| Failure detection | SWIM | SWIM once there are peers; expiry of dances and claims everywhere | Detection speeds recovery; fencing keeps it safe |
| Backpressure | Alarm pheromone level | Tremble dance | The colony's actual forager-to-receiver regulator |
| Bee model | Twelve behaviours across seven species | Thirteen honeybee mechanisms plus sweat bee sociality | Each borrowing copies a mechanism |

V1 shapes the redesign through its lessons. It shipped by deferring Raft, SWIM and Flight and coordinating through object storage; the redesign keeps object-storage coordination as the fallback medium and builds in the same order, solitary first (§12). V1 also showed the value of benchmarking early and instrumenting before optimising, which shapes the gates in §12. V1's toolchain pins came from building on constrained hardware; the redesign cross-compiles for the Pi and drops them.

The paper's evaluation figures (§7 of the paper) are not carried forward. §12 lists what to measure instead.

## Feasibility review

The design is feasible for its first target, edge and IoT sites of three to ten Raspberry Pis holding gigabytes, with cloud machines joining when needed, after five adjustments: networking becomes a layer of its own, each colony is one network site, every commit rests on a safe create-if-absent, foraging across sites always weighs the cost of the WAN, and joins are planned to fit a Bee's memory. Without them it fails at the first NAT, the first Garage install and the first large join on a Pi.

### When small compute is a real alternative to a cloud vendor

Owned small machines win where load is steady, data is born at the edge, data must stay on premises, or moving data out of a cloud costs more than processing it in place. They lose on bursty jobs that need hundreds of cores for an hour, and on scans too large for local disks. The viable position is therefore hybrid by design: owned hardware carries the base load, cloud machines join as ordinary Nodes for bursts and leave afterwards, and data stays where it was born, with only reduced results crossing sites. Apiary competes with a cloud vendor by making the cloud one optional member of the colony.

The same engine has two equal roles: an edge tier whose capped data is harvested for Databricks to refine into products, and a standalone platform for organisations that would rather not run their analytics in a cloud at all.

### Findings and adjustments

| Area | Finding | Severity | Adjustment | § |
| --- | --- | --- | --- | --- |
| Reachability | Gossip and Flight assumed every Node could reach every other; home and mobile NAT, Kubernetes pod networks and Docker bridges break that on day one | High | Nodes dial each other by public key over QUIC, with hole punching and relay fallback, or over an overlay the operator already runs | Networking |
| Sites | One colony spanning a LAN and a cloud region would shuffle across slow uplinks and pay cloud egress on every exchange | High | A colony is one site; across sites, Bees forage only where the payoff beats the WAN cost, with planned fragments as the fallback | 4, Networking, 5 |
| Conditional writes | Delta commits, completion records and piping records all need put-if-absent; Garage's own documentation says it [cannot implement conditional writes](https://garagehq.deuxfleurs.fr/documentation/reference-manual/known-issues/) because it has no consensus algorithm | High | The site comb on the external drive gets create-if-absent from its host's local file system; the cloud harvest store must support conditional writes (AWS S3, R2 and MinIO do); Garage suits neither | 6 |
| Join memory | DataFusion's [hash join cannot spill](https://sedona.apache.org/sedonadb/latest/memory-management/) and fails when its build side exceeds the pool; a Pi Bee has well under a gigabyte | High | Joins are planned to fit a Bee: enough partitions for the build side to fit, and sort-merge join, which spills, when the build size is unknown or too large | 7 |
| Membership and trust | An internet-facing gossip mesh had no notion of who may join | High | Node identity is a key pair; Nodes join with a token signed by the colony key; all traffic is encrypted | Networking |
| Clocks | Expiry used a colony clock; a Pi 4 has no real-time clock and can boot without network time | Medium | Entries carry remaining lifetime, counted down on each Node's monotonic clock | Networking |
| Emergence on small queries | A one-off query with a handful of Patches gives dances nothing to learn from | Medium | Within a query, idle Bees pull the next Patch; dances allocate Bees across queries, stages and time, and learned profitability persists per Node and operator | 5 |
| Duplicate work | Soft claims over slow gossip duplicate short Patches | Medium | Claims stay inside one colony's fast network, and Patches are sized to outlast several gossip rounds | 5 |
| Oscillation | A model of the forager and receiver loop shows [jagged oscillations around the tremble threshold](https://arxiv.org/pdf/1007.3311); the bees' own loop is not perfectly stable | Medium | Threshold spread, hysteresis, a minimum dwell time in each role, and rate-limited signals | 8 |
| Commit throughput | Every Delta commit is a store round trip, and many small ingest commits to one Frame contend | Medium | Each Node batches deposits per Frame on a cadence; blind appends retry without conflict | 7 |
| Storage hardware | SD cards are slow and wear out under constant writes | Medium | Pi Nodes keep crop, cache and spill on an SSD, and the comb lives on the cluster-attached drive | Networking |
| Elasticity | A colony cannot create compute | Medium | Where the platform can (Kubernetes, cloud), a small operator adds and removes Nodes from colony temperature | 9 |
| Predictability | Emergent placement makes performance vary and hard to explain | Medium | Guards cap Bees per query, and every placement is explainable from the marked-Bee traces | 8, 10 |
| Thermal throttling | Pis slow their CPUs when the SoC overheats under sustained load | Low | SoC throttling state is part of Node temperature | 8 |
| Long outages | Sites must survive days without an uplink: unshipped data piles up, a Pi 4 has no real-time clock and loses the date across reboots, and membership tokens can expire mid-outage | High | An external drive sized for days to weeks of ingest; certificates that outlast any expected outage; the Pi 5 clock battery at sites that expect long outages | 7, 8, Networking |
| Cross-site emergence | Signals between sites are slow, cost money, and go stale during outages, so soft claims and fast recruitment do not carry over | High | Dances discounted by WAN cost, hard claims across sites, and the planned split whenever summaries go stale | 5 |
| Loss window | A short loss window grows with every hour that shipping is stalled | Medium | Crops deposit to the external drive over the LAN, which an uplink outage never stalls | 7 |
| One drive | The external drive is one physical device, and the Pi it hangs off is one machine | Medium | A mirrored pair of drives; if the host Pi dies, crops keep ingesting until the drive moves to another Pi; harvested data is safe in the cloud | 6 |

### Where emergence belongs

Emergence earns its keep where signals are cheap and fast against the length of a task, where the same kind of decision recurs many times, and where a wrong guess costs only duplicate work. Inside a site all three hold. Between sites they hold only for work whose payoff beats the cost of the WAN, so cross-site foraging weighs that cost into every dance and falls back to planned fragments when signals between sites go stale. Honeybee coordination stops at the nest, so the cross-site half has no biological precedent (§11).

| Decision | How it is made | Why |
| --- | --- | --- |
| Which Bee runs which Patch in a colony | Emergent: pull within a query, dances across queries | Fast signals, many decisions, cheap mistakes |
| Role mix on a Node | Emergent: response thresholds | Purely local |
| Backpressure and load shedding | Emergent: tremble and stop signals | Local measurements, fast feedback |
| Data tiers within a colony | Emergent: comb pattern rules | Slow, cheap to correct |
| Which colony runs a query fragment | Emergent: inter-colony dances discounted by WAN cost; a planned split by home colony when signals are stale | WAN cost sits inside every payoff |
| Moving data between colonies | Quorum decision | Rare, costly, must not split |
| Order of commits | Delta conditional put | Correctness |
| Starting, restarting and scaling Nodes | The platform: systemd, Docker, Kubernetes, cloud autoscaling | Apiary cannot create machines |

## Biological foundations

Apiary copies thirteen honeybee mechanisms and one sweat bee mechanism, each used only where it solves the same problem in the engine. The honeybee colony is the right model because it allocates a fixed workforce across patchy, shifting resources with no central control and no individual holding the whole picture.

Six colony rules govern every mechanism in this document:

1. **Local information only.** No Bee reads a colony-wide aggregate. Leoncini and colleagues describe bees acting on fragmentary information in ways that suit the state of the whole colony ([Leoncini et al. 2004](https://pmc.ncbi.nlm.nih.gov/articles/PMC536028)).
2. **Individual variation is a control mechanism.** Genetically diverse colonies hold brood temperature steadier because workers' response thresholds differ, which prevents excessive colony-level responses ([Jones et al. 2004](https://pubmed.ncbi.nlm.nih.gov/15218093/)).
3. **Every positive feedback has a negative partner.** Waggle dances recruit; tremble dances, stop signals and dance decay damp.
4. **Self-organisation works best with templates.** The comb's storage pattern emerges from local rules, and is steadier when simple templates such as gravity and the queen's position bias it ([Johnson 2009](https://pmc.ncbi.nlm.nih.gov/articles/PMC2674341/)).
5. **Pay for coordination only where it pays.** Dance recruitment helps most when food patches are few, poor and variable ([Dornhaus et al. 2006](https://experts.arizona.edu/en/publications/benefits-of-recruitment-in-honey-bees-effects-of-ecology-and-colo/)); sweat bees live solitarily where the season is too short for a social colony.
6. **Irrevocable decisions need a quorum, not a leader.**

| Colony mechanism | What the bees do | Engine problem | Apiary mechanism | § |
| --- | --- | --- | --- | --- |
| [Response thresholds with reinforcement](https://pmc.ncbi.nlm.nih.gov/articles/PMC1688885) | A worker takes a task when its stimulus passes her own threshold; doing a task lowers that threshold, neglecting it raises it | Allocate cores to task types with no scheduler | Per-Bee thresholds drawn from a spread and reinforced by practice | 4 |
| [Ethyl oleate inhibition](https://pmc.ncbi.nlm.nih.gov/articles/PMC536028) | Foragers pass an inhibitor by mouth-to-mouth feeding that delays younger bees from starting to forage | Hold the forager share steady as nodes come and go | Inhibitor term from the count of active foragers | 4 |
| [Scouts and recruits](https://dx.doi.org/10.1007/BF00290778) | About 5–35% of foragers scout independently, depending on forage availability | Balance finding new work against exploiting known work | Scout share rises as advertised work falls | 5 |
| [Waggle dance](https://extensionentomology.tamu.edu/wp-content/uploads/sites/5/2016/10/Honey-Bee-Biology-Part-2.pdf) | Run angle codes direction, run duration codes distance, run count codes quality; followers [rarely compare dances](https://pmc.ncbi.nlm.nih.gov/articles/PMC1693068/) | Recruit idle cores to the best work with constant-time choices | Dance records whose circuits scale with profitability; followers sample by circuits | 5 |
| [Expiration of dissent](https://www.doi.org/10.1007/S00265-003-0598-Z) | Scouts dance fewer runs on each return, in a strikingly linear decline | Expire stale advertisements with no collector | Linear circuit decay per renewal | 5 |
| [Comb pattern formation](https://link.springer.com/doi/10.1007/BF00172140) | Brood centre, pollen rim and honey periphery emerge from deposit and removal rules | Place hot, warm and cold data with no tiering policy | Deposit and consumption rules per Cell, plus a recency template | 6 |
| [Ripening and capping](https://news.ncsu.edu/2013/06/how-do-bees-make-honey) | Nectar near 70% water is dried below 18.6% and sealed with wax | Turn raw writes into query-ready, immutable data | Uncapped, ripening and capped Cell states | 7 |
| [Tremble dance](https://beeculture.com/?p=45494) | An unloading wait under 20 s leads to waggle dancing; over 50 s to tremble dancing, which recruits receivers and [inhibits waggle dances](https://link.springer.com/article/10.1007/BF00216597) | Backpressure between pipeline stages that adds capacity downstream | Handoff-wait thresholds drive tremble signals | 8 |
| [Stop signal](https://news.cornell.edu/node/271186) | A short buzz with a head butt that damps a dancer; aimed at dangerous food sources and at rival nest sites | Shed load from failing sources; break ties | Stop signals against a Patch group or a rival proposal | 8, 9 |
| [Thermoregulation](https://pubmed.ncbi.nlm.nih.gov/15218093/) | Brood held near 35 °C by heating and fanning bees with differing thresholds | Keep node load in band without a global metric | Local temperature, spread thresholds, winter cluster | 8 |
| [Quorum sensing](https://www.doi.org/10.1007/S00265-003-0664-6) and [piping](https://www.shoalsmarinelaboratory.org/sites/default/files/media/2026-02/Visscher%26Seeley%202007_SML136.pdf) | Worker piping begins once 10–15 or more scouts gather at one site; the swarm warms, then lifts off | Make irrevocable choices with no leader | Quorum, piping and liftoff for migrations | 9 |
| [Undertakers](https://link.springer.com/article/10.1007/s00040-020-00789-y) | Around 1–2% of workers remove corpses, cued by the [loss of the chemical signs of life](https://theapiarist.org/the-scent-of-death/) | Remove orphaned Cells and dead claims | Undertaker role triggered by expired claims and unreferenced files | 9 |
| [Sweat bee social polymorphism](https://sussex.ac.uk/broadcast/read/5830) | One species lives solitarily where summers are short and socially where they are long | Pay for coordination only when a deployment can use it | Solo, semi-social and eusocial modes | 9 |
| [Trophallaxis as a signal channel](https://pmc.ncbi.nlm.nih.gov/articles/PMC536028) | Mouth-to-mouth food exchange between pairs of bees also carries signals, such as ethyl oleate, through the colony | Spread signals between nodes with no broadcast | Gossip: dances and signals pass between pairs of nodes | 4, 9 |

Parts with no bee mechanism behind them get plain names (Node, catalogue, Delta log) or hive-anatomy names that claim nothing (Hive, Box, Frame, Comb). §11 lists them.

## Architecture overview

A colony is the set of Nodes on one network site, each contributing one Bee per core, coordinating through two media: the comb store, which is durable, and the dance floor, which fades. No Node holds a role the others lack, and any Node's entrance accepts work.

&#91;embedded content: Apiary colony architecture · Beekeeper, Nodes, dance floor, comb store\]

The Beekeeper reaches any Node's entrance and never a Bee. Nodes signal each other on the fading dance floor, pass batches directly over QUIC, and keep everything that must last in the comb store.

### Two media

Anything that must survive a crash goes in the comb; anything that should fade goes on the dance floor. Bees work the same way: stores live in wax, and dances are never written down.

| Medium | Colony counterpart | Holds | Transport | Lifetime |
| --- | --- | --- | --- | --- |
| Comb store | Wax comb | Delta tables (logs and Cells), query plans, durable stage outputs, quorum decisions | `object_store`: the site's cluster-attached external drive, served by the Node it is plugged into (§6) | Until removed |
| Dance floor | The dance floor and trophallaxis contacts | Dances, soft claims, tremble, stop and heat signals, presence, temperature | In memory within a Node; gossip between Nodes; a comb-store prefix when Nodes cannot reach each other | Seconds to minutes; every entry expires |

Each Node holds its own copy of the floor, which is never complete. An entry is keyed by kind, target, Node and Bee, and carries a version and an expiry. Merging keeps the highest version per key and drops expired entries, a state-based CRDT, so the order in which gossip arrives does not matter. A Bee sees only what has reached its Node, which is rule 1 enforced by construction.

### The crop: data before it ships

A forager carries nectar home in her honey crop and hands it over in the hive; if she dies in the field, the load is lost and the colony carries on. Apiary's crop works the same way. Data lands first on the SSD of the Node that ingested it, is queryable there at once, flagged as not yet shipped, and moves into the comb on the site's external drive every cadence, over the LAN. The crop is durable on one Node only, so it is the one place where a Node's death can lose data (§7).

### Hive, harvest and refinery

Data passes through four places, and only the first two are inside the hive.

1. **Crop.** The SSD of the Node that ingested it, for at most one cadence.
2. **Comb.** Delta tables on the site's cluster-attached external drive, where data ripens and is capped. These are the site's stores, kept long enough to work through an outage.
3. **Harvest.** Capped data taken out of the hive to a cloud object store, as Delta tables. Beekeepers take only capped honey and leave the colony enough to see it through winter; Apiary harvests only capped Cells and keeps the site's retention window on the drive.
4. **Refinery.** Databricks, or a similar platform, refines harvested data into what customers buy: curated tables, dashboards, features and models. For a standalone deployment with no such platform, an Apiary colony in the cloud can take this part.

### Vocabulary

The Kind column says whether a name copies a mechanism, names a part of a hive, comes from beekeeping, or is plain.

| Term | Meaning | Kind |
| --- | --- | --- |
| Apiary | One deployment: its catalogue and one or more colonies | Anatomy |
| Hive, Box, Frame | Catalogue, schema, table | Anatomy |
| Comb | A Frame's stored data: its Delta log and Cells | Anatomy |
| Cell | One Parquet data file | Anatomy |
| Comb store | The site's cluster-attached external drive, which holds its comb | Plain |
| Node | One Apiary process on one machine | Plain |
| Bee | One execution slot: a core and its share of memory | Mechanism (§4) |
| Dance floor | The gossiped, fading medium | Mechanism (§5) |
| Patch | One claimable task: one partition of one stage | Mechanism (§5) |
| Nectar | Committed data not yet ripened | Mechanism (§7) |
| Capped Cell | Ripened data in its final layout | Mechanism (§7) |
| Entrance | Where queries and deposits arrive at a Node | Mechanism (§7) |
| Meadow | External sources the colony reads but does not own | Plain |
| Beekeeper | A person or client program | Principle, below |
| Colony | The Nodes on one network site, sharing a dance floor and a comb store | Plain |
| Crop | Data landed on a Node's SSD and not yet shipped to the comb store | Mechanism (§7) |
| Harvest | Capped data taken out of the hive to a cloud object store | Beekeeping (§6) |
| Refinery | Databricks or a similar platform that turns harvested data into products for customers | Beekeeping |
| Comb host | Whichever Node the external drive is plugged into | Plain |

A beekeeper adds boxes, inspects frames and harvests honey, and never tells a bee what to do. Apiary's Beekeeper submits queries and deposits at any Node's entrance (Python, Flight SQL or HTTP) and collects results. Nothing in the API assigns work to a Node or a Bee.

### Nodes and Bees

A Bee is one core plus a reservation from the Node's DataFusion memory pool, with no oversubscription: a Raspberry Pi 4 contributes four Bees and a 32-vCPU cloud machine contributes thirty-two. A Bee runs one Patch at a time.

### Roles, chosen by thresholds

Every Bee holds one role at a time and reconsiders it only between Patches, as a worker finishes one activity before taking up another. No Bee or Node assigns another's role.

| Role | Colony counterpart | Engine work | Local stimulus |
| --- | --- | --- | --- |
| Forager | Employed forager | Run a claimed Patch: scan Cells or Meadow sources, run operators, hand off results | Profitability of the Patch group it holds |
| Follower | Unemployed forager on the dance floor | Sample a dance and claim a Patch from it | Live dances in its Node's floor copy |
| Scout | Scout | Plan new queries, find unadvertised or abandoned Patches, ripening candidates, new Meadow files | Scarcity of advertised work |
| Receiver | Nectar receiver | Accept handed-off batches: exchange, aggregation and write stages | Tremble signals and local handoff wait |
| Ripener | Processor and fanning house bees | Sort, deduplicate, compact, cap and harvest Cells | Uncapped share of nearby Combs |
| Undertaker | Undertaker | Remove expired claims, unreferenced files and abandoned stage outputs | Expired entries and orphans it meets |
| Guard | Entrance guard | Validate deposits and queries: schema, credentials, admission rate | Arrivals waiting at its Node's entrance |

A Bee engages role *j* with the probability given by the response threshold model, where *s* is the stimulus it observes and θ is its own threshold for that role:

```latex
P_{ij} = \frac{s_j^{2}}{s_j^{2} + \theta_{ij}^{2}}
```

Thresholds are drawn per Bee from a log-normal spread, seeded by Node id and Bee index so the simulator replays exactly. The spread σ is the colony's genetic diversity: at σ = 0 every Bee on a Node switches at the same stimulus, the condition behind the less stable brood temperatures Jones and colleagues found in genetically uniform colonies. Performing a role lowers its threshold and neglecting it raises it, within bounds, so specialists emerge without configuration:

```latex
\theta_{ij} \leftarrow \mathrm{clamp}\left(\theta_{ij} - \xi\,\Delta t\,[\,\text{doing } j\,] + \varphi\,\Delta t\,[\,\text{not doing } j\,],\ \theta_{\min},\ \theta_{\max}\right)
```

The forager stimulus carries an inhibitor in the place of ethyl oleate. A Bee estimates the share of foragers, F̂, from the forager dances in its own Node's floor copy, never from a colony-wide count, and damps its foraging stimulus with it:

```latex
s'_{\text{forage}} = \frac{s_{\text{forage}}}{1 + \alpha\,\hat{F}}
```

When Nodes die and F̂ falls, house Bees start foraging sooner. When F̂ is high, new Bees stay in house roles longer. Age is counted in completed Patches: a Bee on a newly joined Node begins with ripening, receiving and short calibration Patches that measure its own scan rate and its latency to the comb store, and posts no dance until it has those measurements.

## Networking

Every Node is reachable by its public key wherever it runs. Apiary connects over QUIC directly on a LAN, punches through NAT where it can, and relays where it cannot, so a Pi behind a home router, a Kubernetes pod and a cloud VM join the same Apiary without inbound firewall rules on the Pi or the pod.

### Identity and membership

Each Node generates an ed25519 key pair on first start, and its public key is its Node id. The Apiary has a signing key held by the Beekeeper. A Node joins with a token signed by that key, naming the Apiary, the colony, an expiry and what the Node may do (run work, ingest, or read only). Peers accept a connection only from a key that carries a valid membership certificate: the entrance guard's colony-odour check, applied to Nodes. Revoked keys are gossiped and tokens expire. Because a site may be offline for days, membership certificates outlast any expected outage, and revocations reach an offline site when it reconnects. Comb-store credentials stay in the platform's secret store and never travel in a token.

### Transport

Nodes talk over [iroh](https://iroh.computer/docs/overview), a Rust QUIC library that dials by public key, encrypts and authenticates every connection with the Node keys, connects directly when it can, hole-punches through NAT, and falls back to relay servers. One connection per peer carries three protocols, separated by ALPN:

| Protocol | Carries | Notes |
| --- | --- | --- |
| Gossip | Dance-floor entries and SWIM probes | Small messages, frequent |
| Exchange | Arrow IPC streams between stages | Replaces Flight between Nodes; Flight SQL stays at the entrance for Beekeepers and BI tools |
| Control | Plan fetches, soft claims, inter-colony summaries | Request and response |

The relay server is self-hostable, and any Node with a public address can run one; in a hybrid Apiary a small cloud VM is the natural relay. Relayed paths are slow, which is one more reason the site rule (below) keeps them out of shuffles.

Apiary needs no VPN, overlay or extra daemon: transport, discovery and the relay all ship in the one binary, and an Apiary of a single site on one LAN needs no relay at all. The transport sits behind Apiary's own trait, so iroh can be replaced later without touching the colony.

### Discovery

| Where | How Nodes find each other |
| --- | --- |
| A LAN | mDNS announcements of Node id and colony, so Pis find each other with no configuration |
| Kubernetes | A headless Service lists peer pods; the key lives in a Secret, so a rescheduled pod rejoins as the same Node |
| Anywhere | The site's comb, or across sites the harvest bucket, as rendezvous: each Node writes its id and current addresses to `floor/<node>`, and any Node that can read the store finds its peers |
| First contact | The join token carries bootstrap peers or a relay address |

### Sites and colonies

A colony is the set of Nodes with fast, cheap links to each other: one LAN, one cluster network, one cloud region. Membership in a site follows Johnson's rule of self-organisation plus a template. The template is a site label in each Node's configuration; the self-organisation is measurement, as every Node records round-trip time, throughput and whether a path is direct or relayed for the peers it talks to. A declared label wins, and measurement fills in where none is given or flags a Node whose links contradict its label.

Each colony has its own dance floor, its own comb on a cluster-attached drive, and its own Frames, each with one home colony. With three to ten Pis, a site's gossip is a full mesh. Gossip never crosses sites at full rate: colonies exchange a low-rate summary of temperature, free Bees, Frames held and dances for work they cannot absorb, and Bees forage across sites only where the payoff beats the cost of the WAN (§5).

&#91;embedded content: Apiary network topology · two colonies, one WAN link\]

Gossip and shuffles stay inside each colony's network. The WAN carries colony summaries and their dances, the inputs and results of work foraged across sites, and relayed traffic when no direct path exists.

### WAN budgets

Each pair of colonies has a bandwidth budget, and a cloud colony also has a monthly egress budget, because cloud providers charge for data leaving their network. The planner rejects a plan that would ship unreduced data across sites unless the Beekeeper allows it for that query, and reports what it would have cost.

### Time

Dance-floor entries carry remaining lifetime, never a wall-clock expiry. The receiver subtracts half the measured round-trip time and counts down on its own monotonic clock, so the floor works on a Pi that booted without network time. Delta commit timestamps do use wall time, so a Node refuses to commit until its clock is synchronised. Crops need no wall time, so a site whose clock is wrong keeps ingesting and ships once the clock is right; sites that expect long outages fit the Pi 5's real-time-clock battery so a reboot keeps the date.

### Deployment by platform

| Platform | How a Node runs | Discovery | Reachability | Notes |
| --- | --- | --- | --- | --- |
| Raspberry Pi on a LAN | One `aarch64` binary under systemd | mDNS | Direct | SSD for crop, cache and spill; a cluster-attached external drive for the comb, ideally a mirrored pair; active cooling; Pi 5 clock battery for long outages |
| Docker or Compose | One container per host, host networking or one published UDP port | mDNS on the host network, or store rendezvous | Direct or hole-punched | Volume for key, cache and spill |
| Kubernetes | A StatefulSet, one pod per Node, key in a Secret | Headless Service | Pods dial out; relay or one UDP Service for inbound | `emptyDir` for cache and spill; the operator in §9 scales replicas |
| Cloud VMs | Binary or container, join token in user data | Store rendezvous or token bootstrap | One UDP port open, or relay only | Natural relay host; the harvest bucket sits in the same cloud |
| Pi site plus cloud | Two colonies in one Apiary | Per site, plus inter-colony summaries | Cloud VM runs the relay | Data stays in its home colony |

## Foraging and scheduling

Idle Bees find work as unemployed foragers do: each follows one dance chosen at random, weighted by how long it runs. Effort flows to the best work while no Bee ranks the options, so a scheduling decision costs the same in a colony of four Bees or four thousand.

### From query to Patches

A Guard admits a query at any Node's entrance and a Scout plans it. DataFusion parses and optimises it into a physical plan, which the Scout cuts into stages at every repartition boundary, as Ballista does, and serialises with `datafusion-proto` to the comb store. Each output partition of a stage is a Patch. Because the plan lives in the comb, any Node can run any Patch, and the query outlives the Node that planned it.

The Scout dances for the leaf stages. The Bee that finishes the last Patch of a stage dances for the next one; if she dies first, a Scout finds a stage whose inputs are complete and nobody advertises, and dances for it.

A query that touches Frames in more than one colony can always be split by home colony before any of this, with each colony foraging its own part and only reduced results crossing the WAN. That planned path is the fallback for cross-site foraging (below).

### Pull within a query, dance across queries

A one-off query with a dozen Patches is finished before a dance could recruit anyone. Within one query, idle Bees in the colony pull the next unclaimed Patch, nearest data first, much as a receiver takes the next forager waiting to unload. Dances do their work across queries, across the stages of long queries, and over time. Measured profitability per Node and per kind of operator persists between queries, so each query starts with what the colony learned from the last.

### Profitability

A Forager that finishes a Patch measures its profitability, π: useful rows produced per Bee-second, with fetch time counted as cost. It divides π by its own calibrated baseline, so a dance reports how good the work is against the dancer's normal pace. Without that, a cloud core's dances would drown every Pi's whatever the work.

### The dance

A dance entry on the floor carries what the waggle dance carries, translated:

| Waggle dance | Dance entry field |
| --- | --- |
| Direction and distance to the patch | Query, stage, Patch group, where its input lives (Cells, Node caches, Meadow) |
| Number of waggle runs | Circuits, c, proportional to normalised profitability |
| The flower's scent on the dancer | Columns read and the Cells' statistics |
| The dancer herself | Node id, Bee id, posting time |

Circuits start at c₀ = ⌈k · π / π\_baseline⌉, capped. Each time the dancer finishes another Patch from the group she renews the dance one step lower, the linear decline of Seeley's expiration of dissent. Between renewals the entry decays with age, so a dead dancer's dance fades with no one deleting it:

```latex
c_{\text{eff}}(t) = \max\!\left(0,\ c_0 - \delta\, n - \frac{t - t_{\text{last}}}{\tau}\right)
```

*n* counts renewals and *t*\_last is the last one. Good work stays advertised because many Bees keep returning to it and each posts her own dance.

### Following, soft claims and duplicate work

A Follower samples at most k dances from her Node's floor copy and picks one with probability proportional to c\_eff, never sorting the floor or comparing two dances side by side. She picks a random unclaimed Patch in the group, posts a soft claim, and starts at once. Random choice within a group spreads Followers across Patches; Weidenmüller and Seeley asked whether imprecise dances for nearby food do the same job in the colony.

Two Bees on different Nodes can claim one Patch before gossip reaches them both, as two foragers can land on one flower. That is allowed. Each Patch's output is committed once, by conditional create of its completion record in the comb store, so the first finisher wins and the second discards her work. The price is duplicate work, bounded by gossip delay against Patch length. When a Node measures a high duplicate rate (short Patches, slow gossip), its Followers wait one gossip round after claiming and yield to the lower Node and Bee id. Patches are sized to run for several gossip rounds at least, and claims never cross sites, so WAN latency never enters the race.

The same rule handles stragglers. A Scout that sees a claimed Patch running well past its group's median re-advertises it, a second Bee runs it, and whichever finishes first is committed. Speculative execution falls out of the foraging model with no extra machinery.

### Scouting

Scouts find work nobody advertises: newly submitted queries, stages ready to run, Patches whose dancers died, uncapped data for Ripeners, and new files in the Meadow. Their stimulus rises as advertised work falls, so scouts are many when the floor is quiet and few when it is busy. Seeley measured scout shares of about 5% to 35% of foragers; Apiary starts with that range as bounds and tunes it in the simulator.

### When not to dance

Dancing costs gossip bandwidth and the colony gains from it only when work is patchy. A Bee dances only when the coefficient of variation of profitability across her last sample exceeds a threshold. On uniform work nobody dances, and Followers take Patches in plan order, which is the cheapest correct schedule when every Patch pays the same.

### Several queries at once

Queries compete as flower patches do, through dance strength. The Beekeeper can set a query's priority, which multiplies c₀ as a richer nectar earns a longer dance. No query gets a reserved share of Bees, and Guards cap how many any one query may hold, so a heavy query cannot drain the colony.

### Foraging across colonies

Cross-site work is emergent by default. A colony whose Patches are waiting while its Bees are busy puts dances for that work into its summary. Scouts in other colonies weigh each such dance by what it would cost to reach the work from where they are:

```latex
\pi_{\text{remote}} = \frac{r}{t_{\text{bee}} + b_{\text{in}}/B + \text{RTT}} \cdot \max\!\left(0,\ 1 - \frac{b_{\text{in}} + b_{\text{out}}}{W_{\text{left}}}\right)
```

*r* is useful rows, *t*\_bee the Bee time, *b*\_in the bytes the Patch must pull across the WAN, *B* the measured bandwidth, *b*\_out the result bytes sent back, and *W*\_left what remains of the WAN budget between the two colonies. A visiting colony takes the work only when this payoff, normalised, beats the best dance on its own floor, so payoffs fall as the budget is spent.

The effect is that compute-heavy work on small inputs travels well and raw scans do not. Model inference and wide aggregations over data a site has already harvested sit close to any colony in the cloud, which can forage them while the site is busy or offline. Scans of data still in the hive sit behind a home uplink and almost never pay to move. Harvesting changes who is near the data.

Claims across sites are hard: a Bee claims a foreign Patch by conditional create of a claim record in the home colony's comb store, because WAN gossip is too slow for soft claims, and the home colony stops dancing for a claimed Patch. When a colony's summaries from another site are older than a staleness bound, it treats that site as unreachable, and every query touching it runs on the planned path.

## Comb: storage layout and memory

Each Frame's comb is a Delta Lake table in its home colony's comb, on the site's cluster-attached external drive, written through `delta-rs`, so commits need no lock service and no leader, and Spark or Databricks can read whatever the colony stores. On top of Delta the colony adds one standard Cell size and a storage pattern that sorts hot, warm and cold data the way a colony sorts brood, pollen and honey.

### Comb store layout

```
<hive>/<box>/<frame>/_delta_log/...           Delta log: the commit record
<hive>/<box>/<frame>/<cell>.parquet           Cells, uncapped and capped
entrance/<node>/<deposit>.*                   raw deposits in any format (§7)
plans/<query>                                 serialised physical plans (§5)
outputs/<query>/<stage>/<patch>/...           stage outputs and completion records
results/<query>/...                           results for the Beekeeper
swarm/<decision>/...                          quorum decisions (§9)
floor/<node>                                  fallback dance floor when gossip cannot reach (§9)
```

### Commits

`delta-rs` commits a version by conditional put of the next log entry, so whichever writer creates it first wins and the loser re-reads and retries if the two writes do not conflict. S3 has offered put-if-absent since 2024, and `delta-rs` [documents R2 and MinIO as supporting conditional puts](https://delta-io.github.io/delta-rs/integrations/object-storage/s3-like) for safe concurrent writes without DynamoDB. On local disk the same rule rests on atomic file creation. Snapshot isolation and time travel come with Delta.

This makes create-if-absent a hard requirement wherever a Delta log lives, because Delta commits, Patch completion records and quorum piping records all rest on it. At a site, the comb host (the Node the drive is plugged into) gets it from its own local file system and serves the drive to the other Nodes over the colony's QUIC connections, so the site runs no extra storage software. Network file shares are not used for the comb in the first release, because whether create-if-absent holds over them depends on server and client settings. The harvest bucket in the cloud must support conditional writes; AWS S3, R2 and MinIO do. Garage [cannot implement conditional writes](https://garagehq.deuxfleurs.fr/documentation/reference-manual/known-issues/) by design, so it suits neither role.

### The comb host and its drive

A hive has one comb and a cluster has one drive. The comb host is whichever Node has the drive attached: the role follows the hardware, nobody appoints it, and the host's Bees forage like any others. If the host Pi dies, the drive still holds every committed version; Nodes keep ingesting into their crops while it is moved to another Pi, which becomes the host when it starts. While the host is down, the loss window stretches back to each Node's last deposit. A drive failure loses only what has not yet been harvested, so a site that cannot accept that runs a mirrored pair of drives. A NAS that serves S3 with conditional writes can stand in for the host and drive together.

Apiary records a Cell's ripeness in the tags of its add action (`apiary.state` = `nectar` or `capped`). Other engines ignore the tags and see an ordinary Delta table.

One caveat shapes the ownership rule. The `delta-rs` maintainers note that Delta on Spark [does not yet interoperate properly with conditional-put writers](https://github.com/delta-io/delta-rs/discussions/4482). Apiary therefore owns the tables it writes, and external engines read them; shared writing is an open question (§12).

Each Frame has two Delta tables: the site table on the drive and the harvest table in the cloud bucket, which receives capped Cells only. Harvest tables can be registered in Unity Catalog as external tables. That is how Apiary feeds Databricks: the refinery reads the harvest, and Apiary stays its only writer.

### One standard Cell size

Workers build comb to a standard cell size so that any worker can use any cell. Apiary does the same, which the paper's per-node Cell sizing did not: a cloud node could write Cells no Pi could hold. The colony's target Cell size follows its smallest Bee budget:

```latex
\text{cell}_{\text{target}} = \mathrm{clamp}\left(f \cdot \min_{b \in \text{live Bees}} \text{budget}_b,\ \text{cell}_{\min},\ \text{cell}_{\max}\right)
```

The fraction *f* leaves room for operator state. Large Bees take Patches of several Cells. Row groups are sized so a small Bee can stream any Cell row group by row group. When a smaller Node joins, existing Cells stay as they are and Ripeners use the new target from then on.

### Brood, pollen and honey: the storage pattern

Camazine showed that the concentric comb pattern needs only three rules: the queen lays near the centre, workers deposit food wherever there is room, and food is removed faster near the brood. Johnson later showed the pattern holds steadier when a template, gravity, biases it. Apiary's tiers follow the same recipe.

| Comb region | Apiary tier | Deposit rule | Removal rule |
| --- | --- | --- | --- |
| Brood nest | A Node's local cache of capped Cells, decoded to Arrow IPC and memory-mapped | A Cell a Bee fetches enters the cache if there is room | Under pressure, evict the Cell with the lowest decayed local read rate |
| Pollen rim | The comb on the site's external drive | Every commit lands here | Moved out when the template ages it and no heat renews it |
| Honey periphery | The harvest in the cloud, once a Cell is past the site's retention window | Copied out at harvest; removed from the drive by Undertakers once past retention | Read across the WAN when needed, and cached at the site again if heat persists |

The template is recency: a capped Cell drifts outward with age as honey drifts upward with gravity, and reads pull it back. Heat is a dance-floor entry each Node gossips per Comb when it reads from it, so no Bee ever sees a colony-wide access table. Retiring a harvested Cell from the drive is one remove in the site table; the harvest table keeps it. The paper's Arrow IPC on Pis returns here as a cache format.

Beekeepers harvest the surplus from the honey supers at the top of the hive, which is where the comb pattern puts the honey and Apiary's recency template puts the oldest data, and leave the colony enough to see it through winter. The retention window is that reserve: the drive keeps enough recent data to answer the site's own queries through an outage, and everything older lives in the harvest.

The brood cache also makes locality emerge. A dance says where its input lives, and a Follower on the Node that already caches those Cells sees a shorter flight and a higher payoff, so work drifts to its data with no placement rule.

At gigabyte scale the drive holds the site's retention window with room to spare, and the brood caches hold its hottest part, so an uplink outage takes away only the harvest and data older than the window.

### Memory per Bee

Each Node runs one DataFusion memory pool, sized to the machine's memory less a reserve, and each Bee runs its Patch under a reservation capped at its share. Operators that would exceed it spill to local disk, or to the comb store on a Node with no disk to spare. Capped Cells never change, so Bees on a Node share the brood cache behind reference counts with no locks.

## Nectar to capped stores: writes and queries

Writes follow the honey flow: data lands in the crop of the Node that ingested it, is queryable there at once, ripens on the Node while it waits, is deposited in the comb on the site's drive every cadence, and is harvested to the cloud once capped. Queries read crops and comb alike, and say how much of their answer has not yet shipped.

### The write lifecycle

1. **Arrival.** Data arrives at any Node's entrance: a Python call, an Arrow Flight `DoPut` stream, or an MQTT topic the Node subscribes to. A continuous stream is a nectar flow, cut into batches by size or time.
2. **Guarding.** A Guard checks the batch's schema fingerprint against the Frame, as entrance guards check a returning bee's colony odour. A match is admitted. New columns become a schema evolution if the Frame allows it; anything else is set aside with a reason.
3. **Landing in the crop.** The Guard appends the batch to the Node's crop: Arrow IPC files on the local SSD and an append-only local log, with no network involved. The rows are queryable at once and read `_stage = 'crop'`.
4. **Ripening on the Node.** While data waits in the crop, the Node's Ripeners work it. Invertase splits sucrose into two simple sugars; ripening splits compound fields (nested JSON, delimited strings) into the Frame's atomic columns. Evaporation removes water; ripening removes bulk: duplicates on a declared key, poor encoding, small files. The Ripener sorts by the Frame's sort key and builds Parquet Cells. Bees begin [turning nectar into honey in flight](https://www.discoverwildlife.com/animal-facts/insects-invertebrates/how-bees-make-honey), before they reach the hive; ripening before deposit does the same, so the comb receives fewer, better files.
5. **Deposit in the comb.** Every cadence, the Node writes its ripe Cells to the drive through the comb host and commits them to the Frame's site table, oldest first. This crosses only the LAN, so an uplink outage never stops it. Appends from different Nodes do not conflict, so `delta-rs` retries them without aborting. The rows then read `_stage = 'comb'`, and the crop drops them once the commit is confirmed.
6. **Capping in the comb.** Cells from different Nodes arrive as separate small sets. Ripeners merge them to the standard size and cap them in one commit marked as no data change, so streaming readers skip it. A capped Cell never changes again; deletes and updates write new Cells and retire old ones.
7. **Harvest.** Whenever the uplink is up, Bees copy capped Cells to the harvest bucket and commit them to the Frame's harvest table, oldest first and paced against the WAN budget. Only capped Cells are harvested, as only capped honey is taken from a hive: uncapped nectar ferments, and uncapped data would hand the refinery small, unsorted files. In a real apiary the beekeeper harvests; here Bees do it, on the beekeeper's schedule and retention rules.
8. **Clearing.** Files removed by capping, and harvested Cells older than the site's retention window, leave the drive when an Undertaker deletes them. A Cell that has not been harvested never leaves the drive, however old.

### The loss window

A forager that dies in the field loses her load. Apiary accepts the same for the crop: if a Node dies before its next deposit, the rows it took in since its last deposit are lost. Deposits cross only the LAN, so the window is one cadence even when the uplink has been down for days. It widens only while the comb host is down, and closes again when the drive is back.

### Seeing where data is

Every Frame carries a virtual column, `_stage`: `crop` for rows read from a Node's crop, `comb` for rows in the site's comb, and `harvested` for rows read back from the cloud harvest. Every result also reports how many rows it used from each stage, and from which Nodes. A query sees every crop in its colony, because each Node lists its undeposited ranges on the dance floor.

### Moisture

Nectar is roughly 70% water and honey must be below 18.6% before bees cap it. Apiary's moisture is the unripened share of a crop or Comb, and it is the Ripener's stimulus: the wetter the data, the more Bees take up ripening. Ripening yields to user writes. If a capping commit conflicts with a delete or update on the same files, the Ripener aborts and posts a stop signal against that Comb (§8).

### The query pipeline

A query runs as DataFusion stages, and every stage boundary is a forager handing nectar to a receiver:

1. **Scan** at the flower: read the Patch's crop ranges, Cells or Meadow files with projection, pruning files by Delta statistics and row groups by Parquet statistics.
2. **Filter and project**, vectorised over Arrow record batches.
3. **Partial aggregate**, or the build and probe sides of a hash join.
4. **Exchange**: hand batches to Receivers by hash partition, as Arrow IPC streams over QUIC when the Receiver's Node is in the same colony, else through `outputs/` in the comb store.
5. **Final aggregate, sort and limit** on the Receivers.
6. **Harvest**: stream results to the Beekeeper over Flight, or write large results under `results/`.

Joins are planned to fit a Bee. DataFusion's hash join keeps its whole build side in memory and cannot spill, so the planner uses Delta statistics to choose enough join partitions that each build side fits the smallest Bee that may run it. When the build size is unknown or still too large, it chooses sort-merge join, which spills. A spilling hash join, which DataFusion contributors are working on, would retire the second case.

The Meadow is foraged and never ripened. Queries read external Parquet, CSV or Delta in place, and Scouts notice new files there, but the colony does not rewrite what it does not own.

### Workloads from day one

Three workloads run side by side from the first release.

| Workload | How it runs | Colony view |
| --- | --- | --- |
| Streaming ingest with rollups | Standing queries: a rollup is registered once and runs as a recurring Patch over each new crop batch on the Node that ingested it, keeping partial state in the crop and shipping rolled-up rows like any other data | A patch its Foragers return to every cadence, the [site fidelity](https://pmc.ncbi.nlm.nih.gov/articles/PMC5579100) foragers show to a good source |
| Ad hoc SQL over recent site data | Ordinary queries over crops, caches and comb | Pull within the query, dances across queries |
| ML features and inference | Features are SQL; models run as scalar or batch functions on ONNX Runtime through the `ort` crate, with model files distributed through the comb store | Compute-heavy work on small inputs, the kind that travels well across sites |

## Homeostasis: backpressure, load shedding and temperature

Three negative feedbacks keep the colony in balance, each acting on local measurements: the tremble dance matches producers to consumers at every stage boundary, the stop signal sheds load from failing sources, and thermoregulation holds each Node's load in a band. The tremble dance adds capacity where it is short before it slows anything down, which the paper's alarm-pheromone backpressure could not.

### Tremble dance: backpressure that recruits

A returning forager reads the colony's balance from one number, how long she waits to unload. Apiary's equivalent is the handoff wait *w*: how long a Bee holding a finished batch waits for a Receiver to take it. Within a Node it is time blocked on a bounded channel; over QUIC it is time held by stream flow control; through the comb store it is the age of the oldest unconsumed output in the partition she writes to.

Seeley's foragers waggle-danced after waits under 20 s and tremble-danced after waits over 50 s. Apiary keeps the shape and the 2.5 ratio, with *w*\_low set to the stage's median time to receive one batch:

| Handoff wait | Bee's response | Effect on the colony |
| --- | --- | --- |
| Below w\_low | Dance for the upstream Patch group, if profitable | More producers recruited |
| Between w\_low and 2.5 × w\_low | Neither | None |
| Above 2.5 × w\_low | Gossip a tremble signal for the stage | Bees that hear it gain Receiver stimulus; Followers damp upstream dances for that query |

Tremble sounds inhibit waggle dancing in the colony, and tremble signals do the same: a Follower weighing a dance multiplies its circuits by a factor that falls with fresh tremble signals downstream. [Work from Würzburg](https://opus.bibliothek.uni-wuerzburg.de/solrsearch/index/search/searchtype/authorsearch/author/%22Thom%2C+Corinna%22) found that tremble-dancing foragers were the ones that sometimes unloaded straight into cells instead of waiting for a receiver. A Bee past the tremble threshold does the same: she stops waiting on the stream and writes her batch to `outputs/` in the comb store.

Bounded channels and QUIC flow control remain the hard limit, like a comb with no empty cells. The tremble dance keeps the colony away from it.

### Stop signal: shedding a failing source

Bees use stop signals to [warn nestmates of danger at a food source](https://news.cornell.edu/node/271186), damping the dances that advertise it. A Bee that meets repeated trouble on a Patch group (timeouts, throttling, a corrupt file, a near miss on her memory reservation) gossips a stop signal against that group. Followers weigh the dance down by recent stop signals, and the dancer cuts her circuits faster at her next renewal:

```latex
c_{\text{weighed}} = c_{\text{eff}} \cdot e^{-\beta\, m_{\text{stop}}} \cdot \gamma^{\, m_{\text{tremble}}}
```

*m* counts unexpired signals and γ is below 1. Stop signals expire, so a source that recovers is foraged again without anyone clearing a blacklist; the paper's evaporating pheromones had this property and it is kept. When the comb store itself throttles, Bees gossip a stop signal against it, and every Bee that hears it slows her writes and listings, with jitter.

### Thermoregulation: load in a narrow band

A honeybee colony holds its brood near 35 °C with no thermostat: heating and fanning bees respond to the temperature they feel, each at her own threshold. Apiary drops the paper's colony temperature, an aggregate that needed colony-wide data, for a temperature each Node measures for itself:

```latex
T = \max\left(u_{\text{cpu}},\ \frac{m_{\text{reserved}}}{m_{\text{pool}}},\ \frac{q_{\text{local}}}{q_{\max}},\ \tau_{\text{soc}}\right)
```

*q* is the Node's queue of claimed but unstarted Patches, and τ\_soc is the SoC temperature as a fraction of the point where the board throttles, so a hot Pi cools itself by taking less work. Each Bee draws her own cooling threshold θ\_cool from a spread across the target band. Above it she stops claiming; below θ\_cool − *h* she resumes. Because thresholds differ, Bees drop out one at a time as a Node heats, and the load curve bends instead of sawing on and off. When a Node runs cold the same spread works the other way: Bees lower their foraging thresholds, prefetch Cells for claimed Patches, and take up ripening.

The simulator test is the Jones experiment in miniature: run one Node at σ = 0 and at σ > 0 under the same arrivals, and compare the variance of *T*.

Feedback loops built from these mechanisms can oscillate, and so can the bees' own: a model of the forager and receiver loop shows jagged swings in search time around the tremble threshold. Apiary damps every loop the same four ways: thresholds spread across Bees, hysteresis on every switch, a minimum dwell time before a Bee may change role again, and a cap on how often a Node may emit each kind of signal.

### Days offline, and the winter cluster

In winter a colony stops foraging outside and lives on its stores. A site that loses its uplink keeps far more of its life going, because it is built to run for days without one. Ingest continues into the crops, standing queries keep rolling up, ad hoc queries run over crops and the comb, and Ripeners keep ripening, so the backlog shrinks while it waits. The site's cross-site dances go stale, and other colonies stop foraging there (§5).

Harvest and cross-site work stop; deposits to the comb do not, because they cross only the LAN. The drive is sized to hold days to weeks of ingest at gigabyte scale, and Cells that have not been harvested stay on it whatever the retention window says. If it fills, Guards slow admission and tell the Beekeeper, and never drop data silently. When the link returns, harvest resumes oldest first, paced by the WAN budget so the backlog does not swamp the uplink. Patch outputs from the outage commit only if their completion records do not yet exist, and deposits carry ids, so one replayed twice is admitted once.

A single Node cut off from its own colony, behind a failed switch port, is the true winter cluster. It stops claiming, finishes in-flight Patches into its crop, keeps one Bee awake retrying with backoff, and keeps its entrance open.

## Colony lifecycle: failure, decisions and growth

The colony recovers from failure by noticing absent signs of life, and makes its few irrevocable decisions the way a swarm picks a new home: scouts inspect options for themselves, dance in proportion to quality, inhibit rivals with stop signals, and commit once enough independent scouts gather at one option.

### No queen

The queen lays the eggs and her pheromone tells the colony she is present; she does not direct work. No Apiary Node has a unique role. The only single-winner objects (the next Delta log entry, a Patch's completion record, a decision's piping record) belong to whichever Bee creates them first.

### Failure: absent signs of life

Undertakers recognise a corpse by the loss of the signs of life, never by a death announcement, and Apiary does the same. Every Node gossips a presence entry it keeps renewing, and SWIM (through the `foca` crate) probes peers in its colony directly and through others before declaring one dead; between colonies, a colony counts as alive while its summaries keep arriving. A dead Node's dances and soft claims simply expire, and Scouts re-advertise its Patches.

Detection speed affects only recovery time. A Node wrongly declared dead that later finishes its Patch loses the race for the completion record and discards its output, so false suspicion costs duplicate work and never a wrong result. Undertakers handle what does not expire on its own: Parquet files no Delta version references past retention, outputs and plans of finished or abandoned queries, and spool leftovers.

### Nest-site selection: quorum decisions

A few decisions are costly to reverse: moving a Frame to another comb store (edge to cloud, or back), changing the standard Cell size after a large change in membership, retiring a storage class, and splitting the colony. Each runs as a house hunt:

1. **Proposal.** A Scout gossips an option for the decision.
2. **Inspection.** Other Scouts inspect the option themselves (latency, free capacity, storage cost) and gossip a quality score. No Bee relays another's opinion.
3. **Advocacy.** Inspectors dance for their option with circuits proportional to quality, decaying linearly, which recruits more inspectors.
4. **Cross-inhibition.** An inspector committed to option A gossips stop signals against dances for option B, weighted by A's quality. Seeley and colleagues showed this breaks deadlock between equally good sites.
5. **Quorum.** A Scout who sees at least *Q* fresh inspections from distinct Nodes at one option writes the decision's piping record by conditional create in the comb store. Distinct Nodes count, because Bees on one machine share a network and their inspections are correlated.
6. **Piping.** Bees that see the piping record do the reversible preparation, copying files to the new location and verifying them, as swarm bees warm their flight muscles before take-off.
7. **Liftoff.** When preparation is complete, one Delta commit switches the Frame's location. The only irreversible step is a single atomic commit.

In swarms, piping began once 10–15 or more scouts were at one site, out of a few hundred scouts. Apiary starts with *Q* = min(10, ⌊N/2⌋ + 1) for *N* live Nodes, so a three-Node colony needs two. [Passino and Seeley's model](https://www.shoalsmarinelaboratory.org/sites/default/files/media/2026-02/Passino%26Seeley%202006_SML127.pdf) studied the quorum threshold's effect: lower quorums decide faster and err more, higher ones are slower and only slightly better, so *Q* is the colony's speed–accuracy dial. One swarm Seeley and Visscher watched reached quorum at two sites at once ([JEB](https://cob.silverchair.com/jeb/article-pdf/1882078/2020.pdf)); the conditional create of the piping record rules that out here.

### Swarming: when the colony is crowded

Colonies reproduce by swarming when they outgrow their cavity. When a comb store stays crowded (persistent stop signals against it) or gossip load on the floor keeps rising, Scouts propose moving some Boxes to a second comb store or splitting the colony in two, each serving part of the Apiary, and the move runs through the quorum process. The Beekeeper can still add a store or a colony by hand, as a beekeeper adds a box.

### Growing and shrinking: the beekeeper's hands

A colony cannot create machines, but Kubernetes and the clouds can. An optional operator reads each colony's temperature and free Bees from the inter-colony summary and changes a StatefulSet's replica count or a cloud autoscaling group's size. It decides only how many Nodes there are; which Bee does what stays emergent. Scale-in drains a Node: its Bees stop claiming, finish in-flight Patches and leave. A Node that vanishes mid-Patch loses nothing committed, only work that another Bee redoes. On a Pi site the beekeeper is a person, adding a board.

### Social modes: the sweat bee borrowing

*Halictus rubicundus* nests alone where the season is too short for two broods and lives socially where it is longer. Apiary takes the lesson that sociality must pay for itself, and switches mode on both colony size and season length, meaning the expected duration of the queued work against the cost of setting up gossip and QUIC connections:

| Mode | When | Coordination |
| --- | --- | --- |
| Solitary | One Node, including one embedded in a Python process | Floor in memory; comb on local disk or a store; no gossip |
| Semi-social | Up to a few dozen Nodes, as at every Pi site, or any size with a short season | Full-mesh gossip, so every Node hears every entry; QUIC exchange |
| Eusocial | Larger colonies, usually in a cloud region, with a long season | SWIM membership with partial views; entries spread from pair to pair and expire; QUIC exchange |

A Node that cannot reach its peers (NAT, a firewall, an intermittent link) keeps its floor in the comb store instead, reading and writing `floor/<node>`. That is slower and works wherever the store is reachable; it is V1's coordination model, kept as the fallback. Each Node chooses its mode from what it can see, with hysteresis so that one flapping Node does not toggle the colony. Mode is per colony, so a Pi site can run semi-social while a cloud colony runs eusocial.

## Rust implementation

Apiary is one Rust workspace on current stable Rust, edition 2024, built from the Arrow ecosystem: DataFusion executes, `delta-rs` keeps the comb, `object_store` reaches every store, `iroh` carries all Node-to-Node traffic, `arrow-flight` serves the entrance, `foca` runs SWIM, and PyO3 serves Python. Ownership enforces two colony invariants at compile time: one Bee holds a batch at a time, and only a ripe Cell can be capped.

### Crates

| Crate | Colony subsystem | Contents |
| --- | --- | --- |
| `apiary-core` | Shared language | Ids, errors, clock and RNG traits, configuration |
| `apiary-floor` | Dance floor | Entry map with versions and expiry; in-memory, gossip and comb-store transports |
| `apiary-colony` | Behaviour | Bees, roles, thresholds, dances, tremble and stop signals, temperature, quorum, social modes |
| `apiary-comb` | Comb | Crops on local SSD, Delta tables, Cell states and tags, ripening, depositing, capping and harvest; the comb host's drive service, an object\_store implementation over QUIC so delta-rs on any Node writes through the host; brood cache and retention |
| `apiary-plan` | Planning | DataFusion sessions, stage splitting, plan serialisation with `datafusion-proto` |
| `apiary-forage` | Foraging | Running one Patch under a Bee's memory reservation, Arrow IPC exchange over QUIC, spill, model functions on ONNX Runtime through ort |
| `apiary-entrance` | Entrance | Flight SQL server, `DoPut` and MQTT ingest, Guards, standing queries |
| `apiary-observe` | Observation hive | Deterministic simulator, fault injection, tracing |
| `apiary` | The Node binary | CLI, configuration, runtimes |
| `apiary-py` | The Beekeeper | PyO3 bindings for an embedded solitary Node and a remote client, built with `maturin` |
| apiary-net | Networking | iroh transport and built-in relay mode, membership certificates and join tokens, discovery (mDNS, headless Service, store rendezvous), site measurement, WAN budgets |
| apiary-operator | The beekeeper's hands | Optional Kubernetes operator (on kube-rs) that scales Nodes from colony temperature |

### Building for the Pi

Nothing compiles on a Pi. CI cross-compiles `aarch64` binaries, containers and Python wheels for 64-bit Raspberry Pi OS alongside `x86_64`, so the edge runs the same build as the cloud and the toolchain follows DataFusion's minimum supported Rust version.

### Threads: work in the hive, flights outside it

Each Node runs two Tokio runtimes. The CPU runtime has one worker thread per core and one Bee per thread; a Bee runs one DataFusion partition at a time under its memory reservation. The I/O runtime carries comb-store requests and all QUIC traffic. Keeping them apart means a slow flight to the store never stalls another Bee's computation, and a long scan never delays a gossip round.

### The dance floor

```rust
/// One entry on the dance floor. Merging keeps the highest version per key
/// and drops anything past its expiry, so arrival order never matters.
#[derive(Clone, Serialize, Deserialize)]
pub struct Entry {
    key: EntryKey,      // kind, target, node, bee
    version: u64,
    expires_at: Millis, // colony clock
    body: EntryBody,    // Dance, SoftClaim, Tremble, Stop, Heat, Presence, Inspection
}

pub trait FloorTransport: Send + Sync + 'static {
    fn publish(&self, entries: Vec<Entry>)
        -> impl Future<Output = Result<(), FloorError>> + Send;
    /// Reads only this Node's copy: rule 1, local information only.
    fn sample(&self, kind: EntryKind, k: usize, rng: &mut impl SeededRng) -> Vec<Entry>;
}
// InMemoryFloor (solitary), GossipFloor over foca (semi-social, eusocial),
// StoreFloor over object_store (the fallback when peers are unreachable).
```

### Bees and roles

```rust
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role { Forager, Follower, Scout, Receiver, Ripener, Undertaker, Guard }

/// One per CPU worker thread. Moved into its thread; never shared.
pub struct Bee {
    id: BeeId,
    reservation: MemoryReservation, // from the Node's DataFusion memory pool
    role: Role,
    thresholds: Thresholds,         // θ per role, from a seeded log-normal spread
    age: u64,                       // completed Patches
}

impl Bee {
    /// Called only between Patches: P(engage j) = s_j² / (s_j² + θ_j²).
    pub fn reconsider(&mut self, stimuli: &Stimuli, rng: &mut impl SeededRng) -> Role { /* ... */ }
}
```

### Cell states as types

```rust
pub struct Nectar; pub struct Ripe; pub struct Capped;
pub struct Cell<S> { meta: CellMeta, _state: PhantomData<S> }

impl Cell<Nectar> {
    /// Merges a Comb's Nectar into ripe Cells of the standard size.
    pub fn ripen(cells: Vec<Self>, recipe: &Recipe, bee: &mut Bee)
        -> Result<Vec<Cell<Ripe>>, RipenError>;
}
impl Cell<Ripe> {
    /// Returns the Cell unchanged if any ripeness check fails.
    pub fn cap(self, checks: &RipenessChecks) -> Result<Cell<Capped>, Cell<Ripe>>;
}

/// One Delta commit: remove the Nectar, add the capped Cells, no data change.
pub async fn commit_capping(table: &mut DeltaTable, out: Vec<Cell<Nectar>>,
    in_: Vec<Cell<Capped>>) -> Result<Version, CommitConflict>;
```

The only path to a capping commit takes `Cell<Capped>`, and the only path to that type runs the ripeness checks.

### Trophallaxis: handoff between Bees

Within a Node, stages pass Arrow record batches over bounded channels; a send moves ownership of the batch's reference-counted buffers with no copy. Between Nodes, QUIC streams carry the same batches as Arrow IPC. The sender times each send that has to wait; that time is the handoff wait *w* that drives the tremble dance (§8).

### The Beekeeper's Python

```python
import apiary

ap = apiary.connect("grpc://pi-01:50051")       # any Node's entrance
# ap = apiary.embedded(data_dir="/data/apiary")  # a solitary Node in this process

ap.sql("CREATE TABLE production.sensors.temperature (ts TIMESTAMP, temperature DOUBLE)")
ap.write("production.sensors.temperature", arrow_table)
ap.sql("SELECT avg(temperature) FROM production.sensors.temperature").to_arrow()
```

The three-part name is Hive, Box and Frame. Nothing in the API names a Node or a Bee.

### The observation hive

Seeley studied colonies in glass-walled observation hives with paint-marked bees. `apiary-observe` is Apiary's version: a virtual clock, a seeded RNG, an in-memory comb store with injected latency and throttling, and a simulated network (on the `turmoil` crate) that can drop, delay and partition gossip and exchange traffic, simulate NAT, and route chosen pairs through a slow relay. Every Bee is marked: each role change, dance, claim, signal and commit is traced with Node id, Bee id and virtual time, so any run replays exactly and each mechanism is tested against the behaviour it copies.

## Where the metaphor breaks

The biology governs performance decisions only: who works on what, where data sits, when to slow down and when to move. Correctness rests on primitives with no bee counterpart (Delta's commit protocol, completion records created once, idempotent deposits), because a colony can afford to lose foragers and spill nectar and a data engine cannot lose a committed row.

### Departures from the bees

- **Nodes do not share a nest.** Every bee leaves from the same hive, so a dance's distance means the same to every follower. Apiary's Nodes differ in latency and cache, so a Follower discounts a dance by her own measured fetch cost.
- **Timescales differ by orders of magnitude.** Seeley's thresholds are in seconds and minutes; Apiary's are in milliseconds to seconds. Only ratios and shapes are copied (the 2.5 ratio of tremble to waggle waits, linear dance decay, the 5–35% scout range), and every value is tuned in the observation hive.
- **The floor is cheap and the comb is not.** Gossip costs bandwidth; every comb-store request costs money and tens of milliseconds. Only durable things go in the comb, and the fallback floor in the store is rate-limited.
- **A colony grows by rearing bees; Apiary cannot.** People add Nodes. Swarming moves data and splits colonies, and creates no capacity.
- **Emergent behaviour is harder to debug than a scheduler.** The mitigation is the observation hive: deterministic replay, every Bee marked, every parameter exposed.

Apiary also has colonies on different sites forage for each other, which real colonies never do: a forager dances only for her own nest, and bees found in other hives are drifting by mistake or robbing. Cross-site foraging reuses the within-colony mechanisms at a scale the biology never reaches, treating each colony as one forager of a larger colony. It has no biological evidence behind it and earns its place only through its own gates (§12).

### Names with no mechanism

Hive, Box, Frame, Comb and Cell are hive anatomy and claim nothing. Node, comb host, Meadow and the Delta log are plain engineering. Harvest and refinery come from beekeeping, not biology: they describe what people do with honey once it leaves the hive.

### What happens to the paper's mechanisms

| Paper mechanism | Disposition | Reason |
| --- | --- | --- |
| Waggle dance task distribution | Kept, rebuilt (§5) | Adds normalised profitability, linear decay, random sampling, gossip transport, and a switch that turns dancing off on uniform work |
| ABC three-tier workers | Replaced (§4) | ABC is an optimisation heuristic; response thresholds come from the colony studies |
| Pheromone backpressure | Replaced (§8) | Alarm pheromone recruits defenders; tremble dances regulate forager-to-receiver flow. Evaporation is kept |
| Colony temperature | Replaced (§8) | A colony-wide aggregate breaks rule 1; each Node measures its own |
| Winter cluster | Kept, redefined (§8) | Now the response to losing the store or the peers |
| Scouts and orientation flights | Kept (§5), redefined (§4) | Scouts find abandoned work; orientation flights became calibration Patches |
| Raft for Ledger commits | Dropped | Delta's conditional put gives one winner with no leader |
| The Ledger | Replaced by the Delta log (§6) | Same pattern, and Spark and Databricks can read it |
| SWIM failure detection | Kept (§9) | Speeds recovery; completion records keep it safe |
| DataFusion | Kept (§5) | Stages are scheduled by the colony |
| PyO3 bindings | Kept (§10) | Embedded solitary Node and client |
| Format-agnostic Cells | Kept at the entrance (§7) | Parquet in the comb, so Delta and any Bee can read every Cell |
| Stingless bee tiered storage | Replaced (§6) | The honeybee comb pattern does the job within one species |
| Mason bee sealed chambers | Folded into capping (§7) | Capped Cells are sealed and never change |
| Leafcutter Cell sizing | Replaced (§6) | Per-node sizes broke any-Bee-any-Cell; one standard size restores it |
| Bumblebee buzz pollination | Dropped | Retries and stop signals already cover reluctant sources |
| Carpenter bee tunnelling | Dropped as biomimicry | Delta and Parquet statistics do the indexing |
| Sweat bee sociality | Kept, extended (§9) | Mode depends on season length as well as colony size |

## Build order, measurements and open questions

The build starts solitary and adds the colony one mechanism at a time, each behind a gate that must pass before the next step; the gates replace the paper's evaluation figures with tests to run.

### Build order

1. **Solitary engine with crop, comb and harvest.** DataFusion over a crop and a Delta comb on an external drive, the entrance (Flight and MQTT), the embedded Python Node, and ripening, deposit, capping and harvest on one Node. Gate: the SSB and TPC-H-derived suites and a sensor-ingest benchmark run on a single Pi 4 with an external drive and on one cloud machine, with baselines recorded before any colony code exists; a Node killed mid-ingest loses at most one cadence of rows; Databricks reads a harvest table registered in Unity Catalog.
2. **Networking, membership and the comb host.** iroh transport with built-in relay, join tokens, mDNS and store discovery, site measurement, and the drive served over QUIC. Gate: a test Apiary of a Pi behind a home router, a Docker host, a Kubernetes pod and a cloud VM forms the expected colonies with no extra software; every pair within a site connects directly; every cross-site pair connects directly or through the relay; a revoked key is refused; a Pi booted without network time keeps a working floor; every Node in a site commits to the drive through its host.
3. **Observation hive.** Gate: the solitary engine runs unchanged against the simulated store, clock and network, and any run, including its NAT, relay and outage paths, replays exactly from its seed.
4. **Bees, roles and thresholds; capping in the comb.** Gate: the Jones test, where σ > 0 gives lower variance in Node temperature than σ = 0 under the same arrivals; no capping commit overrides a concurrent user write; the commit cadence keeps a streaming Frame's commit rate within budget.
5. **Semi-social colony: gossip floor, pull, dances, soft claims, QUIC exchange, standing queries, model functions.** Gate: on a simulated Pi colony running rollups, ad hoc SQL and inference together, dances beat pull alone; the duplicate-work rate stays under an agreed bound; TPC-H-derived joins complete within a Pi 4's per-Bee budget with no memory failures.
6. **Tremble and stop signals, with damping.** Gate: with an injected slow Receiver stage, tremble signals recruit Receivers and the handoff wait falls back below 2.5 × w\_low; with injected store throttling, Followers move away and return once the signals expire; role counts show no sustained oscillation after a step change in load.
7. **Days offline.** Gate: a simulated site cut off for three days keeps ingesting, rolling up, depositing and answering queries over crops and the comb; the drive does not fill at the target ingest rate; when the link returns the site harvests its backlog within its WAN budget; a Pi killed during the outage loses at most one cadence of rows; unplugging the comb host stops no ingest, and the drive comes back on another Pi with every committed version.
8. **Brood, pollen and honey tiers.** Gate: brood-cache hit rate at least matches plain LRU on replayed access traces, and a Cell retired from the drive stays readable from the harvest.
9. **Cross-site foraging.** Gate: on simulated pairs of a Pi colony and a cloud colony, emergent cross-site foraging completes mixed workloads faster than the planned split without exceeding WAN budgets; it falls back to the planned split within one staleness bound when the link drops; no foreign Patch commits twice.
10. **Eusocial mode, fallback floor and operator.** Gate: false suspicion never produces a lost or doubled commit; a Node that cannot reach its peers completes Patches through the comb-store floor; the operator scales out under sustained heat and drains Nodes without losing work.
11. **Quorum decisions and swarming.** Gate: no split decision across repeated simulated runs with two equally good options, with and without partitions.

### Measuring on real hardware

Run the SSB, TPC-H-derived and Apiary-specific suites on Pi 4, Pi 5 and cloud Nodes. Report speedup against measured per-Bee throughput, never against Bee count: a cloud core and a Pi 4 core are not the same Bee, so a speedup larger than the Bee ratio on mixed hardware says more about the cores than about the scheduler.

### Open questions

- [ ] How long is a site's retention window by default, and who sets it per Frame?
- [ ] Does harvest copy every Frame, or only those a refinery needs?
- [ ] For a standalone deployment, is the refinery an Apiary colony in the cloud, or always a third-party platform?
- [ ] How should `_stage` reach users who never look at it: a warning on every result that used crop rows, or only on request?
- [ ] Which ingest protocols beyond Flight and MQTT do the first sites need (OPC UA, Modbus through a gateway, plain HTTP)?
- [ ] How do models reach the Pis, and how are model versions pinned per site?
- [ ] Register harvest tables in Unity Catalog automatically, or leave registration to the Databricks side?
- [ ] Who runs the relay in an Apiary of two edge sites and no cloud: a Node with a forwarded port, or a small rented VM?
- [ ] How does a Node join from a network that blocks UDP: through the relay over TCP, or not at all?
- [ ] Can Apiary and Spark ever share writes to one harvest table on S3, given the conditional-put caveat in §6?
- [ ] Should query priority exist at all, given it is the one lever outside the colony's own signals?
- [ ] Is Q = 2 right for a two-Node colony, where it means unanimity?
- [ ] Do ripening recipes (sort key, deduplication key, field splitting) live in Delta table properties or in Apiary's catalogue?
