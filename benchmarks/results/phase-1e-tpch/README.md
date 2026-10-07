# TPC-H-derived benchmark: V1 baseline and phase 1e

TPC-H-derived queries (the harness's own generator, scale factor 1), Apiary at the phase 1e
commit against the V1 baseline in [`../v1-baseline-tpch`](../v1-baseline-tpch). Same setup as the
SSB results: pi4-4gb limits (2 CPUs, 2 GB per node), one node, local-filesystem storage
(`deploy/docker-compose.pi4-4gb-local.yml`), 1 warmup + 3 timed runs, `--no-cache-clear`, same
laptop. Median milliseconds per query:

| Query | V1 | Phase 1e | Rows |
|---|---:|---:|---:|
| Q1 | 8468 | 3700 | 6 |
| Q2 | 3625 | 3514 | 100 |
| Q3 | 8781 | 3692 | 10 |
| Q4 | 9371 | 3612 | 5 |
| Q5 | 9537 | 3732 | 5 |
| Q6 | 6203 | 3594 | 1 |
| Q7 | failed | 4034 | 4 |
| Q8 | failed | 3684 | 2 |
| Q9 | failed | 3507 | 0 |
| Q10 | 9340 | 3700 | 20 |
| Q11 | 3642 | 3501 | 1017 |
| Q12 | 8622 | 3698 | 2 |
| Q13 | 3827 | 3595 | 29 |
| Q14 | 6311 | 3609 | 1 |
| Q15 | failed | 3603 | 1 |
| Q16 | 3605 | 3499 | 18279 |
| Q17 | 6646 | 3674 | 1 |
| Q18 | 12345 | 4459 | 100 |
| Q19 | 6295 | 3700 | 1 |
| Q20 | failed | 3555 | 0 |
| Q21 | 10379 | 3904 | 100 |
| Q22 | failed | 3503 | 1 |

On the 16 queries V1 could run, the geometric mean falls from 6791 ms to 3693 ms (46% lower)
and the total from 117.0 s to 59.2 s; every row count matches. All 22 queries run on 1e
(geometric mean 3679 ms, total 81.1 s).

## Notes

- **V1 failed six queries** (Q7, Q8, Q9, Q15, Q20, Q22): its hand-parsed table names broke on
  expressions such as `CAST(l_shipdate ...)` ("Cannot resolve frame path"). Phase 1b's native
  catalogue runs them all, so those six have no V1 figure.
- **Q9 and Q20 return no rows.** That is correct for this data: the harness names parts
  `Part N`, so nothing matches `LIKE '%green%'` (Q9) or `LIKE 'forest%'` (Q20). The row counts
  of the other queries match V1's.
- **A bug this run found.** The first 1e run failed Q9, Q15 and Q20: `ap.sql()` returned `None`
  for a query with no rows, and the harness's deserialiser rejected it. It now returns the
  columns with no rows, and the figures above are from the fixed build.
- About 3.5 s of every query is the harness starting a fresh `docker compose exec python3`, so
  differences below a few hundred milliseconds are not visible, and most of the 1e column sits
  at that floor. The V1 column shows where V1 was slow: joins and aggregations over the larger
  tables (Q1, Q3, Q4, Q5, Q10, Q18, Q21).
- The caveats of the SSB results apply: a laptop under Docker, not a Raspberry Pi; local
  storage, not S3; SF1.
