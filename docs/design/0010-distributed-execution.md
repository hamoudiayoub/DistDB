# 0010 — Distributed execution and the optimizer

> Written while building Phase 9; the numbers are from the gate run. The theme is "move the
> computation, not the data": a scan that filters and aggregates should do that on the ranges, next
> to the data, and send the gateway only what it needs; and a planner that knows how big things are
> should choose plans by cost. The parts to defend without notes are the partial aggregation and its
> merge, the reason locks and snapshots still hold on a coprocessor, and the cost model.

## Problem

In Phase 8, every row a query reads crosses the network to the gateway, page by page and range by
range. `SELECT count(*) FROM t` on a million-row table moves a million rows to count them.

The planner, meanwhile, picks plans by rules of thumb:

- joins are built in the order the query wrote them;
- a lookup join is chosen when the left side "looks small".

Phase 9 makes queries cheap where the data is, and plans where the estimates are:

- **pushdown:** filters, projections, partial aggregates and limits run on each range's leader;
- **statistics:** `ANALYZE` collects row counts, distinct counts (HyperLogLog), null fractions and
  histograms;
- **a cost-based optimizer:** access paths, join algorithms and join order (dynamic programming),
  by estimated cost;
- **`EXPLAIN ANALYZE`:** what each operator actually did: rows, time, ranges, nodes, requests and
  bytes.

**The gate:** a *measured* reduction in network bytes and latency on aggregation queries.

## The shape

```
 gateway                                             range leaders (one per range of the span)
 ───────                                             ─────────────────────────────────────────
 MergeAggregate  ◀── partial states ──┐
   └ RemoteScan ── fragment ──▶ range 7: snapshot scan → filter → (partial) aggregate → rows
                 ── fragment ──▶ range 8:   (same program, in parallel)
                 ── fragment ──▶ range 9:   ...
```

A **fragment** is a serialized program:

- a table's descriptor;
- a filter;
- which columns to keep;
- optionally a partial aggregation (group expressions and aggregates);
- optionally a row limit.

The gateway sends the same fragment to every range of the span, in parallel. Each range runs it
over its part of the span, at the transaction's snapshot, and answers rows or partial aggregate
states. The gateway merges.

## The coprocessor

The coprocessor runs on the range leader, inside the read path, as a linearizable read
(ReadIndex) like any transactional scan.

- **Snapshots and locks hold:** it iterates the versions visible at `read_ts` with Percolator's
  rules, and stops with `KeyLocked` at a lock below the snapshot. The gateway resolves the lock
  and retries, as `Txn.get` does. Pushdown never sees anything a plain scan would not.
- **Layers:** the storage layer (`distdb-kv`) must not depend on SQL. The coprocessor is an
  interface in `distdb-rpc`, below both: `Coprocessor.start(program)` returns a run that receives
  `(key, value)` pairs and produces a result. `distdb-sql` implements it (`SqlCoprocessor`), and
  the server plugs it into every store. The expression semantics then come from one place: the
  same `Eval` runs on the range and on the gateway.
- **Paging:** a run may stop early (a row budget), and the response carries a resume key. The
  client clips the request at each range's end and continues in the next range, as it does for
  scans.
- **Own writes:** a transaction's buffered writes live on the gateway, where the ranges cannot see
  them. A fragment over a span holding the transaction's own writes therefore runs locally: same
  program, fed by an ordinary scan with the writes overlaid.

### Partial aggregation

Each aggregate splits into a partial state on the ranges and a merge on the gateway:

| aggregate | partial (per range, per group) | merge |
|---|---|---|
| `count(*)`, `count(x)` | a count | sum of counts |
| `sum(x)` | a sum (NULL if no row) | sum of non-NULL sums |
| `min(x)`, `max(x)` | the min/max | min/max |
| `avg(x)` | (sum, count) | sum of sums / sum of counts |

`avg` cannot merge averages: the average of averages is wrong when ranges hold different numbers
of rows. `count(DISTINCT x)` is not pushed down (merging needs the distinct values themselves).

### Projection pruning, without renumbering

The fragment returns full-width rows, but every column the plan above never reads is NULL (one
byte). The plan keeps its column positions, and the bytes go away.

## Statistics

`ANALYZE t` reads the table once and stores, under `/1/"stats"/{table id}`:

- the **row count**;
- per column, the **null fraction**;
- per column, the **distinct count**, estimated with a **HyperLogLog** of 2^12 registers. Each
  value is hashed; the register chosen by the first 12 bits keeps the longest run of leading zeros
  seen in the rest, and the harmonic mean of `2^register` estimates the count, with a standard
  error of about 1.6%;
- per column, an **equi-depth histogram** of up to 32 buckets: bucket bounds such that each bucket
  holds the same number of rows.

## Cardinality and cost

**Selectivity of predicates:**

- `col = v`: the histogram's share for v's bucket, divided by the bucket's distinct values (or
  `1/ndv`);
- `col < v`: the histogram fraction below v;
- `IS NULL`: the null fraction;
- AND multiplies, OR adds minus the product, NOT subtracts from 1 (independence assumed);
- without statistics, PostgreSQL's defaults: 0.005 for an equality, 1/3 for a range.

**Joins:** an equi-join keeps `|L| × |R| / max(ndv_L, ndv_R)` rows.

**The cost** counts work in units of one network round trip:

- a request (RPC) costs 1;
- a row moved over the network, 0.01;
- a row read on a range, 0.002;
- a row processed on the gateway, 0.001.

Some consequences:

- a point get costs about 1;
- a full scan with pushdown costs ranges × 1 + rows read × 0.002 + rows returned × 0.01;
- an index scan pays a get (1) per matching row;
- a lookup join pays one get per left row;
- a hash join pays for building and probing, in gateway CPU.

**Join order:** dynamic programming over subsets (System R / Selinger). For each set of base
relations, keep the cheapest plan joining them. Combine two disjoint sets that a predicate
connects (cross products only when nothing connects them), and try every algorithm for that pair.
It applies to the inner joins of a query (up to 8 relations). Outer joins keep their written
order, since they are not freely reorderable. A final projection restores the column order the
query asked for.

## EXPLAIN ANALYZE

`EXPLAIN ANALYZE` runs the statement and prints each operator with what it really did: rows,
time, and for remote scans the ranges, nodes and requests it used and the bytes it received. Time
comes from the `Clock` seam, so it is simulated time in the simulator and real time in a server.

## Verification (the Phase 9 gate)

**Measured** by `PushdownGateTest`, on the deterministic simulator:
- **Data:** 20,000 rows of about 150 bytes each on the wire, spread over 40 ranges led by 5
  nodes, after `ANALYZE`.
- **The wire model** (`ShardedKvSimulation.Wire`): 1–5 ms per message, a 1 Gbit/s inbound link per
  node that carries one message at a time, 1 µs per key a range reads, and no losses.
- **Runs:** each query runs 5 times per mode, the modes alternating, after a warm-up of each. The
  table shows medians.
- **Bytes:** what the gateway's KV client sent and received, serialized requests and responses.

| query | bytes, off | bytes, on | requests off / on | latency off | latency on | faster |
|---|---:|---:|---:|---:|---:|---:|
| `count(*)` | 2,999,629 | 31,396 | 119 / 41 | 1,391 ms | 42 ms | 32.8× |
| `GROUP BY region`: count, sum, avg | 2,999,629 | 48,996 | 119 / 41 | 1,383 ms | 38 ms | 36.5× |
| `WHERE qty > 6 GROUP BY product`: sum(qty × price), max | 2,999,629 | 123,053 | 119 / 41 | 1,391 ms | 44 ms | 31.4× |
| min and max, with a filter | 2,999,629 | 40,252 | 119 / 41 | 1,374 ms | 37 ms | 37.3× |
| a narrow key range (1,000 rows) | 149,565 | 3,007 | 7 / 4 | 94 ms | 37 ms | 2.5× |
| a filter only (not an aggregation) | 2,999,629 | 45,217 | 119 / 41 | 1,361 ms | 40 ms | 33.7× |

Every query gives the same answer with and without pushdown. For each aggregation, the test
asserts at least a tenfold cut in bytes and at least a halved latency. The aggregations moved 1–4%
of the bytes.

**Where the latency goes.**
- **Round trips.** Without pushdown, the scan reads 256-row pages, one range after another: 119
  round trips in a row, about 11.7 ms each (two client hops, a ReadIndex round on the range, the
  read). With pushdown, every range answers at once, with one message each.
- **Transfer.** Moving 3 MB at 1 Gbit/s takes only 24 ms of the 1.39 s. In this model, then, the
  latency gain comes almost all from round trips. The bytes gain is separate, and it matters more
  on a slower or shared network: at 100 Mbit/s, the 3 MB alone would take 240 ms.
- **Not modeled:** the gateway's CPU, which decodes and aggregates 20,000 rows without pushdown.
  Pushdown moves that work to the ranges too.

**What pushdown still costs.** About 750 bytes per range, mostly the fragment itself, which carries
the table's descriptor. Ranges could cache descriptors by catalog version, and fragments would
shrink to tens of bytes.

**Correctness held, with pushdown on everywhere.**
- **sqllogictest:** 13 files, 641 queries, all answered as PostgreSQL 18 answers them.
  - The files now include `optimizer.test`: joins of 3 to 5 tables written in bad orders, pushed
    aggregates, limits with offsets, correlated subqueries, and a transaction that reads its own
    writes.
  - They run in memory, on the cluster, and on a cluster whose tables split into up to 109 ranges
    while the statements run. There, stale range caches make requests clip and resume.
- **The SQL bank under faults:** its audits (`count(*), sum(balance), min(balance)`) and index
  audits now run on the ranges, through power cuts, partitions, disk failures, splits and moves.
- **The other simulations:** the anomaly tests, the fence test, and the Phase 4–7 simulations.
  Their wire model is off, so their seeds replay unchanged.

**Planted bugs.** All nine were caught. Two of them first survived every test, and each got a
targeted test:

| | planted bug | caught by |
|---|---|---|
| Q1 | the `avg` merge keeps one range's count | `FragmentMergeTest` |
| Q2 | the coprocessor ignores locks | SQL bank (an audit found 878, not 900); sqllogictest on splitting ranges |
| Q3 | the coprocessor reads the newest version, not the snapshot | SQL bank (an index lookup found 9 rows, the pushed-down scan 10) |
| Q4 | a chain skips the key it should resume at | SQL bank (an account vanished from the audit); sqllogictest on splitting ranges |
| Q5 | pruning drops columns only the gateway's filter reads | sqllogictest (the correlated `EXISTS`) |
| Q6 | pushdown ignores the transaction's own writes | `SessionTest`; sqllogictest (`optimizer.test`) |
| Q7 | a pushed limit forgets the offset | survived everything at first; now `SessionTest` |
| Q8 | the join-order search keeps the costliest plan | survived everything at first; now `SessionTest` |
| Q9 | HyperLogLog's ranks are off by one | `StatsTest` |

- **Q7** survived because every `LIMIT ... OFFSET` in the sqllogictest files has an `ORDER BY`.
  It must: without one, PostgreSQL's row order is undefined. And a limit is pushed only when no
  sort is above it, so no file query ever reached a pushed limit.
- **Q8** survived because a bad join order still gives the right answer. Only a test of the plan
  can see it, and the join-order test checked the answer only.

## Not yet

- Pushing joins down (a distributed hash join with a shuffle between nodes).
- `ORDER BY ... LIMIT` (top-N) on the ranges; only plain limits are pushed.
- Index-only scans, and pushdown on secondary indexes.
- Statistics kept fresh automatically; `ANALYZE` is explicit, as in PostgreSQL.
- Learning costs from measurements; the constants are fixed.
