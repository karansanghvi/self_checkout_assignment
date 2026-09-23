# Architectural Characteristics Analysis
## Week 1 — Layered Monolith (Spring Boot + PostgreSQL)

Implementation of the shared self-checkout API contract
(`reference-repo/spec/self-checkout-openapi.yaml`), measured with the
unmodified reference load client.

---

## 1. What was built

A single deployable Spring Boot application over one PostgreSQL database,
organised into conventional layers:

```
api/         REST controllers + DTOs mirroring the OpenAPI contract
service/     business logic (transactions, inventory, popularity, catalog)
repository/  hand-written SQL via JdbcTemplate
domain/      immutable records
```

**Size:** 1,562 lines of Java, 237 lines of SQL. No ORM — JdbcTemplate with
explicit SQL, chosen so that the concurrency control is visible in the source
rather than hidden behind a persistence framework's dirty-checking and
optimistic-locking machinery.

All seven contract endpoints are synchronous request/response. There is one
process, one database, one transaction boundary per request.

---

## 2. Test environment

| | |
|---|---|
| Hardware | Apple Silicon, 10 cores, 16 GB RAM |
| Runtime | OpenJDK 21.0.12.1 |
| Database | PostgreSQL 18.1, `max_connections=100`, `synchronous_commit=on` (default) |
| Connection pool | HikariCP, max 32 |
| Catalog | 2,000 SKUs × 10,000 units |
| Client | Reference load client, unmodified, Zipf-weighted item sampling |

No database tuning was applied. `synchronous_commit` was deliberately left on:
turning it off would have improved the headline numbers while changing the
durability guarantee, which would make week-to-week comparison dishonest.

Both runs were preceded by a full wipe and reseed (`POST /admin/reset`, ~50 ms).

---

## 3. Measured results

| Metric | 10 stations / 60s | 100 stations / 120s | Change |
|---|---|---|---|
| Transactions completed | 60,266 | 105,314 | |
| Throughput (tx/sec) | 1,004.1 | 876.7 | **−12.7%** |
| Items scanned | 631,353 | 1,104,699 | |
| Throughput (items/sec) | 10,519.6 | 9,196.0 | **−12.6%** |
| Errors | **0** | **0** | — |
| Mean basket size | 10.48 | 10.49 | — |

Per-operation latency (ms):

| Operation | | mean | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| START_TRANSACTION | 10 st | 0.38 | 0.33 | 0.62 | 1.12 | 17.72 |
| | 100 st | 6.55 | 6.29 | 12.12 | 16.74 | 128.84 |
| SCAN_ITEM | 10 st | 0.43 | 0.38 | 0.70 | 1.21 | 23.85 |
| | 100 st | 6.48 | 6.23 | 12.01 | 16.36 | 150.28 |
| COMPLETE_TRANSACTION | 10 st | 5.05 | 4.11 | 13.17 | 19.49 | 61.56 |
| | 100 st | 39.46 | 21.06 | 132.62 | 211.81 | 522.75 |

Correctness, verified by `server/scripts/verify-invariants.sql`:

| | 10 stations | 100 stations |
|---|---|---|
| Rows with negative stock | 0 | 0 |
| SKUs failing reconciliation | 0 / 2,000 | 0 / 2,000 |
| Units sold | 631,353 | 1,104,699 |
| Units decremented | 501,105 | 804,914 |
| Units unfulfilled (stock exhausted) | 130,248 | 299,785 |
| SKUs exhausted | 7 | 13 |
| Deadlocks / serialization failures | 0 | 0 |
| Connection-pool timeouts | 0 | 0 |

`decremented + unfulfilled = sold` exactly, in both runs.

---

## 4. Architectural characteristics

Rated for *this* implementation on *this* workload, with the supporting
evidence. Ratings are relative to what the later architecture styles in the
course are expected to achieve.

### Data integrity / consistency — ★★★★★

The strongest characteristic of this style, and the reason it wins on the
exercise's stated correctness property.

Across 1.74 million scanned units over both runs, stock never went negative
and every one of the 2,000 SKUs reconciled exactly. This is not the result of
careful application logic; it is a consequence of everything happening inside
**one** database transaction on **one** database. The entire safety argument
reduces to a single statement:

```sql
UPDATE inventory SET stock = stock - ? WHERE sku = ? AND stock >= ?
```

There is no read-then-write window. Under READ COMMITTED, when a concurrent
transaction has already modified the row, PostgreSQL blocks on the row lock
and then re-evaluates the `WHERE` clause against the newly committed version
before deciding whether to write. Check and write are the same operation.

Two supporting decisions mattered:

- **Basket lines are locked in SKU order.** Concurrent baskets holding `{A,B}`
  and `{B,A}` would otherwise acquire the two row locks in opposite orders and
  deadlock. Zero deadlocks were observed across 165,580 completions — the
  ordering works.
- **`CHECK (stock >= 0)`** on the column is a database-level backstop. Had the
  application logic been wrong, the write would have been refused rather than
  silently overselling.

The cost of this rating is paid in Scalability, below. It is the central
trade-off of the style.

### Simplicity — ★★★★★

1,562 lines of Java and 237 of SQL implement the entire contract. There is no
service discovery, no inter-service contract, no distributed transaction, no
eventual-consistency reconciliation, no message broker. A developer can read
the completion path end to end in a few minutes, and the concurrency control
is three statements in one file.

This is the baseline against which later weeks' accidental complexity should
be judged.

### Performance — ★★★★☆

10,520 items/sec at 10 stations, with scan p99 at 1.21 ms, is strong for a
durable, fully-persisted implementation. For context, the in-memory reference
mock server achieved 30,089 items/sec — so adding full PostgreSQL durability
cost roughly 3× throughput, not the order of magnitude one might assume.

Two design choices account for most of that:

- **The 2,000-row catalog is cached in memory.** It is immutable after
  seeding, so per-scan price lookup is a hash lookup rather than a database
  round trip. Scans are ~91% of all requests, making this the single largest
  win available.
- **A scan is one database round trip.** A data-modifying CTE upserts the
  per-SKU basket line and bumps the denormalized counters on the transaction
  row in a single statement.

The rating is withheld from five stars because the tail under load is poor —
see Scalability.

### Scalability — ★★☆☆☆

The clearest weakness, and the most interesting result in the data.

**Ten times the concurrency produced 12.7% *less* throughput.** The system was
already saturated at ~10,500 items/sec with 10 stations; the additional 90
stations converted entirely into queueing delay.

Little's Law closes almost exactly, which confirms saturation rather than
measurement noise. For a closed system, `N = X × R`:

| | Stations (N) | Throughput (X) | Implied response time (R) |
|---|---|---|---|
| Baseline | 10 | 1,004.1 tx/s | 9.96 ms |
| Stress | 100 | 876.7 tx/s | 114.06 ms |

Summing the measured per-operation means for an average 10.48-item basket
predicts the same figures independently:

- 10 stations: `0.38 + (10.48 × 0.43) + 5.05` = **9.93 ms** (vs 9.96 measured)
- 100 stations: `6.55 + (10.49 × 6.48) + 39.46` = **113.98 ms** (vs 114.06)

Both models agree to within 0.3%. The system is at capacity, and response time
is rising in exact proportion to offered concurrency while throughput stays
flat — the textbook signature of a saturated closed system.

Vertical scaling is the only lever available: there is one process and one
database, and the contended inventory rows cannot be partitioned without
changing the architecture.

### Elasticity — ★★☆☆☆

The application layer is stateless apart from two in-memory structures (the
catalog cache and the popularity ring buffer), so additional instances could be
started behind a load balancer in seconds. But they would all contend on the
same PostgreSQL rows, so the throughput ceiling would not move — and the
popularity window, being per-instance, would fragment. Fast to scale out,
little benefit from doing so.

### Testability — ★★★★☆

One process and one database make deterministic testing straightforward. The
reinitialization path (`POST /admin/reset`, or `scripts/reset-db.sh`) restores
a known state in ~50 ms using `TRUNCATE` and `generate_series`, so a full test
run can start clean without a meaningful setup cost.

Correctness is verifiable independently of the application by SQL alone
(`verify-invariants.sql`), which is a genuine testability advantage of keeping
all state in one relational store.

Deducted one star: the concurrency behaviour that matters most can only be
exercised under real concurrent load, not by unit tests.

### Deployability — ★★★★☆

One artifact, one database, one Flyway migration chain. Deployment is a JAR
and a schema migration. The flip side is that any change — a pricing tweak or
an analytics change — requires redeploying the whole application.

### Evolvability — ★★☆☆☆

Layer boundaries are clean, but they are compile-time boundaries with no
enforcement. Nothing prevents a controller from reaching into a repository.
More significantly, the layers are organised *technically* (api / service /
repository) rather than by domain, so a change to "inventory" touches three
packages. Extracting inventory as an independent service later — as the
service-based and microservices weeks require — means unpicking it from a
shared transaction boundary, which is precisely where the correctness
guarantees currently come from.

### Fault tolerance — ★☆☆☆☆

No bulkheads anywhere. The database is a single point of failure; if it is
unavailable, every endpoint fails including the read-only ones. A slow query
consumes a connection from the pool shared by all operations, so degradation
in one code path starves the others.

The one deliberate isolation measure: **popularity recomputation runs off the
request thread**, on a single-threaded executor with a queue depth of 1 and a
discard-oldest policy. Analytics can fall behind, but it can never apply
backpressure to checkout. In practice it never fell behind — 2,209 windows
were persisted in the stress run, exactly `floor(1,104,699 / 500)`, so no hop
was dropped even at peak load.

### Availability — ★★☆☆☆

Single process, single database. Any deployment is downtime.

### Cost — ★★★★★

One VM and one managed database. The cheapest configuration the course will
produce.

---

## 5. Where the time actually goes

Decomposing mean response time by operation reveals something the headline
numbers hide.

| Share of transaction cycle | 10 stations | 100 stations |
|---|---|---|
| START_TRANSACTION | 3.8% | 5.7% |
| SCAN_ITEM (×10.48) | 45.4% | 59.6% |
| COMPLETE_TRANSACTION | 50.8% | **34.6%** |

Completion dominates at low concurrency — it is 51% of the cycle despite being
1 request in 12.5 — because it is the only operation that contends for shared
rows. But under stress its *share* falls to 35%, because generic queueing on
scans grows faster than lock contention does.

The tail-latency ratios separate the two effects cleanly:

| p99 / p50 ratio | 10 stations | 100 stations | |
|---|---|---|---|
| START_TRANSACTION | 3.39 | 2.66 | improved |
| SCAN_ITEM | 3.18 | 2.63 | improved |
| COMPLETE_TRANSACTION | 4.74 | **10.06** | **worsened 2.1×** |

**Only the contended operation's tail got worse.** Queueing delay, being
roughly uniform across requests, actually made the uncontended operations'
distributions *tighter* relative to their medians. Lock contention is
different: it is concentrated on a minority of requests, so it stretches the
tail specifically.

The mechanism is the workload's Zipf weighting. The top SKU took 135,389 of
1,104,699 scans in the stress run — **12.3%**, matching the theoretical
`1/H(2000) ≈ 12.2%` almost exactly. Roughly one completion in eight must
serialize on that single inventory row. Those are the requests in the p99.

This is the headline architectural finding: **in a layered monolith, the
throughput ceiling is set by row-level contention on a small number of hot
records, and it shows up in the tail of one operation long before it shows up
in the mean.**

---

## 6. A caveat on the stress numbers

13 SKUs reached zero stock during the stress run, and **27.1% of all scanned
units (299,785 of 1,104,699) could not be fulfilled**. Once a SKU is
exhausted, every basket containing it takes a slower path — a `FOR UPDATE` CTE
plus a `stock_shortfalls` insert — rather than the single conditional `UPDATE`.

So the 100-station figures blend two distinct effects: lock contention, and
the more expensive exhaustion path. Isolating them would require re-running
with a much larger starting stock. The baseline run is less affected (20.6% of
units unfulfilled, 7 SKUs exhausted) but not free of it either.

This is a property of the supplied workload rather than of the architecture:
the Zipf sampler directs ~12% of 1.1M scans at a SKU stocked with 10,000
units, so exhaustion is unavoidable at this duration.

---

## 7. Handling stock exhaustion: an implementation decision

The project README specifies two correctness properties, to hold for every
SKU:

> `initial_stock - final_stock` must equal the total number of
> completed-transaction line items for that SKU, and final stock must never go
> negative.

Both hold for any SKU with stock remaining. As section 6 shows, however, this
workload drives 13 SKUs to zero — at which point the two properties pull in
opposite directions: further units can be counted as sold, or stock can be
held at zero, but not both. The OpenAPI contract also gives `/complete` no
insufficient-stock error path, so a 200 must be returned either way.

**The decision taken here was to preserve the no-negative-stock guarantee and
audit the shortfall.** That ordering reflects which property is physically
meaningful: a stock count is a claim about the real world, and a negative one
is simply false, whereas an unfulfilled unit is a real event that can be
recorded. The reference mock server makes the same choice
(`Math.max(0, current - 1)`); this implementation adds the record.

Every unfulfilled unit is written to a `stock_shortfalls` table, which makes
the accounting checkable in a form that remains true after exhaustion:

```
initial_stock - final_stock == completed_line_items - unfulfilled
```

This held exactly for all 2,000 SKUs in both runs. The README's original
formulation is the special case where `unfulfilled = 0`, and it held for every
SKU that did not exhaust.

The practical benefit is that exhaustion stays visible. Without the audit
table, a run in which the system oversold through a genuine concurrency bug
and a run in which it correctly refused to oversell would produce the same
unbalanced arithmetic, and the two could not be told apart.

---

## 8. Baseline for later weeks

Expectations to test as the architecture changes:

| Characteristic | Expected direction | Why |
|---|---|---|
| Data integrity | **Degrades** | Once inventory is its own service, check-then-decrement crosses a network boundary and loses the single transaction |
| p99 latency | Degrades | Network hops and orchestration overhead add to an already contended path |
| Throughput ceiling | Roughly unchanged | The bottleneck is row contention on hot SKUs, which no amount of service decomposition removes |
| Scalability | Improves only if data is partitioned | Splitting *services* without splitting the *contended rows* moves the bottleneck, it does not remove it |
| Simplicity | Degrades sharply | From 1,562 lines and one transaction boundary |
| Fault tolerance | Improves | Independent failure domains and bulkheads become possible |

The specific figure worth carrying forward: **~10,500 items/sec, scan p99
1.21 ms, complete p99 19.49 ms at 10 stations, with perfect reconciliation.**
Any later architecture that cannot reconcile is not comparable on performance
at all, regardless of its throughput.

---

## Appendix — reproducing these results

```bash
cd server
cp .env.example .env && $EDITOR .env    # set DB_PASSWORD
./scripts/create-db.sh
./scripts/run-server.sh                 # Flyway migrates + seeds

curl -X POST http://localhost:8080/admin/reset
./scripts/run-load-test.sh --stations=10  --duration=60
./scripts/verify.sh

curl -X POST http://localhost:8080/admin/reset
./scripts/run-load-test.sh --stations=100 --duration=120
./scripts/verify.sh
```

The load client requires JDK 21+ (virtual threads); `run-load-test.sh` pins
`JAVA_HOME` accordingly.

Raw reports:
- `server/reports/report-20260922-180321.json` — 10 stations, 60s
- `server/reports/report-20260922-180712.json` — 100 stations, 120s
