# Assignment 5: Microservices Architecture

Self-checkout backend in Spring Boot, PostgreSQL and Kafka. It implements `reference-repo/spec/self-checkout-openapi.yaml`, so the unmodified reference load client runs against it.

## Services

The system has **five independently deployable microservices** and an API gateway. Each service is its own Spring Boot fat jar, runs in its own process, and can be rebuilt and redeployed without touching the others. Each service that owns data owns **its own PostgreSQL database**, and no other service has the credentials to open it.

| Service | Port | Owns | Database (role) | Public endpoints | Internal endpoints |
|---|---|---|---|---|---|
| **catalog-service** | 8081 | What the store sells | `catalog_db` (`catalog_svc`): `items` | `GET /items` | `POST /internal/reset` |
| **inventory-service** | 8083 | Stock: decrements, shortfalls, low-stock alerts | `inventory_db` (`inventory_svc`): `inventory`, `stock_commits`, `stock_commit_lines`, `stock_shortfalls`, `low_stock_alerts` | `GET /inventory/low-stock` | `POST /internal/stock-commits`, `POST /internal/reset` |
| **checkout-service** | 8082 | The transaction lifecycle; orchestrates the completion saga | `checkout_db` (`checkout_svc`): `transactions`, `transaction_items`, `scan_events` (outbox), `relay_state` | `POST /transactions`, `POST /transactions/{id}/items`, `POST /transactions/{id}/complete`, `GET /transactions/{id}` | `POST /internal/reset` |
| **analytics-service** | 8084 | Hopping-window item popularity | `analytics_db` (`analytics_svc`): `popular_item_windows`, `popular_item_entries`, `analytics_state` | `GET /analytics/popular-items` | `POST /internal/reset` |
| **admin-service** | 8085 | System-wide reset (stateless orchestrator) | *none* | `POST /admin/reset` | none |
| *API gateway* | 8080 | Routing only. The load client accepts a single `--baseUrl`. | *none* | routes every public path above | does **not** route `/internal/**` |

Supporting infrastructure:
- **PostgreSQL 18** on the host, with four separate databases.
- **Kafka 3.9** in Docker (KRaft single node), with one topic, `scan-events`, that has one partition.

```
                          ┌─► catalog-service   ──► catalog_db
                          │        ▲  GET /items (catalog replicas, at startup and on reset)
                          │        │
load client ─► gateway ───┼─► checkout-service  ──► checkout_db (+ scan_events outbox)
               :8080      │     │   │
                          │     │   └─ POST /internal/stock-commits (saga step 2) ─► inventory-service ─► inventory_db
                          │     └─ outbox relay ─► Kafka [scan-events] ─► analytics-service ─► analytics_db
                          ├─► inventory-service
                          ├─► analytics-service
                          └─► admin-service ── POST /internal/reset, in order ─► catalog, inventory, analytics, checkout
```

### How the boundaries are enforced

- **Database:** `create-db.sh` makes each role the owner of its database, and revokes `CONNECT` from everyone else. For example, `catalog_svc` connecting to `inventory_db` gets `FATAL: permission denied`. Postgres cannot join across databases, so this is a hard boundary, not a naming convention.
- **Build:** no service module depends on another, and there are no shared data libraries. Each service holds its own repositories and its own Flyway migrations, and migrates its own database on startup. The `common` module contains only wire DTOs, error codes, the `{error, message}` handler, and HTTP helpers.
- **Architecture tests:** `architecture-tests/ServiceBoundariesTest` (ArchUnit) checks that:
  - no service uses another's classes;
  - catalog makes no outbound calls;
  - admin has no database access;
  - only checkout produces to Kafka and only analytics consumes;
  - the stock-commit contract is used only by checkout and inventory.

## Running it

All commands run from `server/`. You need JDK 21, Maven, PostgreSQL and Docker Desktop.

```bash
cp .env.example .env && $EDITOR .env    # set DB_PASSWORD and SERVICE_DB_PASSWORD
./scripts/create-db.sh                  # one database + one role per service (idempotent)
./scripts/kafka-up.sh                   # Kafka in Docker, creates the scan-events topic
./scripts/build-all.sh                  # 65 tests + one -exec.jar per service
./scripts/run-all.sh                    # catalog first, then the rest, then the gateway; then a reset
```

Logs and PIDs land in `server/run/`. Stop everything with `./scripts/stop-all.sh` and `./scripts/kafka-down.sh`.

**Load tests.** Reset between runs:

```bash
./scripts/run-load-test.sh                                                          # default: 10 stations, 60s
./scripts/reset-db.sh && ./scripts/run-load-test.sh --stations=100 --duration=120   # stress
```

- The load client needs JDK 21+: it uses virtual threads and is compiled at class version 65. The machine default is 17, so the script pins `JAVA_HOME` itself.
- Reports land in `server/reports/`.
- `reset-db.sh` calls `POST /admin/reset`. There is no SQL shortcut: the data spans four databases, and three services hold in-memory state derived from it.

**Correctness check:**

```bash
./scripts/verify.sh
```

This waits for the asynchronous scan feed to drain, then checks every database; see the results below. Sections 2, 3 and 4 should return zero rows.

## Load-test results

| Run | Report | Transactions | Scans | Errors | Scan p50 / p99 | Complete p50 / p99 |
|---|---|---|---|---|---|---|
| Default (10 stations, 60s) | [`server/reports/report-week5-default-10stations-60s.json`](server/reports/report-week5-default-10stations-60s.json) | 29,787 | 313,694 | **0** | 0.97 / 2.98 ms | 7.00 / 21.55 ms |
| Stress (`--stations=100 --duration=120`) | [`server/reports/report-week5-stress-100stations-120s.json`](server/reports/report-week5-stress-100stations-120s.json) | 56,510 | 594,661 | **0** | 1.09 / 5.27 ms | 181.6 / 416.5 ms |

After each run, `verify.sh` reported:
- No negative stock.
- **inventory_db on its own:** `initial − stock == Σ fulfilled` for every SKU (zero failing rows).
- **Across services:** for every SKU, units inventory was asked to commit equal units checkout sold (zero failing rows). There was exactly one stock commit per completed transaction (29,787 and 56,510), and no saga was left in `COMPLETING`.
- **Stock accounting:** `initial − final == completed line items − shortfall` (zero failing rows). See "Overselling and stock exhaustion".
- **Scan feed:** outbox rows equal units in baskets (313,694 and 594,661), with no sequence gaps, and the relay published all of them. Analytics persisted exactly `scans / 500` windows (627 and 1,189), under the same epoch as checkout.

### Compared with week 4 (service-based, one shared database)

| Run | Transactions/s | Scan p99 | Complete p50 | Complete p99 |
|---|---|---|---|---|
| Default | 615.2 → 496.3 (−19%) | 3.08 → 2.98 ms | 4.29 → 7.00 ms | 17.15 → 21.55 ms |
| Stress | 494.7 → 470.7 (−5%) | 28.16 → 5.27 ms | 36.1 → 181.6 ms | 369.8 → 416.5 ms |

- **Completing a basket costs more.** It is now three steps across two databases plus an extra HTTP hop, where week 4 used one local transaction. That adds about 2.7 ms per completion. At 10 stations this shows directly as fewer transactions per second.
- **Under stress, both weeks hit the same limit.** `SKU-000001` is in most baskets (Zipf, ~12% of scans), and each completion holds its row lock until inventory commits. Completions therefore take turns on that one row, which caps throughput at roughly 470–495 completions/s in either architecture.
- **Scans got faster under stress.** The p99 dropped from 28 ms to 5 ms because completion work, with its row locks, WAL and connections, moved to a different database and pool. Stations reach the completion queue sooner and wait there instead, which is why the complete p50 rose.
- **Locking the hottest row last helped.** The descending lock order (see "Concurrency") raised default-run scans from 258k to 314k and cut complete p99 from 51 ms to 22 ms. Those comparison figures come from the same code with ascending order.

## Coordinating requests that update several databases

### 1. `POST /transactions/{id}/complete`: orchestrated saga (checkout-service is the orchestrator)

Completing a basket changes `checkout_db` (the sale) and `inventory_db` (the stock). No transaction spans both, so `CompletionSaga` runs three local steps:

| Step | Database | Action |
|---|---|---|
| 1 | checkout_db | `UPDATE … SET status='COMPLETING' WHERE id=? AND status='OPEN' AND item_count>0`. This claim makes double-completion safe and refuses empty baskets. |
| 2 | inventory_db | `POST /internal/stock-commits {transactionId, lines}`. One inventory transaction applies every line or none (see "Concurrency"). |
| 3 | checkout_db | `UPDATE … SET status='COMPLETED' WHERE status='COMPLETING'`, then return the receipt. |

- **Idempotent pivot.** Step 2 is keyed by the transaction id (`stock_commits` primary key). A retry after a timeout, a duplicate `/complete`, or a resumed saga all return the original outcome and change nothing. That is what makes retrying safe when checkout cannot tell whether a timed-out request landed.
- **Failure handling:**
  - Inventory unreachable or returning 5xx: checkout retries three times with backoff. If inventory is still down, the client gets a **503** and the transaction stays `COMPLETING`, which is a durable marker that the saga is in flight. `RecoverySweeper` resumes stale `COMPLETING` sagas every 5 s, and a client retrying `/complete` resumes it too. Recovery only moves forward. Under the clamp-and-audit stock policy inventory never refuses a basket for lack of stock, so once step 2 can succeed there is nothing to undo.
  - Inventory returns a definitive 4xx (e.g. a SKU it has no row for): this is the **compensating action**. Inventory applied nothing, so checkout moves the transaction back to `OPEN`.
  - Neither of checkout's steps keeps a database transaction open across the HTTP call.
  - `COMPLETING` is reported as `OPEN` on the wire, because the contract's status enum is `OPEN | COMPLETED | CANCELLED`.
- **Tested live:** I stopped inventory-service mid-saga. `/complete` returned 503, `GET` showed `OPEN`, and scans were refused with 409. When inventory came back, the sweeper completed the transaction exactly once.

### 2. Scans → analytics: choreography through a transactional outbox and Kafka

- The scan statement writes the basket line and an outbox row (`scan_events`) **in one atomic statement**. So a scan is logged exactly when it is accepted, never lost and never invented.
- `OutboxRelay` publishes the rows, in sequence order, to the one-partition `scan-events` topic, using an idempotent producer with `acks=all`. There is one partition because the popularity window needs a single global order. The relay saves its position only after Kafka acknowledges the batch, which makes delivery at-least-once.
- `ScanConsumer` in analytics drops redelivered sequence numbers and events from older epochs. It keeps its topic offset in `analytics_db`, so a restarted analytics-service replays the run and rebuilds its window.
- Neither service calls the other. Analytics being down never slows a scan, because the topic holds the backlog.

### 3. `POST /admin/reset`: orchestrated fan-out

admin-service owns no data. It stamps a new epoch (the current wall-clock time in ms) and calls `/internal/reset` on each service **in a fixed order**:

1. **Catalog** reseeds first, because the others re-read it.
2. **Inventory** reseeds stock from the catalog.
3. **Analytics** notes where the new epoch starts in the topic. It must reset before checkout, so that no new-epoch event can be published before analytics is waiting for it.
4. **Checkout** empties its tables and moves the relay to the new epoch.

If any step fails, the reset stops and names the service that failed. Every step is a full wipe-and-reseed, so re-running it is safe.

### 4. Catalog data the other services need

Checkout needs prices, analytics needs names, and inventory needs the SKU list and names. Each one replicates `GET /items` from catalog-service:
- **Checkout and analytics** keep an in-memory `CatalogReplica`, loaded at startup and on every reset. The catalog does not change during a run, so the hot scan path stays a single round trip to `checkout_db`, with no call to catalog-service.
- **Inventory** copies each item's name into its own `inventory` rows at reset.

## Concurrency: no overselling

Stock is decremented only by inventory-service, the only service that can reach the `inventory` table. checkout never reads stock, so there is no check-then-decrement across the network. The whole safety argument is one statement, run inside `inventory_db`:

```sql
UPDATE inventory SET stock = stock - ? WHERE sku = ? AND stock >= ?
```

There is no read-then-write window. Under READ COMMITTED, when a concurrent transaction has already modified the row, Postgres blocks on the row lock and then **re-evaluates the WHERE clause against the newly committed version** before deciding whether to write. Check and write are the same operation, so two stations racing for the last unit cannot both win: one sees 1 row affected, the other sees 0.

Supporting decisions:
- **Basket lines are decremented in one global SKU order, descending.** Two concurrent baskets holding `{A,B}` and `{B,A}` would otherwise take the two row locks in opposite orders and deadlock; Postgres would abort one, surfacing as a 500. Any single order prevents that. Descending order locks the Zipf-hottest row, `SKU-000001`, last, so it is held for the shortest time. inventory-service sorts the lines itself rather than trusting the caller.
- **`CHECK (stock >= 0)` on the column** is a database-level backstop. If the application logic is ever wrong, the write is refused rather than silently overselling.
- **The inventory transaction is kept short**, because every row lock is held until it commits. There is no HTTP, no analytics and no receipt formatting inside it.
- **`SELECT ... FOR UPDATE` appears only on the stock-exhaustion path**, fused into a single statement via a CTE, so even the rare path costs one round trip.

Rejected alternatives:
- `SELECT ... FOR UPDATE` on the hot path: two round trips while holding the lock.
- `SERIALIZABLE`: constant serialization failures on the Zipf-hot SKUs.
- Optimistic version-and-retry: retry storms concentrated on exactly the hottest rows.
- Application-level locking: breaks with more than one instance, and abandons the database as the source of truth.

## Overselling and stock exhaustion

The project README asks for:

> `initial_stock - final_stock` must equal the total number of completed-transaction line items for that SKU, and final stock must never go negative.

Both hold for any SKU with stock remaining. Some SKUs will run out, though. The client's Zipf sampler gives the top item ~12% of all scans (`1/H(2000)`) against 10,000 starting units. At zero, the two clauses pull apart: further units can be counted as sold, or stock can be held at zero, but not both.

The contract gives `/complete` **no insufficient-stock error path**, so a 200 must be returned either way. This implementation keeps the no-negative-stock guarantee, since a negative stock count is simply false about the real world. It records each unfulfilled unit in `stock_shortfalls`, so the accounting stays checkable after exhaustion:

```
initial_stock - final_stock == completed_line_items - unfulfilled
```

The original form is the special case where `unfulfilled = 0`. The audit table keeps the two cases distinguishable. Without it, correctly refusing to oversell and actually overselling through a concurrency bug would produce the same unbalanced arithmetic.

## Popularity

The window follows the spec: the most recent `windowSize` (1000) **scans**, recomputed every `slideInterval` (500) scans. It counts scans, not completed purchases, and advances at scan time.

analytics-service feeds the scan stream (see coordination §2) into a window → rank → persist pipeline:
- A ring buffer holds the last 1,000 SKUs.
- Every 500th scan, a ranked snapshot is written to `popular_item_windows` / `popular_item_entries`.
- `GET /analytics/popular-items` serves the latest snapshot.

## Low stock

The design is a hybrid, because the spec wants both "alerts are generated whenever stock drops below a threshold" and an optional per-query `?threshold=` override:
- **Write side:** crossing from `>= threshold` to `< threshold` during a stock commit inserts a `low_stock_alerts` row, giving a truthful `triggeredAt`.
- **Read side:** a live `WHERE stock < :effectiveThreshold` scan, joined to the most recent alert row for `triggeredAt`. A persisted alert was triggered against the *configured* threshold, so it cannot answer a query about a different one.

## Contract details that are easy to break

- `GET /items` returns `ORDER BY sku`. The client's `ItemSampler` uses array *position* as the Zipf rank, so an unstable order silently destroys cross-run comparability of popular-items.
- `POST /transactions` returns **201**, not 200.
- `POST /transactions/{id}/complete` receives a literal `{}`. No request body is bound, since binding one would make that a 400.
- Errors use `{error, message}`. Spring Boot's default error body does not match, so everything routes through `GlobalExceptionHandler`. A failed dependency is `DEPENDENCY_FAILED` with status 503.
- Money is `NUMERIC`/`BigDecimal` end to end. Accumulating 20 `double` additions per basket drifts enough to make `sum(lines) != totalAmount`.

## Configuration

| Property | Service | Default | Meaning |
| --- | --- | --- | --- |
| `app.low-stock-threshold` | inventory | 50 | default threshold for `/inventory/low-stock` |
| `app.popularity.window-size` | analytics | 1000 | scans counted in the window |
| `app.popularity.slide-interval` | analytics | 500 | recompute every N scans |
| `app.catalog-size` | admin | 2000 | items to seed at reset |
| `app.stock-per-item` | admin | 10000 | starting stock per SKU at reset |
| `app.inventory.read-timeout-ms` / `max-attempts` | checkout | 3000 / 3 | the saga's call to inventory |
| `app.recovery.stale-after-ms` | checkout | 15000 | age at which the sweeper resumes a COMPLETING saga |
| `KAFKA_BOOTSTRAP_SERVERS` | checkout, analytics | localhost:9092 | broker |
| `CATALOG_URL`, `INVENTORY_URL`, ... | callers | localhost ports | where each service finds the others |
| `DB_POOL_SIZE` | per service | checkout 32, inventory 16, analytics 4, catalog 2 | Hikari max pool size |

All four pools together (54) stay under Postgres's default `max_connections` of 100. admin-service has no pool, because it has no database.
