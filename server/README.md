# Self-Checkout — Service-Based Spring Boot + PostgreSQL

Implements `reference-repo/spec/self-checkout-openapi.yaml` so the unmodified
reference load client runs against it.

The system is five domain services (catalog, checkout, inventory, analytics,
admin) behind a gateway on :8080, sharing one database. See
[`../SERVICES.md`](../SERVICES.md) for the service list and design.

## Setup

```bash
cp .env.example .env && $EDITOR .env    # set DB_PASSWORD
./scripts/create-db.sh                  # create the self_checkout database
./scripts/build-all.sh                  # one -exec.jar per service
./scripts/run-all.sh                    # admin-service migrates + seeds, then the rest start
./scripts/stop-all.sh                   # when done
```

Logs and PIDs land in `server/run/`.

## Between load-test runs

```bash
./scripts/reset-db.sh          # wipe + reseed 2,000 items x 10,000 units
```

Equivalent alternatives: `POST /admin/reset`, or
`RESET_ON_STARTUP=true ./scripts/run-all.sh`. Either way the running services
see the bumped `system_epoch` and rebuild their in-memory state.

## Running the load test

```bash
./scripts/run-load-test.sh                            # 10 stations, 60s
./scripts/run-load-test.sh --stations=100 --duration=180
```

The client needs a JDK 21+ (it uses virtual threads and is compiled at class
version 65). The machine default here is 17, so the script pins JAVA_HOME to 21
itself. Reports land in `server/reports/`.

## Checking correctness after a run

```bash
./scripts/verify.sh
```

Section 2 of that report should return **zero rows**. See "Overselling" below
for what it actually checks and why it is not the literal invariant from the
project README.

## Concurrency

Stock is decremented at completion, once per scanned unit. The whole safety
argument is one statement:

```sql
UPDATE inventory SET stock = stock - ? WHERE sku = ? AND stock >= ?
```

There is no read-then-write window. Under READ COMMITTED, when a concurrent
transaction has already modified the row, Postgres blocks on the row lock and
then **re-evaluates the WHERE clause against the newly committed version**
before deciding whether to write. Check and write are the same operation, so
two stations racing for the last unit cannot both win — one sees 1 row
affected, the other sees 0.

Supporting decisions:

- **Basket lines are decremented in SKU order.** Two concurrent baskets holding
  `{A,B}` and `{B,A}` would otherwise take the two row locks in opposite orders
  and deadlock; Postgres would abort one, surfacing as a 500. A single global
  lock order makes the cycle impossible.
- **`CHECK (stock >= 0)` on the column** is a database-level backstop. If the
  application logic is ever wrong, the write is refused rather than silently
  overselling.
- **The completion transaction is kept short**, because every inventory row
  lock is held until it commits. No HTTP, no analytics, no receipt formatting
  inside it.
- **`SELECT ... FOR UPDATE` appears only on the stock-exhaustion path**, fused
  into a single statement via a CTE so even the rare path costs one round trip.

Rejected alternatives: `SELECT ... FOR UPDATE` on the hot path (two round trips
while holding the lock), `SERIALIZABLE` (constant serialization failures on the
Zipf-hot SKUs), optimistic version-and-retry (retry storms concentrated on
exactly the hottest rows), and application-level locking (breaks with more than
one instance, and abandons the database as source of truth).

## Overselling and stock exhaustion

The project README asks for:

> `initial_stock - final_stock` must equal the total number of
> completed-transaction line items for that SKU, and final stock must never go
> negative.

Both hold for any SKU with stock remaining. Some SKUs will run out, though:
the client's Zipf sampler gives the top item ~12% of all scans (`1/H(2000)`)
against 10,000 starting units, and the reference run scanned 1.8M items in
60s — around 220,000 units of `SKU-000001` alone. At zero, the two clauses
pull apart: further units can be counted as sold, or stock can be held at
zero, but not both.

The contract gives `/complete` **no insufficient-stock error path**, so a 200
must be returned either way. This implementation preserves the
no-negative-stock guarantee — a negative stock count is simply false about the
real world — and records each unfulfilled unit in `stock_shortfalls`, so the
accounting stays checkable after exhaustion:

```
initial_stock - final_stock == completed_line_items - unfulfilled
```

The original form is the special case where `unfulfilled = 0`. The audit table
is what keeps the two cases distinguishable: without it, correctly refusing to
oversell and actually overselling through a concurrency bug would produce the
same unbalanced arithmetic. `scripts/verify-invariants.sql` checks the form
above.

## Popularity

Hopping window per the spec: the most recent `windowSize` (1000) **scans**,
recomputed every `slideInterval` (500) scans. Note this counts scans, not
completed purchases, and the window advances at scan time.

checkout-service appends every accepted scan to `scan_events`, in the same
statement that adds it to the basket. analytics-service polls that table and
feeds a window → rank → persist pipeline: a ring buffer holds the last 1,000
SKUs, and every 500th scan a ranked snapshot is written to
`popular_item_windows` / `popular_item_entries`. `GET /analytics/popular-items`
serves the latest snapshot.

Until week 3, individual scans were not persisted. They are now, because a
separately deployed analytics service has no other way to see them; that row
is part of the cost of the service split.

## Low stock

Hybrid, because the spec wants both "alerts are generated whenever stock drops
below a threshold" and an optional per-query `?threshold=` override:

- **Write side:** crossing from `>= threshold` to `< threshold` during a
  completion inserts a `low_stock_alerts` row, giving a truthful `triggeredAt`.
- **Read side:** a live `WHERE stock < :effectiveThreshold` scan, joined to the
  most recent alert row for `triggeredAt`. A persisted alert was triggered
  against the *configured* threshold, so it cannot answer a query about a
  different one.

## Contract details that are easy to break

- `GET /items` returns `ORDER BY sku`. The client's `ItemSampler` uses array
  *position* as the Zipf rank, so an unstable order silently destroys
  cross-run comparability of popular-items.
- `POST /transactions` returns **201**, not 200.
- `POST /transactions/{id}/complete` receives a literal `{}`; no request body
  is bound, since binding one would make that a 400.
- Errors use `{error, message}`. Spring Boot's default error body does not
  match, so everything routes through `GlobalExceptionHandler`.
- Money is `NUMERIC`/`BigDecimal` end to end. Accumulating 20 `double`
  additions per basket drifts enough to make `sum(lines) != totalAmount`.

## Configuration

| Property | Default | Meaning |
| --- | --- | --- |
| `app.low-stock-threshold` | 50 | default threshold for `/inventory/low-stock` |
| `app.popularity.window-size` | 1000 | scans counted in the window |
| `app.popularity.slide-interval` | 500 | recompute every N scans |
| `app.catalog-size` | 2000 | items to seed |
| `app.stock-per-item` | 10000 | starting stock per SKU |
| `app.reset-on-startup` | false | wipe + reseed at boot |
| `DB_POOL_SIZE` | per service | Hikari max pool size: checkout 32, analytics 4, inventory 4, catalog 2, admin 2 |

Checkout's pool is deliberately smaller than the station count: briefly queuing
at the pool is much cheaper than driving 100 concurrent Postgres backends. All
five pools together (44) stay well under Postgres's default
`max_connections` of 100.

`app.catalog-size`, `app.stock-per-item` and `app.reset-on-startup` belong to
admin-service; `app.popularity.*` to analytics-service;
`app.low-stock-threshold` is shared, in `common/.../selfcheckout-common.yml`.
