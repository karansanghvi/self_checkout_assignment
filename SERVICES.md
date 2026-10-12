# Assignment 4: Service-Based Architecture

## Services

The system has **five domain services**. Each one is its own Spring Boot fat jar and runs as its own process. A gateway sits in front of them.

| Service | Port | Business function | Endpoints |
|---|---|---|---|
| **catalog-service** | 8081 | What the store sells | `GET /items` |
| **checkout-service** | 8082 | The transaction lifecycle, including decrementing stock at completion | `POST /transactions`, `POST /transactions/{id}/items`, `POST /transactions/{id}/complete`, `GET /transactions/{id}` |
| **inventory-service** | 8083 | Stock-level reporting | `GET /inventory/low-stock` |
| **analytics-service** | 8084 | Hopping-window item popularity | `GET /analytics/popular-items` |
| **admin-service** | 8085 | Reset/reseed. It is also the only service that runs schema migrations. | `POST /admin/reset` |
| *gateway* | 8080 | Not a domain service. It routes each path to the service that owns it, because the load client accepts only one `--baseUrl`. | everything above |

All five services use **one shared PostgreSQL database**. No service calls another one. Their only coupling is the database tables they share.

```
                          ┌─► catalog-service   ─┐
                          ├─► checkout-service  ─┤   (writes scan_events)
load client ─► gateway ───┼─► inventory-service ─┼─► PostgreSQL (one database)
               :8080      ├─► analytics-service ─┤   (polls scan_events)
                          └─► admin-service     ─┘   (bumps system_epoch)
```

## Load-test results

| Run | Report | Scans | Errors | Scan p99 | Complete p99 |
|---|---|---|---|---|---|
| Default (10 stations, 60s) | [`server/reports/report-week4-default-10stations-60s.json`](server/reports/report-week4-default-10stations-60s.json) | 389,579 | 0 | 3.08 ms | 17.15 ms |
| Stress (`--stations=100 --duration=120`) | [`server/reports/report-week4-stress-100stations-120s.json`](server/reports/report-week4-stress-100stations-120s.json) | 624,594 | 0 | 28.16 ms | 369.81 ms |

After each run, `server/scripts/verify.sh` reported:
- No negative stock.
- Zero rows failing the per-SKU accounting check, `initial - final == sold - shortfall`.
- The `scan_events` row count equals the units in baskets exactly (389,579 and 624,594), with no sequence gaps.
- Analytics persisted exactly `scans / 500` windows (779 and 1,249).

Compared with week 3 (the single process), throughput fell from 679,934 to 389,579 scans in the default run. Scan p99 rose from 0.97 ms to 3.08 ms in the default run and from 15.5 ms to 28.2 ms in the stress run. That is the cost of this architecture:
- every request now crosses one extra HTTP hop, through the gateway;
- every scan writes one more row, which is the analytics feed.

## How shared database access is distributed

Database access lives in **one library per domain**, not one shared jar of every repository. Each service declares only the libraries it needs, so its `pom.xml` shows exactly which tables it is coupled to:

| Library | Tables | Used by |
|---|---|---|
| `catalog-data` | `items` (and an in-memory cache of them) | catalog, checkout, analytics |
| `inventory-data` | `inventory`, `low_stock_alerts`, `stock_shortfalls` | checkout (writes), inventory (reads) |
| `checkout-data` | `transactions`, `transaction_items`, `scan_events` (write) | checkout |
| `analytics-data` | `scan_events` (read), `popular_item_*` | analytics |
| `system-data` | `system_epoch`, the reseed | admin, plus every service that holds a cache |
| `schema` | Flyway migrations | admin only |
| `common` | wire DTOs, error codes, the `{error, message}` handler. No database access. | all |

No service module depends on another service module, so one service cannot even compile a call to another. `architecture-tests/ServiceBoundariesTest` (ArchUnit) checks the same rules across all five at once:
- no service uses another service's classes;
- each service touches only the data libraries listed in its row of the table above;
- no service contains an HTTP client;
- the data libraries never depend on the services.

## Where the monolith made in-process calls, and what replaced them

1. **Checkout decrements inventory itself.** Decrementing stock is inventory's business, but it has to commit in the **same database transaction** as marking the basket COMPLETED. A call to inventory-service could not join that transaction. So checkout runs the decrement through the shared `inventory-data` library, and the SQL that prevents overselling is still written once. This is the one place the boundary had to bend.
2. **Scans reach analytics through a table.** Before, checkout called `popularityService.recordScan()`. Now the scan statement adds one more CTE that inserts into `scan_events`, so a scan is logged exactly when it lands in a basket, in a single round trip. Analytics polls the table in sequence order and feeds the unchanged week 3 window → rank → persist pipeline. Concurrent inserts can commit out of sequence order, so the poller consumes only an unbroken run of sequence numbers and waits briefly when it hits a gap.
3. **Resets reach the other services through an epoch.** Before, `ResetService` called the pipeline and the catalog cache directly. Now admin-service increments `system_epoch` in the same transaction that wipes and reseeds. Every service that holds in-memory state polls the epoch and rebuilds; in the test runs they all picked up a reset within about 100 ms. Analytics takes a share lock on the epoch row before it persists a window, so a window from an earlier run can never land after a reset.

## Running it

```bash
cd server
./scripts/build-all.sh          # tests + one -exec.jar per service
./scripts/run-all.sh            # admin first (migrates), then the rest, then the gateway
./scripts/reset-db.sh           # or: curl -XPOST localhost:8080/admin/reset
./scripts/run-load-test.sh                               # default
./scripts/run-load-test.sh --stations=100 --duration=120 # stress
./scripts/verify.sh
./scripts/stop-all.sh
```
