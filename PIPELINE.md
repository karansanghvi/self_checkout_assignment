# Assignment 3: Pipelined Windowed Analytics

## Submission summary

The system has a 3-stage pipeline:

**window** (buffers scans) → **rank** (counts and ranks top-N) → **persist** (writes results to DB)

- **window** (`WindowFilter`): keeps the most recent 1000 scanned SKUs in a ring buffer. Every 500 scans it emits a snapshot of the window with its `windowStart`/`windowEnd`.
- **rank** (`RankFilter`): counts scans per SKU in a snapshot. It sorts by count, breaking ties by SKU, and keeps the top 50.
- **persist** (`PersistFilter`): the sink. It writes each ranked window to Postgres (`popular_item_windows` / `popular_item_entries`). `GET /analytics/popular-items` serves the latest one from there.

Each filter runs on its own thread. The pipes are bounded Java `ArrayBlockingQueue`s, and the request threads act as the data source:

```
POST /transactions/{id}/scan
        │  sku                 (non-blocking enqueue)
        ▼
  [scans pipe, 65,536]   ──►  window  ──►  [windows pipe, 4]  ──►  rank  ──►  [ranked pipe, 4]  ──►  persist ──► Postgres
```

## Load-test results

| Run | Report | Scans | Errors | Scan p99 | Complete p99 |
|---|---|---|---|---|---|
| Default (10 stations, 60s) | [`server/reports/report-week3-default-10stations-60s.json`](server/reports/report-week3-default-10stations-60s.json) | 679,934 | 0 | 0.97 ms | 16.10 ms |
| Stress (`--stations=100 --duration=120`) | [`server/reports/report-week3-stress-100stations-120s.json`](server/reports/report-week3-stress-100stations-120s.json) | 1,133,951 | 0 | 15.50 ms | 203.39 ms |

Both runs persisted every hop: 1,359 windows for the default run and 2,267 for the stress run, which is exactly scans / 500. No pipe dropped a message. The stock invariants in `server/scripts/verify-invariants.sql` returned zero violating rows after each run.

## Design notes

- **Analytics never slows checkout.** A scan costs the request thread one `offer()` onto the scans pipe. If that pipe were ever full, the scan would be dropped and counted, never waited on. The two snapshot pipes drop the *oldest* snapshot when full, because a newer snapshot supersedes it.
- **No locks in the filters.** Each filter's state is touched only by its own thread. The ring-buffer lock that the old `PopularityService` needed is gone.
- **Exact window bounds.** The window filter copies the ring at the exact scan that closes a hop. Before, a background task copied it later, by which point extra scans could already be in it.
- **Control messages travel in-band.** `Reset` and `Stop` go through the same queues as data, so they stay in order with it. `/admin/reset` sends a Reset and waits until the sink confirms it. Only then does it reseed the DB, so a snapshot from the previous run can't land in freshly emptied tables. Control messages are never dropped.
- **Layering is unchanged.** `PopularityService` is still the only entry point (`recordScan`, `popularItems`, `reset`), and `TransactionService`, the controllers, the schema and the API contract did not change. The ArchUnit rules in `LayeringTest` still pass.

## Code

`server/src/main/java/com/school/selfcheckout/analytics/pipeline/`

| File | Role |
|---|---|
| `Pipe` | Bounded queue between two filters, with its overflow policy and drop counter |
| `Filter` | Base class: take → `process()` → emit loop, plus Reset/Stop handling |
| `Message` | What travels on a pipe: `Data`, `Reset`, `Stop` |
| `WindowFilter`, `RankFilter`, `PersistFilter` | The three stages |
| `WindowSnapshot`, `RankedWindow` | Messages passed between the stages |
| `PopularityPipeline` | Wires the pipes and filters and owns the threads |

Tests: `server/src/test/java/com/school/selfcheckout/analytics/pipeline/PopularityPipelineTest.java`
