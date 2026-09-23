# Layered Architecture — Live Walkthrough Script

*Speaking script for a 12–15 min live code walkthrough. Bold lines are stage directions — don't read those out. Everything else is roughly what to say, in your own words.*

**Before you start:** have the server running (`server/scripts/run-server.sh`), and pre-open these tabs in this order so you're never hunting during the talk:
`TransactionController` → `TransactionService` → `TransactionRepository` → `InventoryRepository` → `PopularityService` → `GlobalExceptionHandler` → `LayeringTest`.

---

# 1. Architecture & Design Overview
**(3–5 min)**

## The shape of it — ~1 min

**Open:** this file, and just show the diagram on screen.

```
                    HTTP request
                         │
                         ▼
   ┌─────────────────────────────────────────────┐
   │  api/          controllers, status codes,   │   ← only layer that knows HTTP
   │                error bodies                 │
   └─────────────────────────────────────────────┘
          │                │                │
          ▼                ▼                ▼
   ┌────────────┐  ┌────────────┐  ┌────────────┐
   │transactions│  │ analytics  │  │   admin    │   ← business logic
   └────────────┘  └────────────┘  └────────────┘
          │                │                │
          └────────┬───────┴────────────────┘
                   ▼
            ┌────────────┐
            │  catalog   │   ← shared in-memory read model
            └────────────┘
                   │
                   ▼
            ┌────────────┐
            │ repository │   ← database access, hand-written SQL
            └────────────┘
                   │
                   ▼
              PostgreSQL

   contract/   wire DTOs      ┐
   domain/     records, errors │  dependency-free leaves — any layer may use
   config/     properties      ┘
```

What I want you to take away from this picture is that **the arrows only point down**. Nothing in `transactions` or `repository` knows that `api` exists. And that's not a convention I'm asking you to trust — I'll show you the test that enforces it at the end.

## What lives where — ~1 min

Go down the list, one line each:

- **`api/`** — five controllers plus the error handler. Routing and HTTP status codes, nothing else.
- **`transactions/`** — `TransactionService` (start, scan, complete) and `InventoryService` (stock decrements). This is the checkout write path.
- **`analytics/`** — `PopularityService` and `LowStockService`. Read-side reporting.
- **`catalog/`** — `CatalogService`, an in-memory cache of the 2,000-item catalog. Shared, because both checkout and analytics need SKU lookups.
- **`admin/`** — `ResetService`, for wiping and reseeding between load runs. Not part of the OpenAPI contract.
- **`repository/`** — five classes, all hand-written SQL over `JdbcTemplate`.
- **`domain/` + `contract/`** — immutable records. `domain` is internal, `contract` is what goes on the wire.

## Four decisions worth defending — ~2 min

**Decision 1 — JdbcTemplate, not JPA.**
**Open:** [InventoryRepository.java:54-64](server/src/main/java/com/school/selfcheckout/repository/InventoryRepository.java#L54-L64)

The entire correctness story of this system is one conditional `UPDATE`. With JPA that would be buried under dirty-checking and optimistic locking. I wanted the concurrency control to be *readable*. This is the line that prevents overselling, and you can see it.

**Decision 2 — the `contract/` package exists so the arrows stay pointing down.**
**Open:** [TransactionService.java:4-7](server/src/main/java/com/school/selfcheckout/transactions/TransactionService.java#L4-L7)

Before I refactored, these imports said `api.dto.Dtos`. So my *service layer* imported my *API layer* — the dependency pointed backwards. Moving the DTOs to their own leaf package fixed it without rewriting anything, because now both layers depend on a shared thing instead of one depending on the other.

**Decision 3 — `CheckoutException` carries no HTTP status.**
**Open:** [ErrorCode.java:16-22](server/src/main/java/com/school/selfcheckout/domain/ErrorCode.java#L16-L22)

Five business error codes. No `HttpStatus` anywhere near them. The business layer's job is to say *what went wrong*; deciding that "not open" means a 409 is a protocol decision, and it lives in exactly one place in the API layer. I'll show that mapping later.

**Decision 4 — analytics is its own package, not a folder of leftover services.**
**Open:** [PopularityService.java:71-82](server/src/main/java/com/school/selfcheckout/analytics/PopularityService.java#L71-L82)

Read that comment: *"Analytics should never apply backpressure to checkout."* That's a real architectural boundary — a bounded queue with a discard policy. Making analytics its own package means the boundary is in the structure, not just in a comment. It's also the cheapest thing in this codebase to extract into a separate service later.

---

# 2. Deep-Dive Code Walkthrough
**(6–10 min)**

## 2.1 One request, end to end: a scan — ~2.5 min

I'll trace the hottest endpoint in the system. Under load, scans are about 91% of all traffic.

**Stop 1 —** [TransactionController.java:36-40](server/src/main/java/com/school/selfcheckout/api/TransactionController.java#L36-L40)
The controller is four lines. It binds the path variable and the body, and delegates. No logic. That's deliberate — if there were an `if` statement in here, it'd be in the wrong layer.

**Stop 2 —** [TransactionService.java:67-88](server/src/main/java/com/school/selfcheckout/transactions/TransactionService.java#L67-L88)
This is the whole business operation. Walk down it:

- Line 68 — parse the UUID. A malformed ID becomes a 404, not a 500, because a bad ID simply identifies no transaction.
- Line 70 — look up the item. Note this does **not** hit the database.
- Line 75 — the one and only database round trip.
- Line 84 — record the scan for popularity. In-memory, cheap.

**Stop 3 —** [CatalogService.java:79-81](server/src/main/java/com/school/selfcheckout/catalog/CatalogService.java#L79-L81)
That lookup on line 70 lands here — a hash-map get against an immutable snapshot. The catalog is 2,000 rows that never change after seeding, so caching it turns a per-scan database query into a memory read. On the hot path, this was the single biggest win available.

**Stop 4 —** [TransactionRepository.java:88-108](server/src/main/java/com/school/selfcheckout/repository/TransactionRepository.java#L88-L108)
And here's the one round trip. It looks like two statements but it's one.

The `WITH upsert` block inserts or increments the basket line. The outer `UPDATE` bumps the counters on the transaction row. Postgres guarantees a data-modifying CTE runs exactly once and to completion, so I get both writes in a single network hop.

Point at line 94 — the `EXISTS` guard. If the transaction is missing or already completed, *neither* write happens, and the query returns no rows. Which is why back in the service, a `null` return means "work out whether that was a 404 or a 409."

**Then back out:** the service builds a `ScanResult` at line 86, Spring serialises it, done.

## 2.2 Concurrency: how two stations can't buy the same last item — ~2 min

**Open:** [InventoryRepository.java:54-64](server/src/main/java/com/school/selfcheckout/repository/InventoryRepository.java#L54-L64)

This is the most important SQL in the project.

```sql
UPDATE inventory SET stock = stock - ? WHERE sku = ? AND stock >= ?
```

The key point: **there is no read-then-write window.** I never `SELECT` the stock, check it in Java, then write it back — that's the classic race, and it's the bug this assignment is built around.

Under READ COMMITTED, when two transactions hit the same row, the second one blocks on the row lock. When the first commits, the second *re-evaluates its `WHERE` clause against the newly committed row* before deciding to write. So the check and the write are the same atomic operation. Two stations racing for the last unit: one sees 1 row affected, the other sees 0. It's impossible for both to win.

**Then scroll to** [InventoryRepository.java:66-81](server/src/main/java/com/school/selfcheckout/repository/InventoryRepository.java#L66-L81)
The slow path only runs when stock is about to hit zero. The contract gives `/complete` no insufficient-stock error, so I take what's left and record the shortfall rather than failing the request or going negative.

**Now the deadlock part —** [TransactionRepository.java:139-144](server/src/main/java/com/school/selfcheckout/repository/TransactionRepository.java#L139-L144)

That `ORDER BY sku` is not cosmetic. Two baskets containing the same two SKUs in opposite orders would grab row locks in opposite orders and deadlock — Postgres would detect the cycle and abort one, which surfaces as a 500. Sorting SKUs into one global order makes the cycle impossible to form.

The consumer is [InventoryService.java:42-59](server/src/main/java/com/school/selfcheckout/transactions/InventoryService.java#L42-L59) — and note line 55, I derive the previous stock level arithmetically instead of running another query, so low-stock detection is free.

## 2.3 How the popular-items windowing actually works — ~2 min

The contract asks for the top items among the most recent 1,000 scans, recomputed every 500 scans. That's a **hopping window**.

**Piece 1 — the ring buffer.** [PopularityService.java:56-58](server/src/main/java/com/school/selfcheckout/analytics/PopularityService.java#L56-L58)
A plain `String[]` sized to the window, plus a monotonic `scanSequence` counter. A ring buffer is the natural shape here: appends are O(1), and the window is just whatever the array currently holds.

**Piece 2 — the append.** [PopularityService.java:86-96](server/src/main/java/com/school/selfcheckout/analytics/PopularityService.java#L86-L96)
Line 89 — index by `scanSequence % windowSize`, which wraps automatically. Line 91 — every 500th scan, flag a recompute. Line 94 — hand it to a background thread, so scan latency doesn't spike every 500th request.

**Piece 3 — the recompute.** [PopularityService.java:98-132](server/src/main/java/com/school/selfcheckout/analytics/PopularityService.java#L98-L132)
Line 103 — clone the array under the lock and get out fast, so counting happens off-lock. Lines 108–114 — tally the counts. Lines 117–122 — sort by count descending, tie-broken by SKU so equal counts rank deterministically across runs.

**Piece 4 — persistence.** [PopularityRepository.java:31-53](server/src/main/java/com/school/selfcheckout/repository/PopularityRepository.java#L31-L53)
Each computed snapshot gets written as one header row plus its ranked entries, batched.

**Show the schema:** [V1__schema.sql:54-69](server/src/main/resources/db/migration/V1__schema.sql#L54-L69) — `popular_item_windows` holds one row per hop, `popular_item_entries` holds the ranked SKUs for that hop.

**The decision worth calling out:** I do *not* persist individual scans. The load client generates over a million of them per run, and writing a row per scan just to recompute a top-10 every 500 scans would dominate the latency numbers while measuring nothing interesting. What gets persisted is each computed snapshot — which is what "persist it and expose it" actually requires.

**And reads:** [PopularityService.java:134-159](server/src/main/java/com/school/selfcheckout/analytics/PopularityService.java#L134-L159) just serves the most recent persisted snapshot. Line 138 handles the cold case where fewer than 500 scans have happened and no snapshot exists yet.

## 2.4 Error handling across the layer boundary — ~1.5 min

Three files, in order.

**One —** [TransactionService.java:72](server/src/main/java/com/school/selfcheckout/transactions/TransactionService.java#L72)
The business layer throws `CheckoutException.unknownSku(sku)`. It says *what* is wrong. It has no opinion about HTTP.

**Two —** [CheckoutException.java:24-44](server/src/main/java/com/school/selfcheckout/domain/CheckoutException.java#L24-L44)
Static factories, each pairing an `ErrorCode` with a message. Named constructors, so call sites read like sentences.

**Three —** [GlobalExceptionHandler.java:30-43](server/src/main/java/com/school/selfcheckout/api/GlobalExceptionHandler.java#L30-L43)
Here's where HTTP enters. Line 32 maps the code to a status; line 33 puts the code on the wire.

Point at the switch on lines 38–42 — `NOT_FOUND` and `UNKNOWN_SKU` are 404, `TRANSACTION_NOT_OPEN` and `EMPTY_BASKET` are 409, `INVALID_REQUEST` is 400. It's an **exhaustive** switch over an enum, so if someone adds a sixth error code and forgets to map it, *the code won't compile*.

**The layering point:** before the refactor this class was `ApiException` and it carried an `HttpStatus` — which meant `TransactionService` had to import the API layer just to throw an error. Splitting it into "what went wrong" and "how to report it" is what let the whole business layer stop knowing about HTTP.

## 2.5 Patterns actually in use — ~1 min

Rattle these off, pointing as you go:

- **Layered architecture** — the package structure, enforced at [LayeringTest.java:44-72](server/src/test/java/com/school/selfcheckout/LayeringTest.java#L44-L72).
- **Repository** — [repository/](server/src/main/java/com/school/selfcheckout/repository/), all five classes.
- **Data Mapper** — `RowMapper` lambdas, e.g. [TransactionRepository.java:20-32](server/src/main/java/com/school/selfcheckout/repository/TransactionRepository.java#L20-L32).
- **Constructor injection** — everywhere, e.g. [TransactionService.java:30-38](server/src/main/java/com/school/selfcheckout/transactions/TransactionService.java#L30-L38). No field injection, so every class is trivially unit-testable.
- **Static factory methods** — [CheckoutException.java:24-44](server/src/main/java/com/school/selfcheckout/domain/CheckoutException.java#L24-L44).
- **Cache-aside with an immutable snapshot + double-checked locking** — [CatalogService.java:60-72](server/src/main/java/com/school/selfcheckout/catalog/CatalogService.java#L60-L72). The whole cache swaps atomically as one `AtomicReference`, so readers never see a half-rebuilt catalog.
- **Producer–consumer with a bounded queue** — [PopularityService.java:74-82](server/src/main/java/com/school/selfcheckout/analytics/PopularityService.java#L74-L82).
- **Centralised exception translation** — `@RestControllerAdvice` in [GlobalExceptionHandler.java:25](server/src/main/java/com/school/selfcheckout/api/GlobalExceptionHandler.java#L25).

**Finish here:** [LayeringTest.java:80-121](server/src/test/java/com/school/selfcheckout/LayeringTest.java#L80-L121)

Four rules: nothing below `api` may depend on it; controllers may not reach the database; repositories may not call upward; and the lower layers may not import any HTTP types at all. If I break the layering, the build fails. That's the difference between a diagram and an architecture.

---

# 3. Reflections & Lessons Learned
**(2–4 min)**

## Three things that actually bit me — ~2 min

**1. My layers were backwards and I hadn't noticed.**

Every service imported `api.dto.Dtos`. The package *names* looked like a clean layered architecture, but the dependency arrows pointed the wrong way — the business layer depended on the HTTP layer. It compiled fine, it passed every test, and it looked correct in the folder tree. What I learned is that package structure is just naming; it tells you nothing about direction. You need the dependency graph, which is why I ended up writing the ArchUnit test.

**2. The test that enforced nothing.**

I wrote the layering test the idiomatic way, with ArchUnit's `@AnalyzeClasses` and `@ArchTest` annotations. Green build. Then I actually read the Surefire output: **`Tests run: 0`**. Both ArchUnit's test engine and JUnit Jupiter were discovering the class, Jupiter was winning, finding no `@Test` methods, and reporting success. So I had a test that enforced my entire architecture and also did absolutely nothing.

I rewrote it as plain `@Test` methods driving the rules directly. Then — and this is the part I'd actually do differently from the start — I **deliberately broke the architecture** to confirm the test failed. I added a class in `repository/` that imported a controller. Five of the seven rules fired. *Then* I deleted it and trusted the test.

Lesson: a test you've never seen fail is not a test. It's a hope.

**3. A performance regression that wasn't.**

After refactoring I re-ran the load tests and throughput was down about 2%. I nearly wrote that up as the cost of the refactor.

Two things saved me. First, there was a **stale server still running on port 8080** from before the refactor — its JVM was holding the old classes in memory, so I was about to benchmark week-1 code and label it as the new build. Second, once I fixed that and re-ran three times, I got 986, 963, and 907 tx/s — an 8% spread on *identical* runs. My measurement noise was four times larger than the effect I was trying to measure.

So I checked out the week-1 code into a git worktree and ran both versions back to back on the same machine, same session. They came out the same. The 2% was the laptop getting warm, not my code.

## The numbers — ~30 sec

**Show this table.**

| | week-1 baseline | after refactor | week-1 code re-run *today* |
|---|---|---|---|
| 10 stations / 60s | 1004.1 tx/s | 986.5 tx/s | 976.2 tx/s |
| 100 stations / 120s | 876.7 tx/s | 854.4 tx/s | 852.7 tx/s |
| scan p95 (stress) | 12.01 ms | 12.22 ms | 12.24 ms |
| complete mean (stress) | 39.46 ms | 40.22 ms | 40.35 ms |
| errors | 0 | 0 | 0 |

That third column is the honest comparison — same machine, same afternoon. The refactored build is within noise of the old one on every metric, and marginally faster on the stress run.

Correctness held too: after both runs, `verify-invariants.sql` reported **zero negative-stock rows** and **zero per-SKU discrepancy rows**. On the stress run that's 1,077,054 units scanned, 789,314 decremented, and 287,740 clamped-and-audited across 13 exhausted SKUs — all reconciling exactly.

And 25 unit tests pass: 7 layering rules, 13 API contract tests, 5 on reset defaults.

## What I'd do next — ~1 min

**The DTO fix was the pragmatic one, not the pure one.** I moved the wire DTOs into a shared `contract/` package so the services stopped importing the API layer. The textbook fix is for services to return *domain* types and have controllers map domain to DTO. That's strictly better — the business layer wouldn't know the wire format exists — but it means new result records and mapper code for every endpoint. I took the package move because it fixed the dependency direction for about ten lines of import changes. I'd do the full mapping if this were going to production.

**`ResetService` still returns a raw `Map<String, Object>`.** See [ResetService.java:47](server/src/main/java/com/school/selfcheckout/admin/ResetService.java#L47). It's an untyped wire shape assembled in a service — exactly the thing I cleaned up everywhere else. It survived because it's an admin route that isn't part of the contract, which is a reason and not an excuse.

**No upper bound on `limit`.** [PopularityService.java:135](server/src/main/java/com/school/selfcheckout/analytics/PopularityService.java#L135) clamps `limit <= 0` up to 10, but nothing caps the top end, so `?limit=100000` goes straight into a SQL `LIMIT`. Not a layering problem, just a validation gap I noticed and didn't fix.

**And the bigger one:** the correctness guarantees here all come from a single database transaction. That's why the concurrency story is so clean — and it's exactly what makes splitting this into services hard. The moment inventory lives in its own process, that conditional `UPDATE` stops being the answer and I need something that coordinates across processes. `analytics/` would come out easily. `transactions/` would not.
