# 🔴 File 2: All Problems Explained With Their Solutions

> **Purpose:** This file explains each of the 7 critical problems in the current system, WHY they're problems, and WHAT the solution approach should be.
> **No code is implemented here — this is purely an explanation document.**

---

## Problem 1: NO RATE LIMIT AWARENESS (The #1 Problem)

### What Is a Rate Limit?

When you use an LLM API like OpenAI's GPT-4, the provider sets **hard limits** on how much you can use per minute:
- **Requests per minute (RPM):** Max 500 API calls per minute
- **Tokens per minute (TPM):** Max 90,000 tokens per minute

If you exceed these limits, the API returns a **429 Too Many Requests** error and refuses to serve you.

### What's Happening Right Now?

Look at `src/services/post_call_processor.py`, line 108:
```python
response = await self._call_llm(prompt)
```

There is **zero** checking before this line. The system just fires the LLM request hoping it'll work.

Now imagine a campaign:
```
100,000 calls complete in a few hours
→ Each call triggers a Celery task
→ Each task calls the LLM without checking limits
→ At peak: 5,000+ requests/min are attempted
→ LLM provider limit: 500/min
→ Result: 4,500+ requests get 429 errors
→ Celery retries pile up → Redis fills → MORE failures
→ Complete system meltdown
```

The config file (`config.py`) DEFINES the limits:
```python
LLM_TOKENS_PER_MINUTE = 90000
LLM_REQUESTS_PER_MINUTE = 500
```
But **nothing in the codebase reads these values before making a request**. They're documentation, not enforcement.

### Why This Is The Core Problem

At 1,500 tokens per call analysis, you can process:
- 90,000 ÷ 1,500 = **60 calls per minute** (token-limited)
- Even if RPM allows 500, tokens limit you to 60

100,000 calls ÷ 60 per minute = **~28 hours** to process everything. A campaign runs for ~8 hours. You're mathematically behind before you start, which means you MUST be smart about WHAT to process and WHEN.

### The Solution: Token Bucket Rate Limiter

**Concept:** Imagine a bucket that fills with "tokens" at a steady rate. Each LLM request takes tokens out of the bucket. If the bucket is empty, the request has to wait until more tokens drip in.

**How it works:**
```
BUCKET_SIZE = 90,000 tokens
REFILL_RATE = 90,000 tokens per minute (= 1,500 tokens per second)

Before each LLM request:
1. Calculate how many tokens THIS request will use (~1,500)
2. Check: does the bucket have enough tokens?
   YES → Take the tokens, make the request
   NO → Wait until enough tokens have refilled
3. After LLM response: record ACTUAL tokens used (from the response)
```

**Implementation approach:**
- Use Redis to store the bucket state (current level, last refill time)
- A scheduler sits between the task queue and the LLM API
- Requests get queued and released at the rate the bucket allows
- This guarantees we NEVER exceed the rate limit

**Why Token Bucket?**
- It's the standard algorithm used by API providers themselves
- It allows bursts (if you haven't used your quota, you can use it quickly)
- It naturally throttles when sustained load is high
- It's simple to implement and understand

---

## Problem 2: THE 45-SECOND RECORDING SLEEP

### What's Happening Right Now?

Look at `src/services/recording.py`, line 60:
```python
await asyncio.sleep(settings.RECORDING_WAIT_SECONDS)  # 45 seconds!
```

The system literally PAUSES for 45 seconds. Then tries ONCE to fetch the recording. If it's not there, it gives up silently.

### Why This Is Bad

**Three failure modes:**

1. **Recording ready in 10 seconds:** The system wastes 35 seconds sitting idle. During those 35 seconds, the LLM analysis ALSO can't start (because recording and analysis run sequentially in the Celery task). That's 35 seconds of wasted LLM capacity per call × 100,000 calls = ~41 DAYS of wasted worker time.

2. **Recording ready in 60 seconds:** The system already gave up after 45 seconds. The recording is silently lost — no alert, no retry, no record that it was even attempted. The failure is logged at `DEBUG` level, which is **invisible in production** where log level is `INFO`.

3. **Recording never available:** (e.g., call was never connected). The system can't distinguish this from "not ready yet." Both return `None`.

**And the biggest issue:** Recording upload and LLM analysis are **completely independent**. The LLM reads the transcript TEXT, not the audio recording. There's zero reason they should run sequentially.

### The Solution: Polling with Exponential Backoff + Decoupling

**Part A: Replace sleep with a polling loop**
```
Instead of: sleep(45s) → try once → give up

Do this:
Attempt 1: try immediately
Attempt 2: wait 5s → try
Attempt 3: wait 10s → try
Attempt 4: wait 20s → try
Attempt 5: wait 40s → try
Attempt 6: wait 60s → try (final attempt)
Total: tries 6 times over ~135 seconds with increasing gaps
```

This is called **exponential backoff** — each wait is roughly double the previous one. Benefits:
- Catches recordings that are ready quickly (10s)
- Also catches recordings that take longer (60–90s)
- Doesn't hammer the Exotel API with constant requests

**Part B: Run recording upload separately from LLM analysis**

Instead of:
```
Step 1: sleep 45s → fetch recording (blocks everything)
Step 2: LLM analysis (can't start until step 1 finishes)
```

Do this:
```
Trigger TWO independent tasks:
Task A: Recording poller (runs its retry loop)
Task B: LLM analysis (starts IMMEDIATELY, doesn't wait for recording)
Both run in parallel. Both results are saved to the database independently.
```

**Part C: Always log failures at ERROR or WARNING level**

Every recording failure must produce a structured, visible log:
```python
logger.error(
    "recording_fetch_failed",
    extra={
        "interaction_id": interaction_id,
        "call_sid": call_sid,
        "attempts": 6,
        "total_wait_seconds": 135,
        "final_status": "not_available",
    }
)
```

---

## Problem 3: TASKS SILENTLY DROP ON INFRASTRUCTURE FAILURE

### What's Happening Right Now?

The system has TWO places where task state lives:
1. **Celery's broker** — Redis (the pending task queue)
2. **PostCallRetryQueue** — Also Redis (the retry queue)

Both live in Redis. Redis is an **in-memory** data store. If Redis restarts:
- All pending Celery tasks → **GONE**
- All retry queue entries → **GONE**
- All retry state counters → **GONE**
- No record that any of this was ever pending

The assignment constraint says: **"No analysis result may be permanently lost."** The current system violates this fundamentally.

### Why Redis-Only State Is Dangerous

Redis is designed for **speed**, not **durability**. Even with persistence enabled:
- `RDB` snapshots happen periodically → data between snapshots is lost
- `AOF` (append-only file) adds latency
- A memory overflow causes eviction of keys

For task state that MUST NOT be lost, you need a **durable store** — one that guarantees writes survive crashes. That's what **PostgreSQL** is for.

### The Solution: Postgres-Backed Task State

**Core idea:** Instead of relying on Redis to remember "interaction X needs processing," write that state to a PostgreSQL table. PostgreSQL uses WAL (Write-Ahead Logging) — every write is guaranteed to survive a crash.

**New table: `processing_tasks`**
```
processing_tasks:
  id                  UUID (primary key)
  interaction_id      UUID (foreign key to interactions)
  task_type           VARCHAR ("llm_analysis", "recording_upload", "signal_jobs")
  status              VARCHAR ("pending", "in_progress", "completed", "failed", "dead_letter")
  attempt_count       INTEGER
  max_attempts        INTEGER
  payload             JSONB
  error_message       TEXT
  next_retry_at       TIMESTAMPTZ
  started_at          TIMESTAMPTZ
  completed_at        TIMESTAMPTZ
  created_at          TIMESTAMPTZ
```

**How it works:**
1. When a call ends: INSERT a row into `processing_tasks` with status="pending"
2. A worker picks it up: UPDATE status to "in_progress"
3. On success: UPDATE status to "completed"
4. On failure: UPDATE status to "failed", increment attempt_count, set next_retry_at
5. If max_attempts exceeded: UPDATE status to "dead_letter" (never silently dropped)

**A separate "sweeper" process** periodically queries:
```sql
SELECT * FROM processing_tasks
WHERE status = 'in_progress'
AND started_at < NOW() - INTERVAL '10 minutes'
```
These are tasks that started but never completed (worker crash). Reset them to "pending" for reprocessing.

**Benefits:**
- Redis can restart freely — task state is safe in Postgres
- Every task has a visible status (queryable from dashboard)
- Dead-letter tasks are visible and can be replayed manually
- Full history of attempts and errors

---

## Problem 4: THE BLUNT CIRCUIT BREAKER

### What's Happening Right Now?

Look at `src/services/circuit_breaker.py`:

```python
if usage_ratio >= self._capacity_threshold:  # >= 90%
    self._trip(agent_id)                      # FREEZE for 1800 seconds
    return False
```

The circuit breaker has exactly TWO states:
- **CLOSED (normal):** Allow all calls
- **OPEN (tripped):** Block ALL calls for 30 minutes

There is no middle ground. At 89% usage → full speed. At 90% → complete stop.

### Five Things Wrong With This

| # | Issue | Explanation |
|---|-------|-------------|
| 1 | **Binary response** | No "slow down a bit" option. It's either full throttle or complete stop. |
| 2 | **Wrong granularity** | Freezes at `agent_id` level. One customer's overload freezes ALL customers. |
| 3 | **Measures wrong metric** | Tracks RPM (requests/min) but LLM limits on TPM (tokens/min). A 10-turn call uses 3× more tokens than a 3-turn one. |
| 4 | **Too late** | `record_postcall_start()` runs AFTER deciding to fire the request. The check happens in the dialler (before making calls), but the actual limiter should be before making LLM API calls. |
| 5 | **No visibility** | When tripped, logs "circuit_breaker_tripped" but doesn't say WHY — was it LLM overload? Redis glitch? Queue backlog? |

### The Solution: Gradual Backpressure

Instead of binary freeze, give the dialler a **utilization signal**:

```
LLM usage < 50%  → dialler runs at full speed
LLM usage 50-70% → dialler slows to 75% speed
LLM usage 70-85% → dialler slows to 50% speed
LLM usage 85-95% → dialler slows to 25% speed
LLM usage > 95%  → dialler slows to 5% speed (but never fully stops)
```

This is called **proportional backpressure**. Benefits:
- Never a complete freeze
- Naturally slows down when LLM is busy
- Recovers gradually as LLM headroom opens up
- Can be per-customer instead of per-agent

**Implementation:** Instead of `check_capacity() → bool`, return a `float` (0.0 to 1.0) representing recommended throttle level. The dialler adjusts its call rate accordingly.

---

## Problem 5: NO PER-CUSTOMER TOKEN BUDGETING

### What's Happening Right Now?

All customers share ONE LLM quota. There's no tracking of who uses what. If Customer A runs a 100K-call campaign, they consume ALL the LLM capacity and Customer B's calls sit unprocessed for hours.

The code logs `customer_id` with each analysis but never aggregates it:
```python
# From post_call_processor.py, line 128:
# tokens_used is logged here but never written back to any
# counter that could enforce a per-customer budget.
```

### Why This Matters

Imagine you're running this platform:
- Customer A: Cashify, pays for premium plan, guaranteed 30% of LLM capacity
- Customer B: Small startup, basic plan, gets "best effort"
- Customer C: Enterprise, pays for 50% of capacity

Without budgeting:
- Customer A's campaign starts → fires 100K LLM requests → uses ALL capacity
- Customer B's and C's calls pile up → their dashboards show "processing" for hours
- Customer C calls to complain: "We're paying for guaranteed capacity!"

### The Solution: Per-Customer Token Budgets

**Core idea:** Divide the total LLM capacity (90,000 tokens/min) across customers with allocated and shared pools.

**Example allocation:**
```
Total: 90,000 tokens/min

Customer A: 20,000 tokens/min guaranteed (pre-allocated)
Customer B:  5,000 tokens/min guaranteed
Customer C: 30,000 tokens/min guaranteed
Shared pool: 35,000 tokens/min (available to anyone who needs more)

Total allocated: 55,000 + 35,000 shared = 90,000
```

**Rules:**
1. Each customer can ALWAYS use their guaranteed allocation
2. If a customer needs MORE, they can dip into the shared pool (if available)
3. If a customer EXCEEDS their budget + shared pool → their requests are deferred (queued for later)
4. If a customer is UNDER their allocation, their unused portion flows to the shared pool temporarily

**Tracking in Redis:**
```
Key: "token_budget:{customer_id}:used" → INCRBY actual_tokens_used, TTL 60s
Key: "token_budget:{customer_id}:limit" → The customer's per-minute allocation
```

Before processing a call:
```python
used = redis.get(f"token_budget:{customer_id}:used") or 0
limit = redis.get(f"token_budget:{customer_id}:limit")

if used + estimated_tokens > limit:
    # Check shared pool
    shared_used = redis.get("token_budget:shared:used") or 0
    shared_limit = redis.get("token_budget:shared:limit")
    
    if shared_used + estimated_tokens > shared_limit:
        # DEFER: queue this for later processing
        defer_to_cold_lane(interaction)
    else:
        # Use shared pool
        redis.incrby("token_budget:shared:used", estimated_tokens)
        process_now(interaction)
else:
    redis.incrby(f"token_budget:{customer_id}:used", estimated_tokens)
    process_now(interaction)
```

---

## Problem 6: NO DIFFERENTIATED PROCESSING (Everything Gets Same Treatment)

### What's Happening Right Now?

Look at `src/api/endpoints.py`, line 152:
```python
task = process_interaction_end_background_task.apply_async(
    args=[celery_payload],
    queue="postcall_processing",  # One queue to rule them all
)
```

Every single call — whether it's a confirmed booking worth thousands of dollars or a "wrong number" hangup — goes into the **same queue** with the **same priority**.

### Why This Matters

From the sample transcripts:
- **`rebook_confirmed`**: Customer confirmed an appointment. The sales team needs to know NOW so they can prepare. If this waits 3 hours in a queue, the appointment might be missed.
- **`escalation_needed`**: An angry customer wants a manager to call within 60 minutes. If this waits in queue, the customer files a formal complaint.
- **`not_interested`**: Customer said no. Whether this gets analyzed now or in 6 hours doesn't matter — the outcome is the same.

**The business impact:** High-value calls (confirmed bookings, escalations) generate immediate revenue and prevent churn. Low-value calls (not interested, already purchased) are just record-keeping.

### The Solution: Hot Lane vs Cold Lane

**Two separate processing paths:**

```
                    Call ends
                        │
                        ▼
              ┌─────────────────┐
              │  Quick Triage   │
              │  (rule-based)   │
              └────────┬────────┘
                       │
            ┌──────────┴──────────┐
            ▼                     ▼
      🔥 HOT LANE           🧊 COLD LANE
   (immediate processing)   (deferred, batch)
            │                     │
            ▼                     ▼
   Priority LLM queue       Low-priority queue
   Uses guaranteed           Uses leftover
   token budget              capacity
```

**How to classify (triage)?**

You don't need an LLM to figure out which lane a call belongs to. Simple keyword/pattern matching on the transcript works:

```python
HOT_KEYWORDS = [
    "confirmed", "booked", "scheduled", "appointment",
    "manager", "escalate", "complaint", "angry",
    "demo", "meeting"
]

def classify_urgency(transcript_text: str) -> str:
    text_lower = transcript_text.lower()
    for keyword in HOT_KEYWORDS:
        if keyword in text_lower:
            return "hot"
    return "cold"
```

This is **cheap** (no LLM call needed), **fast** (milliseconds), and **good enough** for lane assignment. It doesn't need to be perfect — it just needs to catch the obvious high-value cases.

**Alternatively:** The customer could configure which call outcomes are "hot" for their business. A Cashify campaign might consider "rebook_confirmed" as hot, while an insurance company might consider "policy_renewed" as hot.

---

## Problem 7: NO AUDITABILITY OR OBSERVABILITY

### What's Happening Right Now?

The logging is scattered, inconsistent, and incomplete:

| What's Missing | Where | Impact |
|---------------|-------|--------|
| No correlation ID | endpoints.py | Can't trace a request end-to-end |
| Failures logged at DEBUG | recording.py | Invisible in production |
| No structured error context | circuit_breaker.py | "Circuit tripped" but WHY? |
| No queue depth visibility | celery_tasks.py | Can't see how backed up we are |
| No per-customer usage tracking | metrics.py | Can't answer "how many tokens did Customer X use?" |
| No alerting | everywhere | Nobody knows when things fail |

### What an On-Call Engineer Experiences Today

```
Scenario: Customer calls at 2 AM: "Our dashboard hasn't updated in 3 hours"

Engineer's debugging journey:
1. Open Kibana/CloudWatch → search for interaction_id
2. Find: "postcall_enqueued" — task was sent to Celery ✅
3. Find: ... nothing else. No log saying the Celery task started.
4. Check Redis: Is the task still in the queue? Redis was restarted at 11 PM.
5. The task is simply gone. No trace of what happened.
6. No alert fired. Nobody knew until the customer called.
```

### The Solution: Structured Audit Logging + Alerts

**Part A: Every processing step logs a structured event**

Every log line must include:
```json
{
    "timestamp": "2024-01-15T14:30:00Z",
    "event": "llm_analysis_started",
    "interaction_id": "abc-123",
    "session_id": "def-456",
    "customer_id": "ghi-789",
    "campaign_id": "jkl-012",
    "correlation_id": "req-xyz-001",
    "step": "llm_analysis",
    "attempt": 1,
    "metadata": {
        "queue_wait_time_ms": 3500,
        "estimated_tokens": 1500
    }
}
```

**Events to log for every interaction:**
```
1. postcall_received        — Webhook received
2. postcall_enqueued        — Task created in processing_tasks table
3. postcall_worker_started  — Worker picked up the task
4. recording_poll_attempt   — Each recording fetch attempt
5. recording_poll_success   — Recording found and uploaded
6. recording_poll_failed    — All recording attempts exhausted
7. llm_analysis_started     — About to call LLM
8. llm_analysis_completed   — LLM returned results
9. llm_analysis_failed      — LLM call failed (with error type: 429, timeout, etc.)
10. signal_jobs_dispatched  — Downstream actions triggered
11. lead_stage_updated      — Lead status changed
12. postcall_completed      — All processing finished successfully
13. postcall_failed         — Processing failed permanently (dead-lettered)
```

**Part B: Audit trail in the database**

New table: `audit_log`
```
audit_log:
  id                UUID
  interaction_id    UUID
  event_type        VARCHAR (from the list above)
  step              VARCHAR
  status            VARCHAR (started, completed, failed)
  attempt           INTEGER
  metadata          JSONB (tokens_used, error_message, latency, etc.)
  created_at        TIMESTAMPTZ
```

This means an engineer can query:
```sql
SELECT * FROM audit_log
WHERE interaction_id = 'abc-123'
ORDER BY created_at;
```
And see the complete journey of that interaction.

**Part C: Alert conditions**

| Alert | Condition | Why |
|-------|-----------|-----|
| Rate limit warning | Token usage > 80% of limit for 5 min | About to hit 429s |
| Rate limit critical | Any 429 response received | Already hitting limits |
| Recording failure spike | > 10% of recordings failing in last 15 min | Exotel may be having issues |
| Processing backlog | Queue depth > 10,000 tasks | Dashboard updates will be delayed |
| Dead letter created | Any task moved to dead_letter status | A task permanently failed |
| Customer budget exceeded | Any customer at > 100% budget | Their processing is being throttled |

---

## Summary: All 7 Problems at a Glance

| # | Problem | Current Behavior | Impact | Fix |
|---|---------|-----------------|--------|-----|
| 1 | No rate limit awareness | LLM requests fire at full speed | 429 errors, cascading failures | Token bucket rate limiter |
| 2 | 45-second recording sleep | Sleep 45s, try once, give up | Wasted time, missed recordings | Exponential backoff polling + decouple from LLM |
| 3 | Tasks silently drop | All state in Redis (volatile) | Redis restart = permanent data loss | Postgres-backed task state |
| 4 | Blunt circuit breaker | Binary: 90% → freeze 30 minutes | Complete dialler halt, lost revenue | Gradual proportional backpressure |
| 5 | No per-customer budgeting | All customers share one quota | One customer starves others | Per-customer token budgets with shared pool |
| 6 | No differentiated processing | All calls in same queue, same priority | High-value calls delayed for hours | Hot lane / cold lane classification |
| 7 | No auditability | Scattered logs, no correlation ID | Can't debug failures | Structured logging + audit trail table + alerts |

---

**Next:** See `3_TASK_BREAKDOWN.md` for the step-by-step plan of what to implement and in what order.
