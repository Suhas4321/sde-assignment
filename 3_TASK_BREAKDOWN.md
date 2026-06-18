# 📋 File 3: Task Breakdown — What to Build, Step by Step

> **Purpose:** This file breaks down EVERY task you need to do, explains WHAT each one means, and gives the recommended ORDER to tackle them.
> **No code is implemented here — this is purely a planning document.**

---

## Overview: What You're Delivering

You need to submit **two things:**

### 1. SUBMISSION.md — Design Document
A written document explaining your architectural decisions. Use `SUBMISSION_TEMPLATE.md` as the starting point. This covers:
- Your assumptions
- Architecture diagram
- How you handle rate limits, budgets, recording, durability, security, etc.
- Trade-offs you considered

### 2. Code Implementation
Actual working code changes to the codebase. Prioritized into Must/Should/Nice-to-have.

---

## Recommended Order of Attack

I recommend doing things in this order because each step builds on the previous one:

```
Phase 1: Design Document
  └── Write SUBMISSION.md (all 15 sections)

Phase 2: Database Layer (Foundation)
  └── Add new tables for task state, audit log, token budgets

Phase 3: Core Fixes (Must Implement)
  ├── 3A: Structured audit logging
  ├── 3B: Durable task execution (Postgres-backed)
  ├── 3C: Rate limit–aware LLM scheduler
  ├── 3D: Per-customer token budget
  └── 3E: Recording poller with retry/backoff

Phase 4: Should Implement
  ├── 4A: Differentiated processing (hot/cold lanes)
  ├── 4B: Alert thresholds
  └── 4C: Rate limit tests

Phase 5: Nice to Have
  ├── 5A: Gradual backpressure (replacing circuit breaker)
  ├── 5B: CRM push with retry
  ├── 5C: Per-customer config
  └── 5D: Encryption at rest
```

---

## Phase 1: Design Document (SUBMISSION.md)

### What To Do
Copy `SUBMISSION_TEMPLATE.md` to `SUBMISSION.md` and fill in all 15 sections.

### Section-by-Section Guide

#### Section 1: Assumptions
Write down everything you're assuming. Examples:
- "I assume hot (urgent) calls are ~5-10% of total volume"
- "I assume recording delivery time from Exotel follows a distribution of 10-90 seconds"
- "I assume the system can tolerate up to 5 minutes of delay for cold-lane calls"
- "I assume per-customer budget allocations are configured ahead of time, not dynamic"
- "I assume the LLM average of 1,500 tokens per call is roughly accurate"
- "I assume short calls (< 4 turns) never need LLM analysis"

#### Section 2: Problem Diagnosis
Summarize the 7 problems from `2_PROBLEMS_AND_SOLUTIONS.md` in YOUR words. Show you understand the root cause, not just the symptoms.

#### Section 3: Architecture Overview
Draw a diagram of the NEW architecture. Key changes from current:
- Recording pipeline runs SEPARATELY from LLM analysis (parallel, not sequential)
- Rate limiter sits between task queue and LLM API
- Per-customer queues or priority tagging
- Task state in Postgres, not just Redis
- Structured audit log

Example ASCII diagram:
```
POST /session/.../end
       │
       ▼
  FastAPI endpoint
       │
       ├── Write to processing_tasks table (Postgres)
       ├── Write audit_log entry
       │
       ├── Quick triage (rule-based, no LLM)
       │       │
       │   ┌───┴───────────┐
       │   ▼               ▼
       │  HOT LANE      COLD LANE
       │  (priority      (deferred
       │   queue)          queue)
       │
       └── Trigger recording poller (independent task)
                              │
           ┌──────────────────┴──────────────────┐
           ▼                                     ▼
    Rate-Limited LLM Scheduler         Recording Poller
    (token bucket algorithm)           (exponential backoff)
           │                                     │
           ▼                                     ▼
    LLM Analysis                         Fetch from Exotel
    - Respect rate limits                - Retry up to 6 times
    - Track per-customer budget          - Upload to S3
    - Log every step                     - Log every attempt
           │                                     │
           ▼                                     ▼
    Update interaction_metadata          Update recording_s3_key
    Trigger signal_jobs                  Log success/failure
    Update lead stage
           │
           ▼
    Write audit_log: completed
```

#### Sections 4-15
Fill in based on the solutions described in `2_PROBLEMS_AND_SOLUTIONS.md`.

---

## Phase 2: Database Layer

### Task 2A: New Table — `processing_tasks`

**What this is:** A Postgres table to track every background task durably. This replaces relying on Redis (which loses data on restart).

**Why you need it:** The constraint says "No analysis result may be permanently lost." Currently if Redis restarts, everything in the Celery queue and retry queue vanishes. With this table, every task is tracked in Postgres (which survives restarts).

**Columns you need:**
```
id                  - UUID, primary key
interaction_id      - UUID, FK to interactions table
customer_id         - UUID (for budget tracking)
campaign_id         - UUID (for grouping)
task_type           - VARCHAR: "llm_analysis", "recording_upload", "signal_jobs", "lead_update"
priority            - VARCHAR: "hot", "cold", "skip"
status              - VARCHAR: "pending", "in_progress", "completed", "failed", "dead_letter"
attempt_count       - INTEGER, starts at 0
max_attempts        - INTEGER, default 5
payload             - JSONB (the full context needed to process this task)
result              - JSONB (the result after successful processing)
error_message       - TEXT (last error message)
next_retry_at       - TIMESTAMPTZ (when to retry, NULL if not waiting)
started_at          - TIMESTAMPTZ
completed_at        - TIMESTAMPTZ
created_at          - TIMESTAMPTZ
updated_at          - TIMESTAMPTZ
```

**How it will be used:**
1. Endpoint receives webhook → INSERT row with status="pending"
2. Worker picks it up → UPDATE status="in_progress", started_at=NOW()
3. Success → UPDATE status="completed", result={...}, completed_at=NOW()
4. Failure → UPDATE status="failed", error_message="...", attempt_count++, next_retry_at=NOW()+backoff
5. Max retries exceeded → UPDATE status="dead_letter" (visible, never silently dropped)

---

### Task 2B: New Table — `audit_log`

**What this is:** A write-only log table recording every processing step for every interaction.

**Why you need it:** The acceptance criteria say "Every interaction has a complete audit trail." Currently there's no way to trace what happened to a specific interaction.

**Columns you need:**
```
id                  - UUID, primary key
interaction_id      - UUID, FK to interactions
customer_id         - UUID
campaign_id         - UUID
event_type          - VARCHAR: "received", "enqueued", "worker_started", 
                       "recording_attempt", "recording_success", "recording_failed",
                       "llm_started", "llm_completed", "llm_failed",
                       "signal_dispatched", "lead_updated", 
                       "completed", "failed_permanently"
step                - VARCHAR: which processing step
status              - VARCHAR: "started", "completed", "failed"
attempt             - INTEGER
metadata            - JSONB: {tokens_used, latency_ms, error, queue_wait_ms, etc.}
created_at          - TIMESTAMPTZ
```

**How it will be used:**
- At every processing step, INSERT a row
- To debug: `SELECT * FROM audit_log WHERE interaction_id = ? ORDER BY created_at`
- For alerts: `SELECT COUNT(*) FROM audit_log WHERE event_type = 'failed_permanently' AND created_at > NOW() - INTERVAL '1 hour'`

---

### Task 2C: New Table — `customer_token_budgets`

**What this is:** Stores per-customer LLM token allocations.

**Columns you need:**
```
id                  - UUID, primary key
customer_id         - UUID, unique
tokens_per_minute   - INTEGER (guaranteed allocation)
requests_per_minute - INTEGER (request limit)
priority            - VARCHAR: "standard", "premium", "enterprise"
is_active           - BOOLEAN
created_at          - TIMESTAMPTZ
updated_at          - TIMESTAMPTZ
```

**Example data:**
```
Customer A: tokens_per_minute=20000, priority="premium"
Customer B: tokens_per_minute=5000, priority="standard"
Shared pool: calculated as total - sum(allocated)
```

---

### Task 2D: Modify Existing `interactions` Table

**Add columns:**
```
processing_priority   - VARCHAR: "hot", "cold", "skip" (which lane)
llm_tokens_used       - INTEGER (actual tokens consumed, for billing)
recording_status      - VARCHAR: "pending", "polling", "uploaded", "failed", "not_applicable"
recording_attempts    - INTEGER (how many times we tried)
processing_started_at - TIMESTAMPTZ (when background processing began)
processing_completed_at - TIMESTAMPTZ (when all processing finished)
```

**Why:** Currently `interaction_metadata` is a JSONB catch-all. Having explicit columns for key fields makes them queryable and indexable.

---

## Phase 3: Core Fixes (Must Implement)

### Task 3A: Structured Audit Logging

**What to build:** A logging utility that every part of the system uses to write structured, consistent log entries.

**Where it goes:** New file `src/services/audit_logger.py`

**What it does:**
- Provides a function like `log_event(interaction_id, event_type, step, status, metadata)`
- Writes to BOTH:
  - Python logger (for stdout/Kibana)
  - The `audit_log` Postgres table (for queryability)
- Every log entry includes: interaction_id, customer_id, campaign_id, timestamp

**Where to use it:** 
- In `endpoints.py`: log "received" and "enqueued"
- In `celery_tasks.py`: log "worker_started", "completed"
- In `recording.py`: log every poll attempt and final result
- In `post_call_processor.py`: log "llm_started", "llm_completed"/"llm_failed"
- In `signal_jobs.py`: log "signal_dispatched"

---

### Task 3B: Durable Task Execution

**What to build:** Replace the Redis-only task tracking with Postgres-backed task state.

**Where it goes:** 
- New file `src/services/task_manager.py` — manages creating/updating tasks in Postgres
- Modify `endpoints.py` — create a processing_tasks row when enqueuing
- Modify `celery_tasks.py` — update task status as processing progresses

**What it does:**
- `create_task(interaction_id, task_type, priority, payload)` → INSERT into processing_tasks
- `claim_task(task_id)` → UPDATE status="in_progress" WHERE status="pending" (atomic)
- `complete_task(task_id, result)` → UPDATE status="completed"
- `fail_task(task_id, error)` → UPDATE status="failed", schedule retry
- `dead_letter_task(task_id)` → UPDATE status="dead_letter" when max retries exceeded

**The sweeper:** A periodic job (runs every 60 seconds) that:
```sql
-- Find tasks stuck in "in_progress" for too long (worker crash)
UPDATE processing_tasks
SET status = 'pending', attempt_count = attempt_count + 1
WHERE status = 'in_progress'
AND started_at < NOW() - INTERVAL '10 minutes'
RETURNING *;
```

---

### Task 3C: Rate Limit–Aware LLM Scheduler

**What to build:** A token bucket rate limiter that sits between the task queue and the LLM API.

**Where it goes:** New file `src/services/rate_limiter.py`

**What it does:**
The token bucket algorithm:

```
State (stored in Redis for fast access):
  - tokens: current number of available tokens (float)
  - last_refill: timestamp of last token refill

REFILL_RATE = 90,000 tokens per 60 seconds = 1,500 tokens per second
BUCKET_MAX = 90,000 tokens

acquire(estimated_tokens):
  1. Calculate time since last_refill
  2. Add refilled tokens: tokens += elapsed_seconds * REFILL_RATE
  3. Cap at BUCKET_MAX
  4. If tokens >= estimated_tokens:
       tokens -= estimated_tokens
       Update Redis
       return True  (proceed with LLM call)
     Else:
       return False  (must wait)
       Calculate wait_time: (estimated_tokens - tokens) / REFILL_RATE

record_actual_usage(estimated_tokens, actual_tokens):
  # After LLM response, adjust for actual usage
  difference = estimated_tokens - actual_tokens
  tokens += difference  # Give back over-estimated tokens
```

**How it integrates:**
In `post_call_processor.py`, BEFORE calling the LLM:
```python
# Instead of directly calling LLM:
can_proceed = await rate_limiter.acquire(estimated_tokens=1500)
if not can_proceed:
    wait_time = rate_limiter.time_until_available(1500)
    # Either wait, or defer to cold lane
```

**Also need:** A separate RPM (requests per minute) limiter alongside the TPM limiter. Both must pass before an LLM call proceeds.

---

### Task 3D: Per-Customer Token Budget

**What to build:** A budget tracker that allocates LLM capacity per customer.

**Where it goes:** New file `src/services/token_budget.py`

**What it does:**
```
check_budget(customer_id, estimated_tokens):
  1. Look up customer's allocation from customer_token_budgets table
  2. Check their current usage (Redis: "token_budget:{customer_id}:used")
  3. If within budget: allow, increment usage counter
  4. If over budget: check shared pool availability
  5. If shared pool available: allow, increment shared counter  
  6. If everything exhausted: defer (return False)

record_usage(customer_id, actual_tokens):
  - Increment the customer's usage counter in Redis
  - Counter has 60-second TTL (auto-resets each minute)
```

**How it integrates with the rate limiter:**
Before any LLM call, TWO checks must pass:
1. Global rate limiter (Task 3C) — are we within total LLM capacity?
2. Customer budget (Task 3D) — is this customer within their allocation?

Both must say "yes" before the LLM request proceeds.

---

### Task 3E: Recording Poller with Retry/Backoff

**What to build:** Replace `asyncio.sleep(45)` with a proper polling loop.

**Where it goes:** Modify `src/services/recording.py`

**What it does:**
```
New function: poll_and_upload_recording(interaction_id, call_sid, exotel_account_id)

BACKOFF_SCHEDULE = [5, 10, 20, 40, 60, 60]  # seconds between attempts
MAX_ATTEMPTS = 6
TOTAL_MAX_WAIT = 195 seconds (~3 minutes)

For each attempt:
  1. Try to fetch recording URL from Exotel API
  2. If 200 + URL:
     - Download recording
     - Upload to S3
     - Update interaction.recording_s3_key in DB
     - Log SUCCESS (structured)
     - Return s3_key
  3. If 404 (not ready yet):
     - Log attempt (structured: attempt #, wait time)
     - Wait BACKOFF_SCHEDULE[attempt] seconds
     - Continue to next attempt
  4. If error (network, timeout):
     - Log error (structured)
     - Wait and retry

If all attempts fail:
  - Log ERROR (structured, NOT debug level)
  - Update interaction.recording_status = "failed"
  - Insert audit_log entry
  - This MUST be visible to operations team
```

**Key change:** This function runs as a SEPARATE task from LLM analysis. They no longer block each other.

---

## Phase 4: Should Implement

### Task 4A: Differentiated Processing (Hot/Cold Lanes)

**What to build:** A triage mechanism that classifies calls into priority lanes before LLM processing.

**Where it goes:** New file `src/services/triage.py`

**What it does:**
```
classify_priority(transcript_text, conversation_data) -> "hot" | "cold" | "skip"

Rules:
  - If transcript < 4 turns → "skip" (no LLM)
  - If transcript contains hot keywords → "hot"
  - Otherwise → "cold"

HOT_KEYWORDS = [
  "confirmed", "booked", "scheduled", "appointment",
  "manager", "escalate", "complaint", "urgent",
  "demo", "meeting", "canceled", "refund"
]
```

**How it integrates:**
In `endpoints.py`, AFTER loading the interaction:
```python
priority = classify_priority(transcript_text, conversation_data)
# Create processing_task with this priority
# Hot tasks get processed before cold tasks
```

**In the worker:** Process all "hot" tasks first, then "cold" tasks when there's spare LLM capacity.

---

### Task 4B: Alert Thresholds

**What to build:** Alert conditions that fire when things go wrong.

**Where it goes:** Can be part of `metrics.py` or a new `src/services/alerting.py`

**Alert conditions to implement:**

| Alert              | Check                     | Threshold            |
|---------------------------|--------------------------|----------------------|
| Rate limit warning  | Token usage %            | > 80% for 5 min      |
| Processing backlog  | Pending tasks count      | > 10,000             |
| Recording failure   | Failed recordings / total| > 10% in 15 min      |
| Dead letter created | Dead letter count        | Any new entry        |
| Customer budget exceeded | Budget usage %          | > 100%               |

For now, these can just be log entries with a specific format that a monitoring tool (Grafana, PagerDuty) could watch for.

---

### Task 4C: Rate Limit Tests

**What to build:** Tests that verify the rate limiter works correctly.
+
**Where it goes:** New file `tests/test_rate_limiter.py`

**Tests to write:**
1. **test_burst_of_1000_calls_no_429s:** Simulate 1000 calls arriving at once, verify the rate limiter queues them properly and no 429 errors leak through
2. **test_customer_budget_isolation:** Exhaust Customer A's budget, verify Customer B's calls still process
3. **test_short_transcripts_skip_llm:** Send a short transcript, verify zero LLM calls made
4. **test_rate_limiter_token_bucket:** Verify the token bucket correctly limits throughput
5. **test_recording_poller_retries:** Simulate delayed recording, verify retry loop and backoff timing

---

## Phase 5: Nice to Have

### Task 5A: Gradual Backpressure

Replace the binary circuit breaker with proportional slowdown.

**Change:** `check_capacity()` returns a float (0.0 to 1.0) instead of a bool. The dialler uses this to adjust its call rate. See Problem 4 in `2_PROBLEMS_AND_SOLUTIONS.md`.

### Task 5B: CRM Push with Retry

Add retry logic to CRM webhook pushes. Track each push attempt in the audit log. Failed pushes should be retried with exponential backoff.

### Task 5C: Per-Customer Configuration

Allow customers to configure:
- Their hot keywords (what counts as urgent for THEIR business)
- Processing preferences (e.g., "always do full analysis" vs "skip not-interested calls")
- CRM webhook URLs
Store in a `customer_config` table, loadable without code deployment.

### Task 5D: Encryption at Rest

- Encrypt transcript data in the `conversation_data` column
- Encrypt recordings in S3 (use S3 server-side encryption)
- Identify PII fields (phone, email, name) and consider column-level encryption

---

## The Acceptance Criteria Mapped to Tasks

| AC# | Criterion | Which Task Solves It |
|-----|-----------|---------------------|
| AC1 | Never exceed LLM rate limits | Task 3C (Rate limiter) |
| AC2 | Per-customer budget isolation | Task 3D (Token budget) |
| AC3 | Tasks survive Redis/Celery restart | Task 3B (Durable task execution) |
| AC4 | Recording poller retries + logs | Task 3E (Recording poller) |
| AC5 | Complete audit trail per interaction | Task 3A (Audit logging) + Task 2B (audit_log table) |
| AC6 | All errors have structured logs | Task 3A (Audit logging) |
| AC7 | No binary dialler freeze | Task 5A (Gradual backpressure) or design doc |
| AC8 | Short transcripts skip LLM | Task 4A (Triage) — already partially exists |
| AC9 | Assumptions stated clearly | Phase 1 (SUBMISSION.md) |
| AC10 | Security addressed | Phase 1, Section 11 (SUBMISSION.md) |

---

## Files You'll Create or Modify

### New Files (CREATE)
| File | Purpose |
|------|---------|
| `SUBMISSION.md` | Design document (copy from template) |
| `src/services/audit_logger.py` | Structured audit logging |
| `src/services/task_manager.py` | Durable Postgres-backed task management |
| `src/services/rate_limiter.py` | Token bucket rate limiter |
| `src/services/token_budget.py` | Per-customer budget tracking |
| `src/services/triage.py` | Hot/cold lane classification |
| `data/migration_001.sql` | Schema changes (new tables, new columns) |
| `tests/test_rate_limiter.py` | Rate limiting tests |
| `tests/test_recording_poller.py` | Recording poller tests |
| `tests/test_budget.py` | Per-customer budget tests |

### Existing Files to Modify
| File | What Changes |
|------|-------------|
| `src/api/endpoints.py` | Add audit logging, create processing_task, add triage |
| `src/tasks/celery_tasks.py` | Use task_manager, decouple recording from LLM, use rate limiter |
| `src/services/post_call_processor.py` | Add rate limit check before LLM call, track tokens per customer |
| `src/services/recording.py` | Replace sleep(45) with polling loop |
| `src/services/circuit_breaker.py` | Replace binary freeze with gradual backpressure |
| `src/services/metrics.py` | Write metrics to Redis counters, not just logs |
| `src/config.py` | Add new settings (backoff schedule, triage keywords, etc.) |
| `docker-compose.yml` | Probably stays the same |

---

## Git Commit Strategy

The assignment says: "A clean commit history showing your progression is part of the submission."

Recommended commits:
```
1. "docs: add SUBMISSION.md with design document"
2. "schema: add processing_tasks, audit_log, customer_token_budgets tables"
3. "feat: add structured audit logging service"
4. "feat: add durable task manager with Postgres backing"
5. "feat: add token bucket rate limiter for LLM requests"
6. "feat: add per-customer token budget enforcement"
7. "feat: replace recording sleep(45s) with exponential backoff poller"
8. "feat: add hot/cold lane triage for differentiated processing"
9. "refactor: integrate rate limiter and task manager into processing pipeline"
10. "test: add tests for rate limiter, budget, and recording poller"
11. "feat: add alert threshold conditions"
12. "docs: update SUBMISSION.md with implementation details"
```

---

## Ready to Start?

I recommend starting with:
1. **Phase 1** — Write SUBMISSION.md first. This forces you to think through the design before coding.
2. **Phase 2** — Database tables next. This is the foundation everything else builds on.
3. **Phase 3** — Then implement the core fixes one by one.

Let me know which task you want to start with, and I'll guide you through it step by step — explaining the concepts as we go, while YOU write the code!
