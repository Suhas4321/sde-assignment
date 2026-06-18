# Post-Call Processing Pipeline — Design Document

**Author:** Antigravity (Principal AI Software Engineer)  
**Date:** June 17, 2026  

---

## 1. Assumptions

1. **Hot Call Distribution**: Around 10-15% of the total call volume contains high-value keywords (like `confirm`, `booked`, `appointment`, `demo`, `escalate`, `manager`, `angry`) that make them candidates for the "hot lane".
2. **Exotel Recording Availability**: Recordings from Exotel become available dynamically between 10 seconds and 90 seconds after a call ends under normal load. We assume a worst-case delay of up to 3 minutes, which justifies a max wait of 195 seconds (~3 minutes) across 6 retry attempts.
3. **LLM average tokens**: A single LLM analysis call consumes 1,500 tokens on average (input + output).
4. **Guaranteed vs. Shared Budget Allocation**: We assume that enterprise customers purchase explicit guaranteed token budgets (e.g. 30k tokens/min), standard customers get a smaller default allocation (e.g. 15k tokens/min), and any leftover capacity forms the shared overflow pool.
5. **No Call Duration/Content LLM analysis for short hangups**: We assume calls with transcripts of less than 4 turns represent hangups or wrong numbers and do not need any LLM analysis, allowing them to bypass LLM processing and save token quota.
6. **Persistence priority**: We assume that database transactions to Postgres (SSD backed) can easily handle peak concurrency of enqueuing 100K tasks, utilizing connection pooling.

---

## 2. Problem Diagnosis

The starting implementation breaks at scale due to these critical design flaws:
1. **Coupled & Blocking Pipeline**: Recording fetching and LLM analysis were run sequentially. A Celery worker thread remained idle and blocked for a hardcoded `asyncio.sleep(45s)` waiting for Exotel audio before it could execute LLM analysis. This blocked the entire worker pool.
2. **Volatile Task State**: Task states lived solely in Redis/Celery. A Redis restart or Celery crash resulted in permanent, silent loss of all pending post-call tasks, violating the "No analysis result may be permanently lost" constraint.
3. **No Rate Limit Awareness**: The LLM API limits (500 RPM / 90,000 TPM) were completely ignored. In a massive campaign, concurrent workers instantly flooded the LLM provider with API calls, resulting in cascading `429 Too Many Requests` errors and worker failures.
4. **Blunt Circuit Breaker**: If RPM exceeded 90% capacity, the system would shut down all outbound dialling for the affected agent for 30 minutes. This caused massive lost revenue for standard spikes, cross-customer.
5. **No Token Isolation**: A single customer running a large campaign could consume 100% of the LLM quota, completely starving other enterprise customers on the same platform.
6. **No Auditability/Observability**: Log levels were set to `DEBUG` for critical recording failures, and there was no correlation or tracing system, making debugging failed interactions in production impossible.

---

## 3. Architecture Overview

To resolve these bottlenecks, we decouple the recording and LLM pipelines and insert robust scheduling and state persistence layers.

```
                    Exotel Call Disconnect Webhook
                                │
                                ▼
                       FastAPI Endpoint
                                │
        ┌───────────────────────┴───────────────────────┐
        ▼                                               ▼
1. Triage Classification                        2. Durable Tasks Created
   - Short call (< 4 turns)                        - recording_upload (Pending)
   - Keyword Match (Hot/Cold)                      - llm_analysis (Pending)
        │                                               │
        │                                               ▼
        │                                        Postgres DB
        ▼                                               │
  Celery Queue                                   (Durable State)
        │                                               │
        ├───────────────────────────────────────────────┤
        │                                               │
        ▼                                               ▼
 [Recording Upload Task]                        [LLM Analysis Task]
  - Polling with Exponential Backoff             - Check Token Budget (token_budget.py)
    schedule: [5, 10, 20, 40, 60, 60]s           - Check Global Rate Limits (rate_limiter.py)
  - Download & Upload to S3                      - Execute LLM / Refund unused budget
  - Update recording_s3_key                      - Trigger signal jobs & lead updates
```

### Key design decisions

1. **Asynchronous Decoupling**: Recording fetching and LLM analysis are executed as independent Celery tasks. LLM analysis does not wait for audio uploads.
2. **Durable Task Lifecycle**: Every task is recorded in a Postgres table `processing_tasks` before dispatch. Celery workers atomically claim tasks from Postgres. If Celery restarts, tasks resume from Postgres.
3. **Double-Bucket Limiting**: A global rate limiter manages both Request and Token limits using a Redis-backed Lua script for atomic capacity reservation.
4. **Per-Customer Token Isolation**: Customers have guaranteed limits with a shared pool fallback. If limits are exceeded, the task reverts to `PENDING` and Celery retries with a countdown delay, without blocking the worker thread.
5. **Rule-Based Triage**: Keywords classify calls into `hot` and `cold` lanes instantly without incurring LLM cost.

---

## 4. Rate Limit Management

We implement a Redis-backed Distributed Token Bucket Limiter to manage LLM API rate limits (500 RPM / 90K TPM).

### How you track rate limit usage
Usage is tracked in Redis counters:
* `rate_limit:tpm:tokens`: Number of tokens currently available in the TPM bucket.
* `rate_limit:tpm:last`: Timestamp of the last bucket refill.
* `rate_limit:rpm:tokens`: Number of requests available in the RPM bucket.
* `rate_limit:rpm:last`: Timestamp of the last RPM refill.

Every check and reservation is performed atomically inside Redis using a Lua script:
1. Calculates time elapsed since the last refill.
2. Increments both TPM and RPM token levels proportionally to refill rates ($1,500$ tokens/sec and $8.33$ requests/sec).
3. If both buckets have sufficient tokens (`current_tpm >= estimated_tokens` and `current_rpm >= 1`), decrements the requested capacity and returns success.
4. If tokens are insufficient, returns failure.

### How you decide what to process now vs. defer
* **Short Calls**: Skipped immediately (no LLM tokens spent).
* **Hot Lane Calls**: Prioritized through Celery's priority queuing (Priority 10). They consume guaranteed budgets or global limits first.
* **Cold Lane Calls**: Processed in Celery with low priority (Priority 1). If rate limits are constrained, they are deferred.
* **Deferral Trigger**: If `acquire` fails, the task state in Postgres is set back to `PENDING` and a Celery retry is triggered with a countdown of 5 seconds. This frees up the worker thread to process other tasks.

### What happens when the limit is hit (recovery, not crash)
The system does not crash or raise uncaught 429s. If the rate limit is hit, the task is deferred back to the Celery broker to run in 5 seconds. Unused customer budgets are immediately refunded. The rate limiter also logs an `ALERT: Rate limit warning` if bucket capacity drops below 20%.

---

## 5. Per-Customer Token Budgeting

Per-customer limits are handled by `TokenBudgetManager` in `src/services/token_budget.py`.

* **Capacity Allocation**: The database defines guaranteed TPM and RPM allocations per customer. The shared pool capacity is computed as `Global Limit - Sum(Guaranteed Allocations)`.
* **Guarantees**: Active customer budgets are isolated. Customer A cannot consume Customer B's pre-allocated guaranteed tokens.
* **Over-budget Fallback**: If a customer exceeds their guaranteed limits, the Lua reservation script falls back to checking the shared overflow pool. If shared capacity is available, it is claimed.
* **Headroom Recycling**: If a customer is under-utilizing their budget, that capacity automatically remains as shared headroom for other customers to utilize.
* **Redis Caching**: Budget configurations are cached in Redis with a 300s TTL to prevent database queries on every call end.
* **Post-Call Budget Refunding**: Because we estimate usage (~1,500 tokens) beforehand, `release_budget` is called upon LLM completion to refund the difference (`estimated_tokens - actual_tokens`) back to the customer's used token counter.

---

## 6. Differentiated Processing

We implement rule-based call triage in [triage.py](file:///d:/Desktop/sde-assignment/src/services/triage.py) which analyzes raw transcripts for keywords before sending tasks to workers:
* **Skip Lane**: Short calls (< 4 turns) skip the LLM analysis entirely and trigger downstream workflows directly, preventing token waste.
* **Hot Lane (Urgent)**: Transcripts matching priority keywords (`confirm`, `reschedule`, `escalate`, `complaint`, etc.) are enqueued with high Celery priority (`priority=10`).
* **Cold Lane (Standard)**: Standard calls are enqueued with normal Celery priority (`priority=1`).

This is fast (O(N) search on transcript strings), cost-free (no LLM calls), and highly configurable per campaign.

---

## 7. Recording Pipeline

Replaced `asyncio.sleep(45)` with a poll-and-retry loop using exponential backoff:
* **Schedule**: Polling delay schedule of `[5, 10, 20, 40, 60, 60]` seconds.
* **Error Handling**: A `404` status code from Exotel indicates the recording is not ready yet, and the poller continues polling. Other HTTP status codes (e.g., `500`, `403`) raise exceptions which trigger standard retries.
* **Visibility**: On-call engineers can easily track failures. Every attempt logs a structured message (`recording_attempt_failed`). If all retries are exhausted, it logs a high-severity `recording_failed_permanently` error and sets `recording_status = "failed"` in Postgres.

---

## 8. Reliability & Durability

* **Durable Postgres Backing**: When a webhook is received, FastAPI inserts a row into `processing_tasks` in Postgres with `status = "pending"`.
* **Atomic Claiming**: Celery workers run a transaction to atomically claim the task (`claim_task`) by updating status to `in_progress`.
* **Worker Crashes**: A periodic sweeper job running every 60 seconds scans for tasks stuck in `in_progress` for over 10 minutes (indicating worker crash) and resets them to `pending` with `attempt_count = attempt_count + 1`.
* **Dead Letter Queue (DLQ)**: If a task fails and exceeds `max_attempts` (default 5), it is marked as `dead_letter` for manual operational replay, ensuring zero silent drops.

---

## 9. Auditability & Observability

We implement structured audit logging. Every log entry includes: `interaction_id`, `customer_id`, `campaign_id`, and `timestamp`.

### What you log (and what fields every log event includes)
Every milestone inserts a row in the Postgres `audit_log` table:
* `received` / `enqueued` (Webhook endpoint)
* `worker_started` (Task pickup)
* `recording_attempt` / `recording_success` / `recording_failed`
* `llm_started` / `llm_completed` / `llm_failed`
* `signal_dispatched` / `lead_updated`
* `completed` / `failed_permanently`

### Alert conditions
We log alerts using the prefix `ALERT:` which are immediately scraper-ready for Datadog or Grafana:
* **Processing Backlog Alert**: Logs `ALERT: Processing backlog threshold exceeded` if pending tasks count > 10,000 (throttled via Redis to once every 10 seconds).
* **Recording Failure Alert**: Logs `ALERT: Recording failure rate high` if failures exceed 10% in the last 15 minutes.
* **Rate Limit Warning**: Logs `ALERT: Rate limit warning` if available tokens drop below 20%.
* **Budget Exhaustion**: Logs `ALERT: Customer budget exceeded` if a customer exhausts guaranteed and shared allocations.
* **Dead Letter Alert**: Logs `ALERT: Dead letter task created` when a task fails permanently.

---

## 10. Data Model

Below is the database schema additions implemented in [migration_001.sql](file:///d:/Desktop/sde-assignment/data/migration_001.sql):

```sql
CREATE TYPE task_status AS ENUM ('pending', 'in_progress', 'completed', 'failed', 'dead_letter');
CREATE TYPE task_priority AS ENUM ('hot', 'cold', 'skip');

-- Customer limits
CREATE TABLE customer_token_budgets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID UNIQUE NOT NULL,
    tokens_per_minute INTEGER NOT NULL,
    requests_per_minute INTEGER NOT NULL,
    priority VARCHAR(50) DEFAULT 'standard',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Durable Task tracking
CREATE TABLE processing_tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL REFERENCES interactions(id) ON DELETE CASCADE,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    task_type VARCHAR(100) NOT NULL,
    priority task_priority DEFAULT 'cold',
    status task_status DEFAULT 'pending',
    attempt_count INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 5,
    payload JSONB DEFAULT '{}',
    result JSONB DEFAULT '{}',
    error_message TEXT,
    next_retry_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Write-only audit log
CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID REFERENCES interactions(id) ON DELETE SET NULL,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    step VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    attempt INTEGER DEFAULT 1,
    event_metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Customer Configuration Settings
CREATE TABLE customer_configs (
    customer_id UUID PRIMARY KEY,
    hot_keywords VARCHAR(100)[] DEFAULT ARRAY['confirmed', 'booked', 'scheduled', 'appointment', 'manager', 'escalate', 'complaint', 'urgent', 'demo', 'meeting', 'canceled', 'refund'],
    crm_webhook_url VARCHAR(500),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

---

## 11. Security

* **Sensitive Data at Rest**: We have implemented column-level encryption at rest for `interactions.conversation_data` (transcripts) and `processing_tasks.payload` / `result` (task parameters and results). This uses a custom SQLAlchemy `EncryptedJSONB` TypeDecorator utilizing AES-128 encryption in CBC mode with SHA256 HMAC through the `cryptography` library's Fernet scheme. It maintains 100% backward compatibility for existing unencrypted JSON/JSONB fields.
* **Recording Protection**: S3 keys (`recording_s3_key`) are stored in Postgres. S3 buckets should enable Server-Side Encryption (SSE-S3) and only expose recordings using temporary pre-signed URLs (valid for 15 minutes) rather than public read ACLs.
* **Transit Security**: All database and Redis connections must enforce SSL/TLS. External webhook endpoints run under HTTPS.

---

## 12. API Interface

We kept the API contract `POST /session/{session_id}/interaction/{interaction_id}/end` completely backward compatible:
* **Why**: Telephony webhook handlers (like Exotel) are hardwired to hit specific endpoints and expect a fast response (< 5s). Keeping the path and parameters unchanged avoids breaking integrations.
* **Change Under the Hood**: Instead of sequentially executing processing logic, the endpoint triages calls in milliseconds, records pending tasks in Postgres, and returns a success response immediately while Celery processes the tasks in parallel.

---

## 13. Trade-offs & Alternatives Considered

| Option | Why Considered | Why Rejected / What We Chose Instead |
|--------|---------------|--------------------------------------|
| **Redis for Task Durability** | Simple to use with Celery. | Rejected because Redis loses task states on restart or OOM. We chose Postgres backing for durable task states. |
| **LLM-based Triage** | High accuracy in prioritizing calls. | Rejected due to cost and latency. We chose rule-based keyword classification since it is instant and free of cost. |
| **Locking Outbound Dialler** | Keeps LLM load completely clean. | Tripping outbound dialling halts business operations entirely. We chose proportional backpressure to slowly throttle calling rates. |
| **Direct Webhook Push** | Simple inline webhook requests inside Celery. | Rejected because transient webhook outages cause permanent data loss. We chose creating a deferred, durable `crm_push` task so it gets automatically retried with exponential backoff and DLQ promotion on failure. |

---

## 14. Known Weaknesses

* **Lua script block time**: In highly distributed clusters, extremely frequent evaluations of Lua scripts on Redis can degrade performance if Redis is single-threaded. However, our Lua script is optimized to run in O(1) time.
* **Database scale limit**: Writing 100K task states and audit logs to Postgres under peak load could cause write amplification. This can be mitigated by utilizing batch inserts or queueing audit logs through a Kafka topic in production.

---

## 15. What I Would Do With More Time

1. **Automated Operational Replay**: Create an API endpoint `/api/v1/tasks/{task_id}/replay` that allows operators to manually retry a task that was promoted to the Dead Letter Queue.
2. **Dynamic Customer Budget Adjustment**: Add a feedback loop that adjusts customer budget allocations in real-time based on their contract tier and current platform traffic load.
3. **Database Write Batching**: Introduce bulk inserts/updates for the audit logs to minimize write overhead during high concurrency peaks (100K calls campaign).

