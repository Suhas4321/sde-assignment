# 🛠️ sde-assignment Solutions Explanation

This file contains the detailed explanation of each issue we solve in the codebase, how we solve it, and its validation against design patterns and SOLID principles.

---

## 📂 Issue 1: The 45-Second Recording Sleep (Problem 2)

### 1. What was the Issue?
* **Problem:** In `src/services/recording.py`, the system paused for a hardcoded `asyncio.sleep(45)` before trying once to fetch the call recording from Exotel.
* **Impact:** 
  1. If a recording was ready in 10s, we wasted 35s of worker time. Since this blocked the Celery task sequentially, it blocked the LLM analysis from starting.
  2. If a recording took 60s under heavy load, the system gave up after 45s, and the recording was permanently lost.
  3. Failures were logged at the `DEBUG` level, making them invisible in production.

### 2. How did we Solve it?
* **Retry Loop:** Replaced the static sleep with a loop checking Exotel's API with an exponential backoff schedule `[5, 10, 20, 40, 60, 60]`.
* **API Error Differentiation:** Updated `_fetch_exotel_recording_url` so a `404` status (indicating the recording is not ready yet) returns `None` to continue polling, while any other HTTP status error (e.g. `500`, `403`) raises an exception to trigger a retry.
* **Structured Logs:** Replaced `DEBUG` logs with structured `INFO`/`ERROR` logs containing `interaction_id`, `call_sid`, and `attempt` number.

### 3. SOLID & Design Pattern Analysis
* **Single Responsibility Principle (SRP):** The function `fetch_and_upload_recording` is solely responsible for orchestrating the retry flow and handling file upload. `_fetch_exotel_recording_url` is solely responsible for calling Exotel API, and `_upload_to_s3` handles S3 logic.
* **Fail-Fast & Clean Exceptions:** Letting HTTP errors propagate rather than swallowing them ensures callers can differentiate between "recording not ready yet (404)" and "network timeout/error", preventing silent failures.

---

## 📂 Issue 2: No Rate Limit Awareness (Problem 1)

### 1. What is the Issue?
* **Problem:** The LLM provider sets hard limits of **500 Requests Per Minute (RPM)** and **90,000 Tokens Per Minute (TPM)**. The current codebase fires LLM requests instantly without any throttling or tracking.
* **Impact:** At 100K calls per campaign, concurrent workers will flood the API, causing `429 Too Many Requests` responses. This triggers immediate Celery retries, overloading the Redis broker and causing a complete system freeze.

### 2. How will we Solve it?
* **Token Bucket Algorithm:** We will implement a rate limiter where a bucket fills with "tokens" at a constant rate ($1500$ tokens/sec to match the $90,000$ TPM limit). Every LLM request must acquire tokens from the bucket before executing. If tokens are not available, it sleeps/defers.
* **RPM Limiter:** A parallel bucket enforces the 500 RPM limit. Both token and request checks must pass.
* **Distributed State:** The limiter state is stored in Redis so that separate Celery worker processes can share the capacity counters in real-time.

### 3. SOLID & Design Pattern Analysis
* **Single Responsibility Principle (SRP):** We separate the rate-limiting responsibility into a dedicated `AbstractRateLimiter` class, so `PostCallProcessor` does not contain logic for token bucket refills, Redis locking, or retry math.
* **Open/Closed Principle (OCP) & Strategy Pattern:** We define an abstract interface (`AbstractRateLimiter`). In production, we run a `RedisTokenBucketLimiter`. For testing, we swap in a `LocalMemoryRateLimiter` or `NoOpRateLimiter` without changing any consumer code.
* **Liskov Substitution Principle (LSP):** Any subclass of `AbstractRateLimiter` can replace another subclass without crashing the application.
* **Dependency Inversion Principle (DIP):** `PostCallProcessor` relies on the `AbstractRateLimiter` interface, and we inject the concrete rate limiter instance at runtime.
* **Factory Pattern:** We will use a factory to generate different rate limiters (e.g., per-customer buckets or a global bucket) based on settings.

---

## 📂 Issue 3: No Auditability or Observability (Problem 7)

### 1. What is the Issue?
* **Problem:** There is no centralized tracking of a call's post-processing journey. System logs are unstructured, lack correlation IDs, and log failures at the `DEBUG` level. There is no queryable audit trail database table.
* **Impact:** If a customer complains that their CRM did not update or dashboard is out of sync, on-call engineers cannot easily trace what happened, at what step a call failed, or whether a retry was attempted.

### 2. How will we Solve it?
* **Audit Log Table:** We will create a `audit_log` PostgreSQL table storing each transition state: `received`, `enqueued`, `recording_attempt`, `recording_success`, `llm_started`, `llm_completed`, etc.
* **Audit Logger Service:** Build `src/services/audit_logger.py` exposing a clean logger that:
  1. Outputs structured JSON log entries containing `interaction_id`, `customer_id`, `campaign_id`, and `step` to stdout.
  2. Persists the event directly in the Postgres `audit_log` table asynchronously.
* **Alert Conditions:** Define structured trigger patterns (e.g., when the same call fails LLM analysis 3 times, or when recording failure rates spike >10%).

* **Dependency Injection:** The database session is injected or retrieved within the audit logger session context to keep logic decoupled.

---

## 📂 Issue 4: Tasks Silently Drop on Restart (Problem 3)

### 1. What is the Issue?
* **Problem:** In the current system, task state only lives in Redis (the Celery broker). If Redis restarts, all pending calls, retries, and in-progress tasks are lost permanently.
* **Impact:** This violates the core constraint "No analysis result may be permanently lost."

### 2. How will we Solve it?
* **Postgres-Backed Task State:** We will persist the lifecycle of every background job in the `processing_tasks` Postgres table.
* **Task Manager Service:** Create `src/services/task_manager.py` that handles task transitions:
  - `create_task(...)`: Inserts a new task row with status `pending`.
  - `claim_task(...)`: Atomically updates task status to `in_progress` when a worker starts processing.
  - `complete_task(...)`: Marks status as `completed` and stores result payload.
  - `fail_task(...)`: Increments attempt counts, logs the error, and schedules a retry (`next_retry_at`) using exponential backoff.
  - `dead_letter_task(...)`: Marks status as `dead_letter` if max retries are exceeded, so operations can inspect and manually retry it.
* **Sweeper daemon:** A periodic routine to find tasks stuck in `in_progress` for too long (e.g. 10 minutes) indicating worker process crash, and reset them to `pending`.

### 3. SOLID & Design Pattern Analysis
* **State Pattern:** The lifecycle of a task transitions through distinct states (`pending`, `in_progress`, `completed`, `failed`, `dead_letter`). The task manager enforces valid state transitions.
* **Repository Pattern:** `TaskManager` abstracts DB queries away from Celery workers and FastAPI endpoints, serving as a clean data access layer (DAL) for tasks.
* **Single Responsibility Principle (SRP):** Decoupled from Celery orchestration. Celery acts only as a transport mechanism, while the database records absolute truth about task progression.

---

## 📂 Issue 5: No Per-Customer Token Budgeting (Problem 5)

### 1. What is the Issue?
* **Problem:** All customers share a single global LLM token quota. One customer executing a large campaign can exhaust the total rate limit, starving other active customers on the platform.
* **Impact:** High-paying enterprise customers receive no capacity guarantees, and their post-call processing lags due to lower-tier burst campaigns.

### 2. How will we Solve it?
* **Allocation and Shared Pool:** Each customer gets an allocated tokens-per-minute (TPM) budget. The remainder of the total global TPM capacity is defined as the *shared pool*.
* **Redis Counter Tracking:** Keep current usage counters with a 60s TTL in Redis:
  - `token_budget:used:{customer_id}`: tracks individual usage.
  - `token_budget:used:shared`: tracks shared pool utilization.
* **Atomic Budget Validation:** Build `src/services/token_budget.py` exposing:
  - `check_and_reserve_budget(...)`: Checks customer-specific allocations, falls back to the shared pool if exceeded, and increments atomically using a Lua script to avoid race conditions under concurrency.
  - `release_budget(...)`: Refunds overestimated tokens on call completion.
* **Database Caching:** Cache database budget definitions in Redis with a TTL (e.g. 5 minutes) to avoid round-tripping to Postgres for every check.

* **Dependency Injection:** Database and Redis connections are injected, making it easy to test budget checks offline using mock dependencies.

---

## 📂 Issue 6: Coupled Pipeline & Blocking Execution (Integration)

### 1. What is the Issue?
* **Problem:** In the original `celery_tasks.py`, fetching the call recording from Exotel (Step 1) and analyzing the transcript using the LLM (Step 2) ran sequentially. Step 2 could not begin until Step 1 finished.
* **Impact:** Since Exotel takes 10s–90s to make recordings available, the worker thread sat idle doing nothing, blocking time-sensitive LLM analysis from starting.

### 2. How will we Solve it?
* **Decoupled Tasks:** Split the post-call pipeline into two separate, independent Celery tasks:
  1. `process_recording_upload_task`: Handles polling and uploading the audio recording to S3.
  2. `process_llm_analysis_task`: Handles rate-limiting checks, LLM execution, and downstream workflows (WhatsApp, CRM, lead stage updates).
* **Worker Execution Flow:**
  - Endpoint `endpoints.py` receives webhook.
  - Automatically triages priority ("hot" vs "cold").
  - Inserts task records in `processing_tasks` Postgres table.
  - Dispatches both tasks to Celery independently.
* **Resilient Rate-Limiting & Budget Enforcement:**
  - Before starting `process_llm_analysis_task`, check the Redis Rate Limiter and Token Budget Manager.
  - If rate limits or budgets are exceeded, defer the task by scheduling it with a Celery retry countdown (e.g. 5–10 seconds) without holding worker threads.

### 3. SOLID & Design Pattern Analysis
* **Asynchronous Decoupling (Broker Pattern):** Utilizing Celery as an asynchronous message broker to run non-dependent workflows in parallel, maximizing worker utility.
* **Single Responsibility Principle (SRP):** Each Celery task has exactly one execution responsibility (either media management or text classification).
* **Observer / Publish-Subscribe Pattern:** The completion of the call publishes independent events (tasks) which are processed concurrently by active workers.

---

## 📂 Issue 7: Alerting & Threshold Warnings (Task 4B)

### 1. What was the Issue?
* **Problem:** There were no alert conditions to notify operators when critical thresholds were breached (e.g., rate limit warnings, queue backlog depths, customer budget exhaustion, high recording failure rates, and dead-lettered tasks).
* **Impact:** Operations teams would remain unaware of system overloads, Redis/worker crashes causing stuck queues, or telephony provider failures causing massive recording drops, leading to silent processing degradation.

### 2. How did we Solve it?
* **Alerting Service ([alerting.py](file:///d:/Desktop/sde-assignment/src/services/alerting.py)):** Created a centralized, throttled alert manager that validates system states:
  - **Queue Backlog Alerts:** Compares pending tasks count against the `10,000` task limit. Triggers `ALERT: Processing backlog threshold exceeded` if breached.
  - **Recording Failure Alerts:** Calculates the recording failure rate over the last 15 minutes. Triggers `ALERT: Recording failure rate high` if failures exceed 10% of total tasks (with a minimum sample of 10).
  - **Redis Throttling:** Throttles queue backlog queries to once every 10 seconds and recording failure checks to once every 60 seconds using Redis keys (`alert:backlog:last_checked`, `alert:rec_fail:last_checked`) to prevent database overhead.
* **Inline Threshold Triggers:**
  - **Rate Limiter Warning:** Triggers `ALERT: Rate limit warning` in [rate_limiter.py](file:///d:/Desktop/sde-assignment/src/services/rate_limiter.py) when the token bucket capacity drops below 20% (utilization > 80%).
  - **Customer Budget Exhaustion:** Triggers `ALERT: Customer budget exceeded` in [token_budget.py](file:///d:/Desktop/sde-assignment/src/services/token_budget.py) when a customer has exhausted both their guaranteed allocation and the global shared overflow pool.
  - **Dead Letter Task Logging:** Triggers `ALERT: Dead letter task created` inside [task_manager.py](file:///d:/Desktop/sde-assignment/src/services/task_manager.py) when a processing task exceeds its maximum retry threshold and is quarantined.

### 3. SOLID & Design Pattern Analysis
* **Single Responsibility Principle (SRP):** Outsource all logic for calculating complex database-driven ratios (backlog size, 15-min failure rate) to a dedicated `alerting.py` module, decoupling it from task lifecycle and database persistence methods.
* **Separation of Concerns:** The logger format is unified with an explicit `ALERT:` prefix, facilitating easy log parsing and scraping by external APM platforms (e.g. Grafana Loki, Datadog, or PagerDuty).

---

## 📂 Issue 8: Blunt Circuit Breaker & Proportional Backpressure (Task 5A)

### 1. What was the Issue?
* **Problem:** The system used a blunt binary circuit breaker (`PostCallCircuitBreaker`). If RPM usage reached 90%, it froze outbound dialling for the agent for a hardcoded 1800 seconds (30 minutes) cross-customer.
* **Impact:** There was no middle gear; a slight spike in usage immediately shut down outbound dialling entirely, severely impacting business revenue and campaign throughput. Furthermore, it froze dialling globally, penalizing non-offending campaigns.

### 2. How did we Solve it?
* **Proportional Backpressure:** Refactored `check_capacity` in [circuit_breaker.py](file:///d:/Desktop/sde-assignment/src/services/circuit_breaker.py) to return a throttle multiplier float (`0.05` to `1.0`) instead of a boolean value:
  - Usage < 50% -> `1.0` (Full speed)
  - Usage 50-70% -> `0.75` (75% speed)
  - Usage 70-85% -> `0.50` (50% speed)
  - Usage 85-95% -> `0.25` (25% speed)
  - Usage >= 95% -> `0.05` (5% speed, ensuring dialling never completely halts)
* **Design Decoupling:** Replaced the binary frozen states (`CircuitState`, `is_open`, `_trip`) with this direct, memoryless, and real-time utilization check, reducing complexity and avoiding stale frozen states.

### 3. SOLID & Design Pattern Analysis
* **Interface Design Principle (LSP/ISP):** Standardized capacity verification into a unified signal that clients can use to scale traffic rate proportionally.
* **Fail-Safe Robustness:** Wrapped Redis queries in robust try-except blocks to return a safe fallback value of `1.0` (full capacity) on cache connection errors, ensuring operations don't freeze due to external cache glitches.

---

## 📂 Issue 9: Per-Customer Dynamic Configuration (Task 5C)

### 1. What was the Issue?
* **Problem:** Configuration settings (like "hot" triage keywords or webhook destinations) were static, hardcoded, and applied globally to all campaigns.
* **Impact:** Different business domains prioritize different conversations. Standard keywords might classify calls incorrectly for certain companies, and changing keyword profiles or hook URLs required code deployments, which slowed campaign launches.

### 2. How did we Solve it?
* **Config Table & Model:** Introduced the `customer_configs` table in [migration_001.sql](file:///d:/Desktop/sde-assignment/data/migration_001.sql) and mapped it to `CustomerConfig` in [customer_config.py](file:///d:/Desktop/sde-assignment/src/models/customer_config.py) to store per-customer `hot_keywords` arrays and `crm_webhook_url` configurations.
* **Throttled Cache Layer:** Built `CustomerConfigManager` in [customer_config_manager.py](file:///d:/Desktop/sde-assignment/src/services/customer_config_manager.py) retrieving parameters from a Redis cache (TTL 300s) on cache hits, falling back to database fetches on cache misses, keeping SQL lookups to a minimum.
* **Triage Customization:** Refactored `classify_priority` in [triage.py](file:///d:/Desktop/sde-assignment/src/services/triage.py) to accept customized keywords, which are dynamically passed based on the enqueued call's customer profile.

### 3. SOLID & Design Pattern Analysis
* **Proxy Pattern / Cache Aside:** The `CustomerConfigManager` acts as a caching proxy separating the consumer (FastAPI endpoint / triage service) from direct database lookups.
* **Open/Closed Principle (OCP):** Triage rules are open to extension (customers customize keywords in database tables) but closed to modification (endpoints/triage code logic remains unchanged).

---

## 📂 Issue 10: Downstream CRM Push Webhook Failure & Retries (Task 5B)

### 1. What was the Issue?
* **Problem:** Webhook dispatches to customer CRMs were fire-and-forget, sequentially coupled to endpoint loops, or executed blindly without retries.
* **Impact:** If a customer's CRM API was offline, processing results were permanently lost.

### 2. How did we Solve it?
* **Durable Webhook Tasks:** Integrated CRM pushes into the Postgres durable tasks layer. When LLM completes, it creates a `crm_push` task of type `ProcessingTask` in Postgres, and schedules a Celery worker to push it.
* **Failure Propagation:** The Celery task executes the webhook using `httpx`. If the webhook fails, it raises an exception which automatically calls `task_manager.fail_task()`, incrementing the attempt count, logging the failure, and scheduling an exponential backoff retry.
* **DLQ Safety:** If the webhook continues failing after 5 attempts, it is automatically promoted to `DEAD_LETTER` status in Postgres for manual operational visibility and replay.

### 3. SOLID & Design Pattern Analysis
* **Command Pattern:** The webhook payload is encapsulated as a standalone execution command inside the `processing_tasks` table, allowing the system to defer, schedule, or replay it independently of LLM analysis.
* **Reusability / DRY Principle:** Integrated CRM retries directly into the existing database task manager, reusing the exact same exponential retry and DLQ logic that drives LLM and recording tasks.

---

## 📂 Issue 11: Encryption at Rest for Sensitive Data (Task 5D)

### 1. What was the Issue?
* **Problem:** Transcripts (`conversation_data` in the `interactions` table), task contexts (`payload` in the `processing_tasks` table), and outcomes (`result` in `processing_tasks`) contain sensitive Customer PII and conversation records. They were stored as plaintext JSON/JSONB in the database.
* **Impact:** Any database dump leaks, unauthorized read accesses, or SQL injection attacks could expose private transcripts and API keys, violating standard security compliance (e.g. SOC2, GDPR) and customer trust.

### 2. How did we Solve it?
* **EncryptedJSONB TypeDecorator:** Created a custom SQLAlchemy `TypeDecorator` that wraps the database JSON/JSONB column type.
* **AES-128 CBC Encryption:** Uses cryptography's `Fernet` symmetric encryption scheme.
* **Robust Fallback:** When storing data, it automatically encrypts it to a string. When reading, if the data is a ciphertext, it decrypts and parses it. If it is already a plain dictionary or unencrypted JSON string, it returns it directly. This guarantees 100% backward compatibility with preexisting database rows.

### 3. SOLID & Design Pattern Analysis
* **Proxy Pattern:** The custom type decorator acts as a transparent data wrapper proxy. Consumers of the SQLAlchemy model query and assign raw dictionaries exactly as before without needing any custom encryption logic, fulfilling the Single Responsibility Principle.
* **Open/Closed Principle (OCP):** The models are extended to support encryption-at-rest without changing the model consumer queries or application API interfaces.

---

## 📂 Issue 12: Stuck-Task Periodic Sweeper (Beat Integration)

### 1. What was the Issue?
* **Problem:** While `task_manager.reset_stuck_tasks()` was defined to handle recovery when a worker crashes mid-task, it was never actually scheduled to run periodically in production.
* **Impact:** Tasks stranded in `in_progress` status due to Celery worker OOMs, host crashes, or database connection pool timeouts would remain stuck forever, violating the "No task is permanently lost" guarantee.

### 2. How did we Solve it?
* **Celery Beat Scheduler:** Configured Celery Beat scheduler in `src/tasks/celery_app.py` to trigger the sweeper periodically.
* **Stuck Task Reclamation:** Implemented `reset_stuck_tasks_periodic` running every 60 seconds. It calls the sweeper with a 10-minute timeout threshold, reclaims the stuck tasks back to `PENDING` state, and increments the attempt count so they resume execution on live workers.
* **Structured Alerting:** Emits a log info event listing the number of reclaimed tasks when any stuck tasks are recovered.

### 3. SOLID & Design Pattern Analysis
* **Observer Pattern (Celery Beat):** The scheduler acts as an event publisher triggering task state checks at predefined intervals.
* **Dependency Inversion Principle (DIP):** The periodic Celery task depends on the abstract task manager's state recovery contract rather than writing custom database update queries directly.

