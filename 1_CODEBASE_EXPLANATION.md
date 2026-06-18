# 📘 File 1: Understanding the Codebase

> **Purpose:** This file explains WHAT this repo is about, WHAT every file does, and HOW the system currently works.
> **No code is implemented here — this is purely an explanation document.**

---

## What Is This System?

This is a **Voice AI Platform** for B2B outbound calling campaigns.

### The Business Scenario

Imagine you run a company called "Agent Universe" that provides AI-powered phone calling as a service:

1. **A business** (like Cashify, an insurance company, a loan provider, etc.) signs up as a **Customer** on your platform
2. They upload a list of **Leads** — people they want to call (phone numbers + names)
3. They create a **Campaign** — "Call all 50,000 leads about our new offer"
4. Your platform's **AI voice bot** automatically calls each lead
5. **After each call ends**, the system needs to do a bunch of processing — THIS is what the codebase handles

### What Happens After a Call Ends? (The "Post-Call Pipeline")

When any call finishes, the system must:

```
1. FETCH & STORE the call recording (download from telephony provider → upload to S3)
2. ANALYZE the transcript using an LLM (GPT-4) to understand:
   - What happened? (call_stage: "rebook_confirmed", "not_interested", etc.)
   - What information was mentioned? (entities: dates, times, names)
   - A summary for the dashboard
3. UPDATE the dashboard with results
4. PUSH results to the customer's CRM (if configured)
5. TRIGGER downstream actions:
   - Send a WhatsApp message ("Your appointment is confirmed for 3PM tomorrow")
   - Book a callback slot
   - Flag for human review if the lead was angry
```

### Scale

- **~100,000 calls per campaign run**
- **Multiple customers** running campaigns simultaneously
- The LLM provider (OpenAI) has **hard rate limits**: max 500 requests/min, 90,000 tokens/min
- The current system **breaks at this scale**. Your job is to fix it.

---

## Key Terminology

| Term | What It Means | Example |
|------|---------------|---------|
| **Customer** | A business using this platform (NOT the person being called) | Cashify, an insurance company |
| **Lead** | A person being called | Mr. Sharma, phone +919876543210 |
| **Campaign** | A batch of calls a customer wants to make | "Call all leads about the new offer" |
| **Session** | A calling session for one specific lead | Session for Mr. Sharma |
| **Interaction** | One specific call attempt within a session | The 2nd call to Mr. Sharma |
| **Agent** | The AI voice bot that makes the call | Bot configured with a script |
| **Dialler** | The system component that dispatches outbound calls | Decides when to make the next call |
| **Exotel** | The telephony provider (like Twilio but for India) | Handles actual phone connections |
| **call_stage** | The outcome/disposition of a call | "rebook_confirmed", "not_interested" |
| **Celery** | A Python task queue for background processing | Workers pick up tasks and execute them |
| **Redis** | An in-memory data store used as Celery's message broker | Holds tasks waiting to be processed |
| **Circuit Breaker** | A safety mechanism to prevent system overload | Like a fuse box in your house |

---

## File-by-File Explanation

### 📁 Directory Overview

```
sde-assignment/
├── src/                          ← ALL source code lives here
│   ├── app.py                    ← FastAPI app entry point (very small)
│   ├── config.py                 ← ALL settings (LLM limits, timeouts, etc.)
│   ├── api/
│   │   └── endpoints.py          ← The webhook — entry point when a call ends
│   ├── models/
│   │   ├── base.py               ← SQLAlchemy base class (1 line)
│   │   ├── interaction.py        ← Database model for a call attempt
│   │   ├── session.py            ← Database model for a calling session
│   │   └── lead.py               ← Database model for a person being called
│   ├── services/
│   │   ├── post_call_processor.py ← The LLM analysis engine
│   │   ├── recording.py          ← Recording fetch + S3 upload
│   │   ├── circuit_breaker.py    ← Tries to prevent LLM overload
│   │   ├── retry_queue.py        ← Redis-based retry for failed tasks
│   │   ├── signal_jobs.py        ← Downstream actions (WhatsApp, CRM push)
│   │   └── metrics.py            ← Timing/outcome metrics (just logs)
│   ├── tasks/
│   │   ├── celery_app.py         ← Celery setup and configuration
│   │   └── celery_tasks.py       ← Main background processing pipeline
│   └── utils/
│       ├── db.py                 ← Database connection (PostgreSQL)
│       └── redis_client.py       ← Redis connection
├── tests/
│   ├── conftest.py               ← Test fixtures and helpers
│   ├── test_post_call.py         ← Tests (they document BUGS, not correct behavior)
│   └── fixtures/
│       └── sample_transcripts.json ← Sample call transcripts (your test data)
├── data/
│   └── schema.sql                ← Database tables + seed data
├── docker-compose.yml            ← Postgres + Redis containers
├── requirements.txt              ← Python dependencies
└── SUBMISSION_TEMPLATE.md        ← Template for your design document
```

---

### Detailed File Explanations

---

### 1. `src/config.py` — The Settings Hub

**What it does:** Stores ALL configuration values in a single `Settings` class.

**Important settings and what they mean:**

| Setting | Default Value | What It Controls |
|---------|---------------|------------------|
| `DATABASE_URL` | `postgresql+asyncpg://...localhost:5432/voicebot` | Connection to PostgreSQL database |
| `REDIS_URL` | `redis://localhost:6379/0` | Connection to Redis (for caching, metrics) |
| `CELERY_BROKER_URL` | `redis://localhost:6379/1` | Redis as Celery's task queue broker |
| `LLM_PROVIDER` | `"openai"` | Which LLM provider to use |
| `LLM_MODEL` | `"gpt-4o"` | Which model |
| `LLM_API_KEY` | `"sk-mock-key-for-assessment"` | API key (mock for this assignment) |
| `LLM_REQUESTS_PER_MINUTE` | `500` | **HARD LIMIT**: Max 500 LLM API calls per minute |
| `LLM_TOKENS_PER_MINUTE` | `90,000` | **HARD LIMIT**: Max 90K tokens per minute |
| `LLM_AVG_TOKENS_PER_CALL` | `1,500` | Average tokens used per call analysis |
| `RECORDING_WAIT_SECONDS` | `45` | Seconds to sleep before fetching recording |
| `CIRCUIT_BREAKER_CAPACITY_THRESHOLD` | `0.90` | Trip circuit breaker at 90% capacity |
| `CIRCUIT_BREAKER_FREEZE_SECONDS` | `1800` | Freeze dialler for 30 minutes when tripped |
| `POSTCALL_MAX_RETRIES` | `3` | Max retry attempts for failed tasks |
| `POSTCALL_RETRY_DELAY` | `60` | Fixed 60s between retries |

**KEY OBSERVATION from the code comments:**
> `LLM_TOKENS_PER_MINUTE` and `LLM_REQUESTS_PER_MINUTE` are **defined here but nothing reads them before firing a request**. They exist as documentation, not enforcement.

**Quick math:**
- 90,000 tokens/min ÷ 1,500 tokens/call = **60 calls can be analyzed per minute**
- 100,000 calls per campaign ÷ 60 per minute = **~28 hours** to process everything
- If your campaign window is 8 hours, you're massively behind

---

### 2. `src/app.py` — App Entry Point

**What it does:** Creates the FastAPI application and mounts the router.

This is tiny — just 11 lines:
- Creates a `FastAPI` instance titled "VoiceBot Post-Call Processing"
- Includes the `router` from `endpoints.py` with a `/api/v1` prefix

So the full endpoint URL becomes: `POST /api/v1/session/{session_id}/interaction/{interaction_id}/end`

---

### 3. `src/api/endpoints.py` — The Webhook (Entry Point for Every Call)

**What it does:** This is the first code that runs when a call ends.

When Exotel (the telephony provider) disconnects a call, it sends a **webhook** (HTTP POST) to this endpoint.

**The request from Exotel includes:**
- `call_sid` — Exotel's unique ID for the call
- `duration_seconds` — How long the call lasted
- `call_status` — Status from Exotel
- `additional_data` — Extra metadata (campaign ID, lead phone, etc.)

**Step-by-step flow:**

```
1. Load the interaction from the database (_load_interaction)
   → Currently returns MOCK data (hardcoded dictionary, not a real DB query)

2. Mark the interaction status as "ENDED" (_update_interaction_status)
   → Currently just logs it (mock)

3. Check transcript length:
   IF transcript has < 4 turns → "short call"
   ELSE → "long call"

4a. SHORT CALL path:
    - Fire signal_jobs with call_stage="short_call" using asyncio.create_task()
    - Fire update_lead_stage with call_stage="short_call"
    - These are FIRE-AND-FORGET — no record, no retry

4b. LONG CALL path:
    - Package all data into a payload dictionary
    - Send it to Celery: process_interaction_end_background_task.apply_async()
    - ALSO fire signal_jobs with analysis_result={} (EMPTY — Celery hasn't run yet!)
    - ALSO fire update_lead_stage with call_stage="processing" (placeholder)

5. Return 200 OK immediately (Exotel has a 5-second timeout)
```

**Important note:** `_load_interaction()` and `_update_interaction_status()` are currently **mock functions** — they don't talk to a real database. They return hardcoded data. This is intentional for the assessment.

---

### 4. `src/tasks/celery_tasks.py` — The Main Processing Pipeline

**What it does:** This is the HEART of the system. Every long-transcript call gets processed here.

**How Celery works (beginner explanation):**
- Think of Celery as a **to-do list** for background jobs
- You "put a task on the list" (enqueue)
- Worker processes "check the list" and execute tasks one by one
- The list itself is stored in **Redis** (the broker)
- If Redis dies, the list is lost

**The task is decorated with:**
- `bind=True` — the task gets a reference to itself (for retrying)
- `max_retries=3` — retry up to 3 times on failure
- `default_retry_delay=60` — wait 60 seconds between retries (fixed, not exponential)
- `acks_late=True` — task is acknowledged AFTER completion (a worker crash causes redelivery)
- `queue="postcall_processing"` — all tasks go to a single queue

**What the task does (4 sequential steps):**

```
Step 1: RECORDING
   - Calls fetch_and_upload_recording()
   - This SLEEPS for 45 seconds, then tries ONCE to get the recording
   - If recording not found → returns None silently (no alert)
   - LLM analysis CANNOT START until this completes (even though they're independent!)

Step 2: LLM ANALYSIS
   - Creates a PostCallProcessor
   - Calls process_post_call(ctx) which:
     a. Builds an LLM prompt
     b. Calls the LLM API (mock in this codebase)
     c. Parses the response
     d. Writes result to interaction_metadata
   - NO rate limit check before calling the LLM
   - If we get a 429 → exception → Celery retry → back of the 100K queue

Step 3: SIGNAL JOBS
   - Triggers downstream actions with the REAL analysis result
   - If this fails → logged as warning, continues to step 4
   - No retry mechanism

Step 4: LEAD STAGE UPDATE
   - Updates the lead's stage (e.g., "rebook_confirmed" → "booked")
   - If this fails → logged as warning, drops it
```

**On failure:**
- The task is added to BOTH:
  - Celery's retry mechanism (`self.retry(exc=e)`)
  - The PostCallRetryQueue in Redis (`retry_queue.enqueue_retry(...)`)
- These two don't coordinate → potential DOUBLE processing

**Key math:** Each Celery worker handles one task at a time. With 3.5s LLM latency + 45s recording wait:
- Each task takes ~48.5 seconds
- 10 workers = ~2 tasks/second
- 100,000 tasks ÷ 2/second = ~14 hours to drain

---

### 5. `src/services/post_call_processor.py` — The LLM Engine

**What it does:** Sends the call transcript to an LLM and gets back a structured analysis.

**Key classes:**

**PostCallContext** — Everything needed to process one call:
- `interaction_id`, `session_id`, `lead_id`, `campaign_id`, `customer_id`, `agent_id`
- `call_sid` — Exotel's call identifier
- `transcript_text` — The conversation as text
- `conversation_data` — Full transcript as JSON
- `additional_data` — Extra metadata
- `ended_at` — When the call ended

**AnalysisResult** — What comes back from the LLM:
- `call_stage` — The disposition ("rebook_confirmed", "not_interested", etc.)
- `entities` — Extracted data (dates, amounts, names)
- `summary` — Human-readable summary
- `tokens_used` — Actual tokens consumed (important for billing)
- `latency_ms` — How long the LLM call took

**The process_post_call flow:**
1. Tell circuit breaker an LLM request is starting (`record_postcall_start`)
2. Build the LLM prompt (system prompt + transcript + additional context)
3. Call the LLM API (currently returns mock data)
4. Parse the response into an AnalysisResult
5. Write the result to `interaction_metadata` (dashboard's hot cache)
6. Log the result
7. Tell circuit breaker the request finished (`record_postcall_end`)

**What the LLM prompt asks for:**
```
Extract from the transcript:
1. call_stage — The outcome/disposition
2. entities — Key information (dates, times, amounts, names)
3. summary — A brief summary
Respond in JSON format
```

**Important observation from code comments:**
> call_stage is usually detectable from just a few sentences — sometimes a single phrase. Full entity extraction is only useful if the call had a meaningful outcome. This is a hint about differentiated processing.

---

### 6. `src/services/recording.py` — Recording Pipeline

**What it does:** Fetches the call recording from Exotel's API and uploads it to S3.

**How Exotel recordings work:**
1. After a call ends, Exotel processes the audio
2. A recording URL becomes available via their REST API
3. Time to availability: typically 10–30 seconds, but can be 60–90s under load
4. API: `GET /v1/Accounts/{account_sid}/Calls/{call_sid}/Recording`
5. Returns 200 + recording_url if ready, 404 if not yet available
6. The API is poll-friendly — they don't rate-limit the status endpoint

**Current implementation:**
```python
await asyncio.sleep(45)  # Wait 45 seconds unconditionally
recording_url = await _fetch_exotel_recording_url(call_sid, account_id)  # Try ONCE
if not recording_url:
    logger.debug(...)  # Log at DEBUG level (invisible in production)
    return None         # Give up silently
```

**What happens after getting the URL:**
- Download the audio from Exotel's URL
- Upload to S3 with key `recordings/{interaction_id}.mp3`
- Return the S3 key

**What the mock does:** Just returns the S3 key string without actually uploading anything.

---

### 7. `src/services/circuit_breaker.py` — Overload "Protection"

**What it does:** The dialler (call-making system) asks the circuit breaker "Can I make a new call?" before dispatching. The circuit breaker checks LLM usage and says yes or no.

**CircuitState** tracks per agent:
- `is_open` — Is the breaker tripped?
- `freeze_until` — When does the freeze end?
- `consecutive_failures` — Tracked but never used (intended for half-open state that was never built)

**check_capacity(agent_id) flow:**
```
IF circuit is open AND freeze hasn't expired:
    → return False (don't make calls)
IF freeze expired:
    → reset circuit (don't check if problem is resolved)

Read current RPM from Redis key "llm:postcall:rpm"
Calculate usage_ratio = current_rpm / max_rpm

IF usage_ratio >= 90%:
    → TRIP: freeze ALL calls for this agent for 1800 seconds (30 min)
    → return False
ELSE:
    → return True (allow calls)
```

**record_postcall_start():** Increments Redis counter `llm:postcall:rpm` with 60s TTL
**record_postcall_end():** Decrements the same counter

---

### 8. `src/services/retry_queue.py` — Redis Retry Queue

**What it does:** When a Celery task fails, the payload is pushed here for later retry.

**RetryEntry** stores:
- `interaction_id` — Which interaction
- `attempt` — Current attempt number
- `last_error` — What went wrong
- `next_retry_at` — When to try again (timestamp)
- `payload` — The full task payload

**enqueue_retry flow:**
1. Check how many times this interaction has been retried (state stored in Redis)
2. If >= max_retries (3): log error, drop the task forever, return False
3. Otherwise: increment counter, push to Redis list, return True

**dequeue_ready flow:**
1. Pop all entries from the Redis list
2. If an entry's `next_retry_at` has passed → add to "ready" list
3. If not ready yet → push back to end of queue (changes ordering!)
4. Return all ready entries

---

### 9. `src/services/signal_jobs.py` — Downstream Actions

**What it does:** Triggers business actions AFTER the analysis is complete.

Two functions:
- `trigger_signal_jobs()` — Dispatches to downstream services based on the analysis
  - Send WhatsApp ("Your appointment is confirmed for 3PM tomorrow")
  - Book a callback slot
  - Push to CRM
  - Flag for human review
- `update_lead_stage()` — Updates the lead's sales funnel stage in the database
  - "rebook_confirmed" → lead becomes "booked"
  - "not_interested" → lead becomes "closed_lost"
  - "callback_requested" → lead becomes "follow_up"

**Both are currently mock implementations** — they just log.

---

### 10. `src/services/metrics.py` — Metrics Tracker

**What it does:** Records timing and token usage for each interaction.

Three methods:
- `track_processing_started()` — Records start time in Redis
- `track_processing_completed()` — Logs tokens used, LLM latency, wall clock time
- `track_processing_failed()` — Logs permanent failure

**Everything only goes to stdout logs. Nothing is queryable.**

---

### 11. Database Models (`src/models/`)

Three SQLAlchemy models matching the three database tables:

**Lead** — A person being called:
- `id`, `campaign_id`, `customer_id`
- `name`, `phone`, `email`
- `stage` — Sales funnel stage ("new", "contacted", "booked", "closed_lost")
- `lead_data` — JSONB for arbitrary data

**Session** — A calling session for a lead:
- `id`, `lead_id`, `campaign_id`, `customer_id`, `agent_id`
- `status` — ACTIVE, COMPLETED, FAILED

**Interaction** — A single call attempt:
- `id`, `session_id`, `lead_id`, `campaign_id`, `customer_id`, `agent_id`
- `status` — INITIATED, RINGING, IN_PROGRESS, ENDED, FAILED, PROCESSING
- `call_sid` — Exotel's call ID
- `conversation_data` — JSONB with transcript
- `interaction_metadata` — JSONB dashboard cache (analysis results go HERE)
- `recording_url`, `recording_s3_key` — Recording locations
- `postcall_celery_task_id` — Which Celery task is processing this
- `retry_count`, `error_log` — Retry tracking

**Helper properties on Interaction:**
- `transcript_text` — Converts transcript JSON to readable text
- `is_short_transcript` — True if < 4 turns
- `exotel_account_id` — Pulls from conversation_data

---

### 12. `data/schema.sql` — Database Schema + Seed Data

Creates the three tables (leads, sessions, interactions) with proper indexes.

**Seed data includes 3 sample interactions:**
1. `exotel-call-001` — Rebook confirmed (6 turns, long transcript)
2. `exotel-call-002` — Not interested (3 turns, short transcript)
3. `exotel-call-003` — Wrong number (2 turns, short transcript)

---

### 13. Sample Transcripts (`tests/fixtures/sample_transcripts.json`)

7 different call scenarios with `expected_disposition` and `expected_lane`:

| Key | What Happened | Expected Lane |
|-----|--------------|---------------|
| `rebook_confirmed` | Customer confirmed rescheduling | **hot** (process NOW) |
| `demo_booked` | Customer booked a product demo | **hot** (process NOW) |
| `escalation_needed` | Angry customer wants a manager | **hot** (process NOW) |
| `not_interested` | Customer said no | **cold** (batch later) |
| `callback_requested` | Customer asked to be called back | **cold** (batch later) |
| `already_purchased` | Customer already bought | **cold** (batch later) |
| `hinglish_ambiguous` | Mixed Hindi-English, unclear intent | **cold** (batch later) |
| `short_call_hangup` | "Wrong number" — hung up | **skip** (no LLM) |

---

### 14. Tests (`tests/`)

The test file `test_post_call.py` documents the CURRENT broken behavior:

1. **test_every_call_gets_full_llm_analysis** — Even a "not interested" call gets full LLM analysis (waste)
2. **test_rebook_gets_same_priority_as_not_interested** — No priority differentiation
3. **test_short_transcript_detected** — Short transcripts ARE detected but only at endpoint level
4. **test_recording_blocks_processing** — 45s sleep blocks everything
5. **test_circuit_breaker_freezes_dialler** — Binary 30-minute freeze

---

## The Current End-to-End Flow Diagram

```
     Exotel (telephony provider) finishes a call
                      │
                      ▼
     POST /api/v1/session/{sid}/interaction/{iid}/end
                      │
                      ▼
            ┌─────────────────────┐
            │   FastAPI Endpoint   │
            │   (endpoints.py)     │
            │                      │
            │ 1. Load interaction  │
            │ 2. Mark as ENDED     │
            │ 3. Check transcript  │
            │    length            │
            └──────────┬───────────┘
                       │
            ┌──────────┴──────────┐
            ▼                     ▼
   SHORT (< 4 turns)     LONG (≥ 4 turns)
            │                     │
            ▼                     ▼
   asyncio.create_task:    Celery task enqueued
   • signal_jobs("short")         │
   • update_lead("short")        ▼
   (fire & forget)       ┌──────────────────┐
                          │  Celery Worker    │
       ALSO fires:        │                  │
   asyncio.create_task:   │ 1. sleep(45s) ─┐ │
   • signal_jobs({})      │    fetch rec   │ │
     (EMPTY payload!)     │    upload S3   │ │
   • update_lead          │               ◄┘ │
     ("processing")       │ 2. LLM call     │
                          │    (no rate      │
                          │     check!)      │
                          │                  │
                          │ 3. signal_jobs   │
                          │    (real result) │
                          │                  │
                          │ 4. update lead   │
                          │    stage         │
                          └────────┬─────────┘
                                   │
                             On FAILURE:
                          ┌────────┴────────┐
                          ▼                 ▼
                    Celery retry      Redis retry queue
                    (3 tries, 60s)    (3 tries, 60s)
                    BOTH fire!         Same Redis = same
                    → DOUBLE           failure mode as
                    processing         Celery broker
```

---

**Next:** See `2_PROBLEMS_AND_SOLUTIONS.md` for a detailed breakdown of each problem and its solution.
