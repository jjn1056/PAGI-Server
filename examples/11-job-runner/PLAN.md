# Async Job Runner - Implementation Plan

## Overview

A demonstration application showcasing PAGI's async capabilities through a task queue system where users submit jobs, watch real-time progress via SSE, and manage the queue via WebSocket.

## Architecture

```
examples/11-job-runner/
├── app.pl                    # Main PAGI application (routing, middleware, lifespan)
├── PLAN.md                   # This file
├── STATUS.md                 # Progress tracking and scratch space
├── README.md                 # User documentation
├── lib/JobRunner/
│   ├── Queue.pm              # Job queue state management
│   ├── Worker.pm             # Async job execution engine
│   ├── Jobs.pm               # Job type definitions (prime, countdown, etc.)
│   ├── HTTP.pm               # REST API endpoints + static file serving
│   ├── SSE.pm                # Per-job progress streaming
│   └── WebSocket.pm          # Real-time queue updates and admin commands
└── public/
    ├── index.html            # Dashboard UI
    ├── css/style.css         # Styling
    └── js/app.js             # Frontend JavaScript
```

---

## Phase 1: Core Infrastructure

### Step 1.1: Queue.pm - Job State Management

**Purpose:** Centralized in-memory storage for all job state.

**Data Structures:**
```perl
my %jobs;           # job_id => { id, type, params, status, progress, result, error,
                    #             created_at, started_at, completed_at, worker_id }
my @pending_queue;  # Ordered list of job IDs waiting to run
my %running_jobs;   # job_id => 1 (currently executing)
my $job_counter;    # Auto-incrementing job ID
my $event_loop;     # IO::Async loop reference
```

**Job Statuses:**
- `pending` - In queue, waiting to be picked up
- `running` - Currently executing
- `completed` - Finished successfully
- `failed` - Finished with error
- `cancelled` - Aborted by user

**Exported Functions:**
```perl
# Job lifecycle
create_job($type, $params) -> $job_id
get_job($job_id) -> $job_hashref
get_all_jobs() -> \@jobs
update_job($job_id, $updates) -> $job
cancel_job($job_id) -> $success

# Queue management
get_pending_jobs() -> \@job_ids
get_running_jobs() -> \@job_ids
pop_next_job() -> $job_id (moves from pending to running)
complete_job($job_id, $result)
fail_job($job_id, $error)

# Progress tracking
update_progress($job_id, $percent, $message)
get_progress($job_id) -> { percent, message }

# Subscribers (for broadcasting updates)
add_queue_subscriber($id, $send_cb)
remove_queue_subscriber($id)
broadcast_queue_event($event_type, $data)

# Stats
get_queue_stats() -> { pending, running, completed, failed, total }

# Event loop
set_event_loop($loop)
```

**Acceptance Criteria:**
- [ ] Can create jobs with unique IDs
- [ ] Can transition jobs through status lifecycle
- [ ] Can update and retrieve progress
- [ ] Subscribers receive broadcast events

---

### Step 1.2: Jobs.pm - Job Type Definitions

**Purpose:** Define the actual work each job type performs.

**Job Type Interface:**
```perl
# Each job type is a hashref:
{
    name        => 'countdown',
    description => 'Count down from N seconds',
    params      => [ { name => 'seconds', type => 'integer', default => 10, min => 1, max => 60 } ],
    execute     => async sub ($job, $progress_cb, $cancel_check) { ... },
}
```

**Job Types to Implement:**

#### 1. Countdown Timer
- **Params:** `seconds` (1-60, default 10)
- **Behavior:** Count down, reporting progress each second
- **Progress:** `{ percent: 30, message: "7 seconds remaining..." }`
- **Result:** `{ message: "Countdown complete!" }`

#### 2. Prime Calculator
- **Params:** `limit` (100-100000, default 1000)
- **Behavior:** Find all primes up to limit using Sieve of Eratosthenes
- **Progress:** Reports every 10% of range processed
- **Result:** `{ count: 168, primes: [2, 3, 5, ...], duration: 0.05 }`

#### 3. Fibonacci Generator
- **Params:** `count` (1-1000, default 50)
- **Behavior:** Generate first N Fibonacci numbers
- **Progress:** Reports every 10 numbers generated
- **Result:** `{ numbers: [1, 1, 2, 3, 5, ...], duration: 0.01 }`

#### 4. URL Fetcher
- **Params:** `urls` (array of URLs, max 10)
- **Behavior:** Fetch URLs concurrently, report as each completes
- **Progress:** `{ percent: 50, message: "Fetched 5/10 URLs" }`
- **Result:** `{ results: [ { url, status, size, duration }, ... ] }`

#### 5. Echo Delay
- **Params:** `message` (string), `delay` (1-30 seconds)
- **Behavior:** Wait for delay, then return message
- **Progress:** Reports waiting time remaining
- **Result:** `{ message: "...", echoed_at: timestamp }`

**Exported Functions:**
```perl
get_job_types() -> \@job_type_definitions
get_job_type($name) -> $job_type_def
validate_job_params($type, $params) -> ($valid, $error)
execute_job($job, $progress_cb, $cancel_check) -> Future
```

**Acceptance Criteria:**
- [ ] All 5 job types defined with proper metadata
- [ ] Parameter validation works correctly
- [ ] Each job type can execute and report progress
- [ ] Jobs can be cancelled mid-execution

---

### Step 1.3: Worker.pm - Async Job Execution Engine

**Purpose:** Picks up jobs from queue and executes them asynchronously.

**Design:**
- Configurable concurrency limit (default: 3 simultaneous jobs)
- Uses IO::Async timers to poll for new work
- Each job runs in its own async context
- Handles cancellation gracefully

**Key Functions:**
```perl
start_worker($loop, $concurrency) # Start the worker loop
stop_worker()                      # Stop accepting new jobs
get_worker_stats() -> { active, capacity, processed }
```

**Worker Loop Logic:**
```
every 100ms:
    if (running_count < concurrency):
        job_id = pop_next_job()
        if job_id:
            spawn async job execution

async execute_job:
    update job status to 'running'
    broadcast 'job_started' event

    try:
        result = await job_type.execute(job, progress_cb, cancel_check)
        complete_job(job_id, result)
        broadcast 'job_completed' event
    catch:
        fail_job(job_id, error)
        broadcast 'job_failed' event
```

**Progress Callback:**
```perl
my $progress_cb = sub ($percent, $message) {
    update_progress($job_id, $percent, $message);
    broadcast_job_progress($job_id, $percent, $message);
};
```

**Cancellation Check:**
```perl
my $cancel_check = sub {
    my $job = get_job($job_id);
    return $job->{status} eq 'cancelled';
};
```

**Acceptance Criteria:**
- [ ] Worker starts and polls for jobs
- [ ] Jobs execute with progress updates
- [ ] Concurrent execution respects limit
- [ ] Cancellation stops job execution
- [ ] Failed jobs are marked appropriately

---

## Phase 2: Protocol Handlers

### Step 2.1: HTTP.pm - REST API + Static Files

**Endpoints:**

#### Static Files
- `GET /` → `public/index.html`
- `GET /css/*` → `public/css/*`
- `GET /js/*` → `public/js/*`

#### Job API
| Method | Path | Description | Request | Response |
|--------|------|-------------|---------|----------|
| GET | `/api/jobs` | List all jobs | - | `[{id, type, status, progress, created_at}, ...]` |
| POST | `/api/jobs` | Create job | `{type, params}` | `{id, type, status, created_at}` |
| GET | `/api/jobs/:id` | Get job details | - | `{id, type, params, status, progress, result, ...}` |
| DELETE | `/api/jobs/:id` | Cancel job | - | `{success: true}` |

#### Job Types API
| Method | Path | Description | Response |
|--------|------|-------------|----------|
| GET | `/api/job-types` | List available types | `[{name, description, params}, ...]` |

#### Queue Stats API
| Method | Path | Description | Response |
|--------|------|-------------|----------|
| GET | `/api/stats` | Queue statistics | `{pending, running, completed, failed, worker: {active, capacity}}` |

**Error Responses:**
```json
{ "error": "Job not found", "code": 404 }
```

**Acceptance Criteria:**
- [ ] Static files served correctly with proper MIME types
- [ ] All API endpoints return valid JSON
- [ ] Job creation validates type and params
- [ ] Job cancellation works for pending and running jobs
- [ ] 404 for non-existent jobs

---

### Step 2.2: SSE.pm - Per-Job Progress Streaming

**Endpoint:** `GET /api/jobs/:id/progress`

**Protocol:**
```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache

event: status
data: {"status": "running", "started_at": 1234567890}

event: progress
data: {"percent": 25, "message": "Processing..."}

event: progress
data: {"percent": 50, "message": "Halfway there..."}

event: complete
data: {"status": "completed", "result": {...}, "duration": 3.2}
```

**Event Types:**
- `status` - Job status changed
- `progress` - Progress update
- `complete` - Job finished successfully
- `failed` - Job finished with error
- `cancelled` - Job was cancelled

**Behavior:**
1. On connect, send current job state immediately
2. Subscribe to progress updates for this job
3. Push events as they occur
4. Close stream when job completes/fails/cancels
5. Handle client disconnect gracefully

**Acceptance Criteria:**
- [ ] SSE stream starts with current job state
- [ ] Progress updates pushed in real-time
- [ ] Stream closes on job completion
- [ ] Multiple clients can watch same job
- [ ] Client disconnect doesn't affect other clients

---

### Step 2.3: WebSocket.pm - Real-Time Queue Management

**Endpoint:** `GET /ws/queue`

**Server → Client Messages:**
```json
{ "type": "queue_state", "jobs": [...], "stats": {...} }
{ "type": "job_created", "job": {...} }
{ "type": "job_started", "job_id": "...", "started_at": ... }
{ "type": "job_progress", "job_id": "...", "percent": 50, "message": "..." }
{ "type": "job_completed", "job_id": "...", "result": {...} }
{ "type": "job_failed", "job_id": "...", "error": "..." }
{ "type": "job_cancelled", "job_id": "..." }
{ "type": "stats_update", "stats": {...} }
```

**Client → Server Messages:**
```json
{ "type": "create_job", "job_type": "countdown", "params": { "seconds": 10 } }
{ "type": "cancel_job", "job_id": "..." }
{ "type": "clear_completed" }
{ "type": "get_state" }
{ "type": "ping" }
```

**Behavior:**
1. On connect, send full queue state
2. Subscribe to all queue events
3. Handle client commands
4. Broadcast updates to all connected clients
5. Ping/pong for keepalive

**Acceptance Criteria:**
- [ ] Initial state sent on connect
- [ ] All queue events broadcast to clients
- [ ] Job creation via WebSocket works
- [ ] Job cancellation via WebSocket works
- [ ] Multiple clients stay in sync

---

## Phase 3: Main Application

### Step 3.1: app.pl - Application Assembly

**Components:**
1. Lifespan handler (start worker on startup, stop on shutdown)
2. Router middleware for path dispatching
3. Logging middleware
4. CORS headers for API endpoints

**Route Table:**
```perl
# WebSocket
'/ws/queue' => websocket_handler()

# SSE
'/api/jobs/*/progress' => sse_handler()  # Note: need to extract job ID

# HTTP API
'POST /api/jobs'       => http_handler()
'GET /api/jobs'        => http_handler()
'GET /api/jobs/*'      => http_handler()
'DELETE /api/jobs/*'   => http_handler()
'GET /api/job-types'   => http_handler()
'GET /api/stats'       => http_handler()

# Static files (fallback)
'*' => http_handler()
```

**Lifespan Events:**
```perl
on startup:
    initialize Queue
    start Worker with concurrency=3
    log "Job Runner started"

on shutdown:
    stop Worker
    log "Job Runner stopped"
```

**Acceptance Criteria:**
- [ ] Server starts and initializes worker
- [ ] All routes dispatch correctly
- [ ] Lifespan events fire properly
- [ ] Graceful shutdown stops worker

---

## Phase 4: Frontend

### Step 4.1: index.html - Dashboard Structure

**Layout:**
```
+--------------------------------------------------+
|  Job Runner Dashboard                    [Stats] |
+--------------------------------------------------+
|  +----------------+  +-------------------------+ |
|  | Job Types      |  | Queue                   | |
|  | [Countdown   ] |  | ID  Type   Status  Prog | |
|  | [Prime Calc  ] |  | 1   count  running  45% | |
|  | [Fibonacci   ] |  | 2   prime  pending   0% | |
|  | [URL Fetcher ] |  | 3   fib    complete 100%| |
|  | [Echo Delay  ] |  +-------------------------+ |
|  +----------------+                              |
|  +----------------+  +-------------------------+ |
|  | New Job Form   |  | Job Details             | |
|  | Type: [      ] |  | ID: 1                   | |
|  | Params:        |  | Type: countdown         | |
|  | [seconds: 10 ] |  | Status: running         | |
|  | [Submit]       |  | Progress: 45%           | |
|  +----------------+  | [==========    ] 45%    | |
|                      | Message: 5s remaining   | |
|                      | [Cancel]                | |
|                      +-------------------------+ |
+--------------------------------------------------+
```

**Sections:**
1. **Header** - Title, connection status, live stats
2. **Job Types** - Clickable list to populate form
3. **New Job Form** - Dynamic form based on selected type
4. **Queue** - Live list of all jobs with status
5. **Job Details** - Detailed view of selected job with progress bar

---

### Step 4.2: style.css - Styling

**Design Principles:**
- Clean, modern look (similar to chat app)
- Dark/light theme support
- Responsive layout
- Animated progress bars
- Status colors (pending=gray, running=blue, completed=green, failed=red)

**Key Elements:**
- `.job-card` - Individual job in queue list
- `.progress-bar` - Animated progress indicator
- `.status-badge` - Colored status indicator
- `.job-type-btn` - Job type selector buttons
- `.form-group` - Form input groups
- `.detail-panel` - Job details panel

---

### Step 4.3: app.js - Frontend Logic

**State:**
```javascript
const state = {
    ws: null,                    // WebSocket connection
    jobs: {},                    // job_id => job object
    jobTypes: [],                // Available job types
    selectedJobType: null,       // Currently selected type for form
    selectedJobId: null,         // Currently viewing job details
    sseConnections: {},          // job_id => EventSource for progress
    stats: { pending: 0, running: 0, completed: 0, failed: 0 }
};
```

**WebSocket Handling:**
- Connect to `/ws/queue`
- Handle all event types
- Update UI on each event
- Reconnect on disconnect

**SSE Handling:**
- When viewing a running job, connect to `/api/jobs/:id/progress`
- Update progress bar in real-time
- Close SSE when job completes or user selects different job

**Form Handling:**
- Populate form fields based on selected job type
- Validate inputs client-side
- Submit via WebSocket or HTTP POST

**Queue Display:**
- Sort: running first, then pending, then completed/failed
- Click job to view details
- Cancel button for pending/running jobs
- Clear completed button

---

## Phase 5: Testing & Polish

### Step 5.1: Integration Testing

**Test Scenarios:**
1. Submit countdown job, watch progress, see completion
2. Submit multiple jobs, verify concurrent execution
3. Cancel running job, verify it stops
4. Cancel pending job, verify it's removed from queue
5. Submit invalid job, verify error handling
6. Disconnect/reconnect WebSocket, verify state recovery
7. Open multiple browser tabs, verify sync
8. Submit URL fetcher with real URLs, verify concurrent fetching

### Step 5.2: Documentation

**README.md Contents:**
- Feature overview
- Running instructions
- API documentation
- Architecture explanation
- Screenshots

---

## Implementation Order

1. **Step 1.1** - Queue.pm (state management)
2. **Step 1.2** - Jobs.pm (job type definitions, start with countdown only)
3. **Step 1.3** - Worker.pm (job execution)
4. **Step 2.1** - HTTP.pm (API + static files)
5. **Test** - Verify jobs can be created and executed via curl
6. **Step 2.2** - SSE.pm (progress streaming)
7. **Test** - Verify progress streams via curl
8. **Step 2.3** - WebSocket.pm (real-time updates)
9. **Step 3.1** - app.pl (wire everything together)
10. **Test** - Full backend working via curl/wscat
11. **Step 4.1** - index.html (structure)
12. **Step 4.2** - style.css (styling)
13. **Step 4.3** - app.js (interactivity)
14. **Test** - Full end-to-end in browser
15. **Step 1.2b** - Add remaining job types (prime, fibonacci, URL, echo)
16. **Step 5.1** - Integration testing
17. **Step 5.2** - Documentation

---

## Success Metrics

- [ ] Can submit a job and see it complete
- [ ] Progress bar updates in real-time
- [ ] Multiple jobs run concurrently (up to limit)
- [ ] Cancellation works for pending and running jobs
- [ ] Multiple browser tabs stay synchronized
- [ ] Clean, responsive UI
- [ ] All 5 job types working
