# Async Job Runner - PAGI Demo

A real-time job queue dashboard demonstrating PAGI's async capabilities with HTTP, SSE, and WebSocket protocols working together.

> **This copy is deliberately IO::Async-bound, and that is the point.** The
> background worker tick, the WebSocket keepalive ping and the per-job delays
> use `IO::Async::Timer::Periodic` and `::Countdown`, obtaining the loop via
> `IO::Async::Loop` in application code. It therefore runs only under an
> IO::Async-based server such as PAGI::Server.
>
> **It is not the recommended approach.** A PAGI application should name no
> event loop. The loop-agnostic version of this same example lives in the spec
> repository at `PAGI/examples/11-job-runner`, where every timer is a
> `Future::IO->sleep` and the application runs unchanged under any conforming
> server. Compare the two if you want the diff.
>
> It is kept here because binding the loop is a real choice with real
> benefits, and pretending otherwise would be dishonest. Reaching directly for
> `IO::Async::Timer::*` gives you the whole timer API -- rescheduling,
> adjustable intervals, `IO::Async::Function`, the rest of the ecosystem --
> rather than only the ability to sleep. If you know your deployment is
> PAGI::Server and you accept the trade, this is what that looks like.
>
> The trade: the application no longer runs under a non-IO::Async PAGI server,
> and the coupling does not stay in one file. Here it reaches five, because a
> loop handed in at startup gets threaded wherever timing is needed. That
> spread is the real cost, and it is easier to see in a worked example than to
> describe.

## Running

From the PAGI root directory:

```bash
perl -Ilib -Iexamples/11-job-runner/lib bin/pagi-server \
    --app examples/11-job-runner/app.pl --port 5001
```

Then open http://localhost:5001 in your browser.

## Features

- **Real-time job queue** - Create countdown jobs and watch them execute
- **Live progress streaming** - SSE updates show second-by-second progress
- **WebSocket dashboard** - Queue-wide updates pushed to all connected clients
- **Concurrent execution** - Worker processes up to 3 jobs simultaneously

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Browser (app.js)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │  WebSocket   │  │     SSE      │  │   HTTP (REST)    │  │
│  │ Queue Events │  │ Job Progress │  │   Static Files   │  │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘  │
└─────────┼─────────────────┼───────────────────┼────────────┘
          │                 │                   │
          ▼                 ▼                   ▼
┌─────────────────────────────────────────────────────────────┐
│                     PAGI Server                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ WebSocket.pm │  │   SSE.pm     │  │     HTTP.pm      │  │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘  │
│         │                 │                   │            │
│         └────────────┬────┴───────────────────┘            │
│                      ▼                                     │
│              ┌───────────────┐                             │
│              │   Queue.pm    │ ◄── Job State Management    │
│              └───────┬───────┘                             │
│                      │                                     │
│              ┌───────▼───────┐                             │
│              │  Worker.pm    │ ◄── Async Job Execution     │
│              └───────┬───────┘                             │
│                      │                                     │
│              ┌───────▼───────┐                             │
│              │   Jobs.pm     │ ◄── Job Type Definitions    │
│              └───────────────┘                             │
└─────────────────────────────────────────────────────────────┘
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/stats` | Queue and worker statistics |
| GET | `/api/job-types` | Available job types |
| GET | `/api/jobs` | List all jobs |
| POST | `/api/jobs` | Create a new job |
| GET | `/api/jobs/:id` | Get job details |
| DELETE | `/api/jobs/:id` | Cancel a job |
| GET | `/api/jobs/:id/progress` | SSE progress stream |

## WebSocket Protocol

Connect to `/ws/queue` for real-time updates.

**Server -> Client:**
- `queue_state` - Full state on connect
- `job_created` - New job added
- `job_started` - Job began executing
- `job_progress` - Progress update
- `job_completed` - Job finished successfully
- `job_failed` - Job failed with error
- `job_cancelled` - Job was cancelled

**Client -> Server:**
- `create_job` - Create new job `{ job_type, params }`
- `cancel_job` - Cancel job `{ job_id }`
- `clear_completed` - Remove finished jobs
- `get_state` - Request full state

## Job Types

- **countdown** - Counts down from N seconds with progress updates
- **prime** - Finds all prime numbers up to N (with progress)
- **fibonacci** - Calculates Fibonacci sequence up to N terms
- **echo** - Echoes a message back after a configurable delay

## Testing with curl

```bash
# Create a 5-second countdown job
curl -X POST http://localhost:5001/api/jobs \
    -H "Content-Type: application/json" \
    -d '{"job_type":"countdown","params":{"seconds":5}}'

# Watch job progress (SSE)
curl -N http://localhost:5001/api/jobs/1/progress \
    -H "Accept: text/event-stream"

# Get queue stats
curl http://localhost:5001/api/stats
```
