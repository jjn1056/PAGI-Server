# 13 – Custom Logging

Sends the server's own diagnostics somewhere other than `STDERR`, and shows why
the `logger` option is a coderef rather than a logging object.

**Requires [Log::Dispatch](https://metacpan.org/pod/Log::Dispatch)**, which the
distribution does not depend on — this is the one example that needs it:

```bash
cpanm Log::Dispatch
```

## Why this example is a script

Every other example is an `app.pl` you hand to `pagi-server`. This one is not,
because `logger` takes a coderef and a command line cannot carry one. So it
constructs `PAGI::Server` itself:

```bash
perl examples/13-custom-logging/run.pl
```

Then, in another terminal:

```bash
curl localhost:5000/          # a normal response
curl localhost:5000/boom      # the app throws
curl localhost:5000/silent    # the app never starts a response
```

## What you should see

On the terminal, everything from `debug` up (your `loop` line will differ).
The access log also defaults to `STDERR`, so a line per request is
interleaved with these; they are left out here:

```
[PAGI::Server] PAGI::Server 0.003000 listening on http://127.0.0.1:5000/
[PAGI::Server]   lifespan  not supported, continuing without it
[PAGI::Server]   loop      Poll, max_conn 1000, http2 available, tls available, future_xs off
[PAGI::Server::Connection] PAGI application error: the database is on fire
[PAGI::Server::Connection] PAGI application returned without starting a response
```

In `server.log`, only `warning` and above — the two real problems, without the
startup chatter:

```
[PAGI::Server::Connection] PAGI application error: the database is on fire
[PAGI::Server::Connection] PAGI application returned without starting a response
```

## Three things worth noticing

**`category` says who is talking.** `PAGI::Server` is the server's own
lifecycle; `PAGI::Server::Connection` is a per-request diagnostic. In a real
deployment that is what you route or filter on — connection noise and
lifecycle events usually want different treatment.

**The level translation is the point.** Log::Dispatch names its levels after
syslog and has no `fatal`. Handing it one is not an error: `level_is_valid`
returns false and the message is **discarded silently** — no output, no
exception, no warning. The two-line translation in `run.pl` is the whole reason
the sink is a coderef. An object with duck-typed level methods could not be
adapted without writing a wrapper class.

**Two thresholds, doing different jobs.** `log_level => 'debug'` is the
server's: below it, nothing reaches your sink at all. `min_level` on each
Log::Dispatch output is yours: the screen takes everything, the file takes
problems only. The server's threshold is a floor, not a policy.

## Other shapes

**JSON lines need no code.** `pagi-server --log-format json app.pl` writes one
JSON object per line to `STDERR`, and it is already the default when
`pagi-server` runs in production mode (no terminal, or `--env production`):

```
{"time":"2026-09-29T23:41:07.123Z","level":"error","category":"PAGI::Server::Connection","message":"PAGI application error: the database is on fire","pid":48211}
```

**A different schema is the reason to write a sink.** If your log pipeline
expects, say, Elastic Common Schema names, reshape the event yourself. Every
event carries `level`, `message`, `category` and `pid`, plus `worker` in a
multi-worker child:

```perl
use JSON::PP ();
use POSIX qw(strftime);
my $json = JSON::PP->new->canonical;

logger => sub {
    my ($event) = @_;
    print STDOUT $json->encode({
        '@timestamp'  => strftime('%Y-%m-%dT%H:%M:%SZ', gmtime),
        'log.level'   => $event->{level},
        'log.logger'  => $event->{category},
        'message'     => $event->{message},
        'process.pid' => $event->{pid},
    }), "\n";
},
```

If you only want the diagnostics in a **file** rather than reshaped, you do not
need a runner script at all — `pagi-server --error-log /var/log/pagi/error.log`
does that from the command line, in either format, and the destination
survives `--daemonize`.

## See also

- `PAGI::Server` — the `logger`, `log_level` and `log_format` options
- `PAGI::Server::Runner` — `--error-log`, and how it differs from `--access-log`
