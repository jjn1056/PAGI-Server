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

On the terminal, everything from `debug` up:

```
[PAGI::Server] Lifespan not supported, continuing without it
[PAGI::Server] PAGI::Server 0.002012 (PAGI 0.002008) listening on http://127.0.0.1:5000/
[PAGI::Server]   loop Poll, max_conn 1000, http2 available, tls available, future_xs not installed
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

Structured output is the same seam with a different body — no other change:

```perl
use JSON::PP ();
my $json = JSON::PP->new->canonical;

logger => sub {
    my ($event) = @_;
    print STDOUT $json->encode({
        ts    => scalar(gmtime),
        level => $event->{level},
        src   => $event->{category},
        msg   => $event->{message},
    }), "\n";
},
```

If you only want the diagnostics in a **file** rather than reshaped, you do not
need a runner script at all — `pagi-server --error-log /var/log/pagi/error.log`
does that from the command line, and the destination survives `--daemonize`.

## See also

- `PAGI::Server` — the `logger` and `log_level` options
- `PAGI::Server::Runner` — `--error-log`, and how it differs from `--access-log`
