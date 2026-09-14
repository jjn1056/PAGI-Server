use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use IO::Socket::INET;
use Socket qw(SO_RCVBUF);
use MIME::Base64 ();
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Test: abort() end to end on HTTP/1.1, on every scope
# ============================================================
# PAGI::Spec::Www, "Connection Object Interface": abort "Requests that the
# server end this scope's transport now: close the connection on HTTP/1.1 ...
# it is not awaitable and MUST NOT wait for an in-flight write to drain". The
# server then follows "State Transition Order" exactly as for a
# transport-detected abnormal end -- app_abort with the application's detail,
# on_disconnect (never on_complete), the scope's disconnect event to pending
# receives, pending sends settled successfully, later sends post-close no-ops.
#
# Two consequences are the subject of most of the assertions below. "The
# server MUST NOT log an incomplete-response or no-response error for an
# aborted scope": every row asserts zero error lines and, by name, that the
# websocket D13 and sse D12 incomplete paths did not fire. And "Before accept
# or start it ends the handshake with no HTTP response: the client observes a
# failed connection, not a status."
#
# The unit-level contract (idempotence, the hook, the detail) is t/37's; this
# file is about what reaches the wire and what the application observes.

my $loop = IO::Async::Loop->new;

sub create_server {
    my ($app, %opts) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, shutdown_timeout => 1,
        %opts,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub ws_request {
    return "GET /socket HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
         . "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: "
         . MIME::Base64::encode_base64('0123456789abcdef', '') . "\r\n\r\n";
}
sub sse_request  { return "GET /events HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n" }
sub http_request { my ($path) = @_; return "GET " . ($path // '/r') . " HTTP/1.1\r\nHost: x\r\n\r\n" }

# Open a socket, send $request, and pump the loop, accumulating everything the
# server writes. Returns the still-open socket and a reader closure, so a row
# can inspect the wire part-way through and abort from outside the app.
sub h1_open {
    my ($port, $request) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5)
        or die "connect: $!";
    print $sock $request;
    $sock->blocking(0);
    my $wire = '';
    my $eof  = 0;
    my $pump = sub {
        my ($rounds) = @_;
        for (1 .. ($rounds // 20)) {
            $loop->loop_once(0.05);
            my $buf;
            my $n = sysread($sock, $buf, 65536);
            if (defined $n) { $n ? ($wire .= $buf) : ($eof = 1) }
        }
        return $wire;
    };
    return ($sock, $pump, \$wire, \$eof);
}

# The error lines a server logs, and the two incomplete-response paths that an
# aborted scope must never reach.
sub errors_in { return [map { $_->{message} } grep { ($_->{level} // '') eq 'error' } @{$_[0]}] }
my $INCOMPLETE = qr/without a closing handshake|without sse\.close|incomplete response|returned without/;

# ============================================================
# 1. websocket, before accept
# ============================================================

subtest 'websocket before accept: abort ends the handshake with no response bytes' => sub {
    my (%r, @log);
    my $lookup = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $c = $scope->{'pagi.connection'};
        $c->on_disconnect(sub { push @{$r{disc}}, [@_] });
        $c->on_complete(sub { $r{complete}++ });
        await $receive->();                       # websocket.connect
        # Park a receive before the abort: Www.pod "Disconnect event" names the
        # pre-accept abort's event, 1006 with the condition's reason token.
        my $parked = $receive->();
        $r{parked} = 1;
        my $verdict = await $lookup;              # a pool lookup, resolved by the test
        $c->abort('policy') if $verdict eq 'deny';
        $r{event}     = await $parked;            # settled by abort's own teardown
        # "treat later sends as post-close no-ops": this resolves, it does not fail.
        $r{send_ok}   = eval { await $send->({ type => 'websocket.accept' }); 1 } ? 1 : 0;
        $r{send_err}  = $@;
        $r{reason}    = $c->disconnect_reason;
        $r{detail}    = $c->disconnect_detail;
        $r{connected} = $c->is_connected;
        $r{started}   = $c->response_started;
        $r{done}      = 1;
    };
    my $server = create_server($app, logger => sub { push @log, $_[0] });
    my ($sock, $pump, $wire, $eof) = h1_open($server->port, ws_request());
    $pump->(20);
    ok($r{parked}, 'the app parked on its lookup');
    is($$wire, '', 'nothing has reached the client yet');

    $lookup->done('deny');
    $pump->(30);
    close $sock;
    $server->shutdown->get;

    ok($r{done}, 'the app ran to completion');
    is($$wire, '', 'the client observes a failed connection, not a status');
    ok($$eof, 'the transport was closed');
    is($r{send_ok}, 1, 'the send after abort resolved as a post-close no-op')
        or diag("send error: $r{send_err}");
    is($r{event}{type}, 'websocket.disconnect',
        'the parked receive observed websocket.disconnect');
    is($r{event}{code}, 1006, 'code 1006 (no Close frame was exchanged)');
    is($r{event}{reason}, 'app_abort', 'reason app_abort');
    is($r{reason}, 'app_abort', 'disconnect_reason is app_abort');
    is($r{detail}, 'policy', 'disconnect_detail is the application string');
    is($r{connected}, 0, 'is_connected is false');
    is($r{started}, 0, 'response_started stayed false');
    is(scalar @{$r{disc} // []}, 1, 'on_disconnect fired exactly once');
    is($r{disc}[0], ['app_abort', 'policy'], 'with the token and the detail');
    is($r{complete}, undef, 'on_complete never fired');
    is(errors_in(\@log), [], 'no error line was logged for the aborted scope');
    is([grep { /$INCOMPLETE/ } @{errors_in(\@log)}], [],
        'neither incomplete-response path fired');
};

# ============================================================
# 2. websocket, after accept
# ============================================================

subtest 'websocket after accept: abort sends no Close frame and delivers 1006 app_abort' => sub {
    my (%r, @log);
    my $gate = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $c = $scope->{'pagi.connection'};
        $c->on_disconnect(sub { push @{$r{disc}}, [@_] });
        $c->on_complete(sub { $r{complete}++ });
        await $receive->();                       # websocket.connect
        await $send->({ type => 'websocket.accept' });
        await $send->({ type => 'websocket.send', text => 'live' });
        $r{accepted} = 1;
        await $gate;
        my $parked = $receive->();                # genuinely parked before the abort
        $c->abort('mid-session');
        my $ev = await $parked;                   # settled by the abort's own teardown
        $r{event}  = $ev;
        $r{reason} = $c->disconnect_reason;
        $r{detail} = $c->disconnect_detail;
        $r{done}   = 1;
    };
    my $server = create_server($app, logger => sub { push @log, $_[0] });
    my ($sock, $pump, $wire, $eof) = h1_open($server->port, ws_request());
    $pump->(25);
    ok($r{accepted}, 'the socket was accepted and carried a frame');
    like($$wire, qr/\x81\x04live/, 'the application frame reached the client');

    $gate->done;
    $pump->(30);
    close $sock;
    $server->shutdown->get;

    ok($r{done}, 'the app ran to completion');
    # Every byte the server wrote here is an HTTP header, a base64 key, or an
    # ASCII text frame, so a 0x88 octet could only be a Close frame opcode.
    unlike($$wire, qr/\x88/, 'no Close frame was sent: an abort is not a closing handshake');
    ok($$eof, 'the transport was closed');
    is($r{event}{type}, 'websocket.disconnect', 'the parked receive got websocket.disconnect');
    is($r{event}{code}, 1006, 'code 1006: the abnormal closure of RFC 6455');
    is($r{event}{reason}, 'app_abort',
        'the event reason agrees with the object (Www.pod "Agreement with disconnect events")');
    is($r{reason}, 'app_abort', 'disconnect_reason is app_abort');
    is($r{detail}, 'mid-session', 'disconnect_detail is the application string');
    is(scalar @{$r{disc} // []}, 1, 'on_disconnect fired exactly once');
    is($r{complete}, undef, 'on_complete never fired');
    is(errors_in(\@log), [],
        'no error line was logged: the D13 incomplete-session path must not fire for an aborted socket');
};

# ============================================================
# 3. sse, after start
# ============================================================

subtest 'sse after start: abort ends the stream with no terminator and delivers app_abort' => sub {
    my (%r, @log);
    my $gate = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'sse';
        my $c = $scope->{'pagi.connection'};
        $c->on_disconnect(sub { push @{$r{disc}}, [@_] });
        $c->on_complete(sub { $r{complete}++ });
        await $receive->();                       # sse.request
        await $send->({ type => 'sse.start', status => 200,
                        headers => [['content-type', 'text/event-stream']] });
        await $send->({ type => 'sse.send', data => 'one' });
        $r{started} = 1;
        # Park until the test has seen the event on the wire. abort closes the
        # transport without draining, so anything still in the write buffer at
        # that moment is discarded -- the assertion below is about a stream the
        # client really was reading, not about what a race left behind.
        await $gate;
        my $parked = $receive->();                # genuinely parked before the abort
        $c->abort('drained');
        my $ev = await $parked;
        $r{event}  = $ev;
        $r{reason} = $c->disconnect_reason;
        $r{detail} = $c->disconnect_detail;
        $r{done}   = 1;
    };
    my $server = create_server($app, logger => sub { push @log, $_[0] });
    my ($sock, $pump, $wire, $eof) = h1_open($server->port, sse_request());
    $pump->(25);
    ok($r{started}, 'the stream started');
    like($$wire, qr/data: one/, 'the event that was sent reached the client');

    $gate->done;
    $pump->(30);
    close $sock;
    $server->shutdown->get;

    ok($r{done}, 'the app ran to completion');
    unlike($$wire, qr/\r\n0\r\n\r\n/, 'no chunked terminator: the stream has no end-of-stream marker');
    ok($$eof, 'the transport was closed');
    is($r{event}{type}, 'sse.disconnect', 'the parked receive got sse.disconnect');
    is($r{event}{reason}, 'app_abort', 'the event reason agrees with the object');
    is($r{reason}, 'app_abort', 'disconnect_reason is app_abort');
    is($r{detail}, 'drained', 'disconnect_detail is the application string');
    is(scalar @{$r{disc} // []}, 1, 'on_disconnect fired exactly once');
    is($r{complete}, undef, 'on_complete never fired');
    is(errors_in(\@log), [],
        'no error line was logged: the D12 incomplete-stream path must not fire for an aborted stream');
};

# ============================================================
# 4. http, mid-body, with a send parked on backpressure
# ============================================================
# "it is not awaitable and MUST NOT wait for an in-flight write to drain,
# since that write may be blocked by the peer". The peer here is a client that
# has stopped reading, so close_when_empty would never return.

subtest 'http mid-body: abort resolves a send parked on backpressure and closes now' => sub {
    my (%obs, @log);
    # Large enough that the outstanding write cannot fit in the sending
    # kernel's socket buffer. A 16KB chunk parks the send on the watermark
    # just as well, but the kernel then swallows the whole backlog, and
    # close_when_empty finishes promptly even though the peer never read a
    # byte -- which would make the close_now assertion below vacuous.
    my $payload = 'h' x (2 * 1024 * 1024);
    my $cs;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'http';
        $cs = $scope->{'pagi.connection'};
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type', 'text/plain']] });
        for my $n (1 .. 400) {
            $obs{started}++;
            my $f = $send->({ type => 'http.response.body', body => $payload, more => 1 });
            my $ok = eval { await $f; 1 };
            $obs{last_ready}     = $f->is_ready     ? 1 : 0;
            $obs{last_cancelled} = $f->is_cancelled ? 1 : 0;
            unless ($ok) { $obs{send_failed} = "$@"; last }
            $obs{completed}++;
            last unless $cs->is_connected;        # the abort ended the scope
        }
        $obs{app_completed} = 1;
        return;                                   # no terminal body: the scope is already over
    };

    my $server = create_server($app,
        logger => sub { push @log, $_[0] },
        write_high_watermark => 8192, write_low_watermark => 2048);
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 5) or die $!;
    $sock->sockopt(SO_RCVBUF, 4096);              # shrink so an unread response backs up quickly
    $sock->blocking(0);
    print $sock http_request('/flood');

    # Wait for the flood to park: started outruns completed and stays that way.
    my $parked = 0;
    for (1 .. 80) {
        if ($obs{started} && $obs{started} == ($obs{completed} // 0) + 1) {
            my ($s, $c) = ($obs{started}, $obs{completed});
            $loop->loop_once(0.1);
            $parked = ($obs{started} == $s && $obs{completed} == $c);
            last if $parked;
        }
        $loop->loop_once(0.1);
    }
    ok($parked, 'an http body send is parked on backpressure')
        or diag("started=$obs{started} completed=$obs{completed}");
    ok($cs, 'the app captured its connection object');

    $cs->abort('overloaded');                     # MUST NOT wait for the parked write
    $loop->loop_once(0.05);                       # one turn

    ok($obs{app_completed}, 'the application resumed within one loop turn');
    is($obs{last_ready}, 1, 'the parked send is ready');
    is($obs{last_cancelled}, 0, 'and it resolved rather than being cancelled');
    ok(!$obs{send_failed}, 'the parked send resolved successfully, not failed')
        or diag("send failed with: $obs{send_failed}");
    is($cs->disconnect_reason, 'app_abort', 'disconnect_reason is app_abort');
    is($cs->disconnect_detail, 'overloaded', 'disconnect_detail is the application string');
    is($cs->response_complete, 0, 'response_complete is false after an abnormal end');

    # What separates close_now from close_when_empty is not that the socket
    # eventually closes -- both close once this client resumes reading -- but
    # WHAT the client can still receive. close_now discards the outstanding
    # write; close_when_empty delivers all of it first. So drain to EOF and
    # count: measured here, close_now yields 8KB and close_when_empty the
    # whole 2MB body.
    my ($total, $closed) = (0, 0);
    for (1 .. 2000) {
        my $buf; my $n = sysread($sock, $buf, 65536);
        if (defined $n) { $n ? ($total += $n) : ($closed = 1) }
        last if $closed;
        $loop->loop_once(0.005);
    }
    ok($closed, 'the client sees the socket closed');
    ok($total < length($payload),
        'the outstanding write was discarded, not drained: abort did not wait for it')
        or diag("received $total bytes of a " . length($payload) . " byte body");

    close $sock;
    $server->shutdown->get;
    is(errors_in(\@log), [],
        'no incomplete-response error was logged for the aborted response');
};

# ============================================================
# 5. abort after a completed response
# ============================================================
# "abort after either terminal outcome is a no-op that preserves that
# outcome." The clean end is marked when the application returns, so the test
# holds the object and aborts from outside, after the response is complete.

subtest 'abort after a completed response is a no-op that preserves the completion' => sub {
    my (%r, @log);
    my $cs;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'http';
        my $c = $scope->{'pagi.connection'};
        $cs //= $c;
        $c->on_disconnect(sub { push @{$r{disc}}, [@_] });
        $c->on_complete(sub { $r{complete}++ });
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type', 'text/plain'], ['content-length', 2]] });
        await $send->({ type => 'http.response.body', body => 'ok' });
        return;
    };
    my $server = create_server($app, logger => sub { push @log, $_[0] });
    my ($sock, $pump, $wire, $eof) = h1_open($server->port, http_request('/done'));
    $pump->(25);
    like($$wire, qr{^HTTP/1\.1 200}, 'the response was delivered');
    ok($cs, 'the app captured its connection object');
    is($cs->response_complete, 1, 'the scope ended cleanly');

    $cs->abort('too late');
    $pump->(10);

    is($cs->response_complete, 1, 'response_complete is still true after abort');
    is($cs->disconnect_reason, undef, 'no disconnect reason was recorded');
    is($r{complete}, 1, 'on_complete had fired, exactly once');
    is($r{disc}, undef, 'on_disconnect never fired');
    ok(!$$eof, 'the keep-alive connection is still open');

    print $sock http_request('/again');
    $pump->(25);
    # The first response's body carries no trailing newline, so the second
    # status line is not at the start of a line: match it anywhere.
    my @statuses = ($$wire =~ m{HTTP/1\.1 (\d\d\d)}g);
    is(scalar @statuses, 2, 'a second request was served on the same connection')
        or diag("statuses: @statuses");

    close $sock;
    $server->shutdown->get;
    is(errors_in(\@log), [], 'nothing was logged');
};

done_testing;
