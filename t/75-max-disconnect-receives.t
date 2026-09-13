use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Future::AsyncAwait;
use MIME::Base64 ();
use FindBin;
use lib "$FindBin::Bin/../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Test: max_disconnect_receives -- the cap on receives answered
#       with a synthesized disconnect event
# ============================================================
# L<PAGI::Spec::Www> ("Disconnect - receive event" for http and websocket,
# "SSE Disconnect - receive event") says a receive() made after the scope's
# disconnect event has been delivered resolves with that event again. An
# application whose receive loop never checks for the event therefore spins
# synchronously: the server answers from an already-resolved Future, the
# event loop never turns, and every other connection in the process is
# starved.
#
# PAGI::Server keeps the re-delivery and bounds it. After
# max_disconnect_receives such answers on one scope, the next receive()
# fails instead, which ends the application coroutine and returns the
# process to its event loop. The bound is configurable and 0 restores
# unbounded re-delivery. Www.pod "Meaning per scope", under "Receiving after
# the scope's end", allows exactly this and leaves the number to the server
# (PAGI::Server::Compliance, PAGI SPECIFICATION RULINGS); the cases below pin
# both the bound and the spec behaviours it must not disturb.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use Protocol::WebSocket::Frame;

my $have_h2 = do {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available ? 1 : 0;
};

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

my $DEFAULT_CAP = 100;

# Every server built here logs into this collector rather than STDERR, so a
# case can assert the exact set of lines its scope produced.
my @LOG;

sub cap_message {
    my ($n, $max) = @_;
    return "receive() called $n times after the scope ended; "
         . "the application is not checking for it "
         . "(PAGI::Server max_disconnect_receives=$max)";
}

# ============================================================
# HTTP/1.1 harness (shape borrowed from t/71)
# ============================================================

sub create_server {
    my ($app, %opts) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0,
        log_level => 'debug', access_log => undef, shutdown_timeout => 1,
        logger => sub { push @LOG, $_[0] },
        %opts,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub h1_request {
    my ($kind, $path) = @_;
    $path //= '/r';
    return "GET $path HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
         . "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: "
         . MIME::Base64::encode_base64('0123456789abcdef', '') . "\r\n\r\n"
        if $kind eq 'websocket';
    return "GET $path HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n"
        if $kind eq 'sse';
    return "GET $path HTTP/1.1\r\nHost: x\r\n\r\n";
}

# A connected client socket plus a reader that drains whatever the server has
# written so far. Readers are registered globally so one pump serves every
# open client -- the starvation case needs a second client's response to
# arrive while the first client's scope is being answered.
my @READERS;

sub h1_connect {
    my ($port, $request) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5)
        or die "connect: $!";
    print $sock $request if defined $request;
    $sock->blocking(0);
    my $wire = '';
    my $eof  = 0;
    my $reader = sub {
        return if $eof || !defined fileno($sock);
        my $buf;
        my $n = sysread($sock, $buf, 65536);
        if (defined $n) { $n ? ($wire .= $buf) : ($eof = 1) }
    };
    push @READERS, $reader;
    return ($sock, \$wire, \$eof);
}

# The cap is what stops a sloppy receive loop. Without it the application
# spins synchronously inside a single loop_once and nothing in this process
# regains control, so an alarm is the only bound a test can impose -- a
# guard in the application would hide the very behaviour under test. Every
# pump carries it, and a case that needed it records the fact for its own
# assertion rather than hanging the file.
my $ALARM_FIRED = 0;

sub bounded {
    my ($body, $seconds) = @_;
    local $SIG{ALRM} = sub { $ALARM_FIRED = 1; die "pump alarm\n" };
    alarm($seconds // 5);
    my $ok  = eval { $body->(); 1 };
    my $err = $@;
    alarm(0);
    die $err if !$ok && $err ne "pump alarm\n";
    return $ALARM_FIRED;
}

# $done is what the pump is waiting for. Once it holds the pump runs on for
# another 20 turns -- a second of settling, so an assertion that nothing
# further happens is made against a quiet loop -- and then stops, rather than
# spending its whole round count on a condition already met.
sub pump {
    my ($rounds, $done) = @_;
    my $after = 0;
    return bounded(sub {
        for (1 .. ($rounds // 20)) {
            $loop->loop_once(0.05);
            $_->() for @READERS;
            last if $done && $done->() && ++$after > 20;
        }
    });
}

sub reset_case {
    @LOG         = ();
    @READERS     = ();
    $ALARM_FIRED = 0;
}

# Every log line the case's scope produced, counted by level. A case takes
# its mark once its server is up, so the count covers the scope alone and
# not the startup lines the server writes when it begins listening. Asserted
# at every level, not just error: a line about the cap at any level that no
# case names is exactly the unasserted output the Compliance "Error logging"
# ruling forbids, and these servers log into a collector, so the suite's
# stderr diff never sees them.
sub log_levels_since {
    my ($mark) = @_;
    my %by_level;
    $by_level{ $_->{level} }++ for @LOG[$mark .. $#LOG];
    return \%by_level;
}

sub shutdown_server {
    my ($server) = @_;
    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
}

# ============================================================
# HTTP/2 harness (shape borrowed from t/71)
# ============================================================

sub create_h2_connection {
    my (%o) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app    = $o{app} // sub { };
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, http2 => 1,
        log_level => 'debug', access_log => undef,
        logger => sub { push @LOG, $_[0] },
        %{ $o{server_opts} // {} },
    );
    $loop->add($server);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        max_disconnect_receives => $server->{max_disconnect_receives},
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;
    return ($conn, $stream, $sock_b, $server);
}

sub create_client {
    my (%o) = @_;
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => $o{on_header}          // sub { 0 },
        on_frame_recv      => sub { 0 },
        on_data_chunk_recv => $o{on_data_chunk_recv} // sub { 0 },
        on_stream_close    => $o{on_stream_close}    // sub { 0 },
    });
}

sub complete_h2_handshake {
    my ($client, $client_sock) = @_;
    $loop->loop_once(0.1);
    my $settings = '';
    $client_sock->sysread($settings, 4096);
    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($settings);
    $loop->loop_once(0.1);
    my $ack = '';
    $client_sock->sysread($ack, 4096);
    $client->mem_recv($ack) if length($ack);
    my $out = $client->mem_send;
    $client_sock->syswrite($out) if length($out);
    $loop->loop_once(0.1);
    my $extra = '';
    $client_sock->sysread($extra, 4096);
    $client->mem_recv($extra) if length($extra);
}

sub exchange_frames {
    my ($client, $client_sock, $rounds, $done) = @_;
    my $after = 0;
    return bounded(sub {
        for (1 .. ($rounds // 20)) {
            $loop->loop_once(0.05);
            if (defined fileno($client_sock)) {   # the client may have left
                my $buf = '';
                $client_sock->sysread($buf, 16384);
                $client->mem_recv($buf) if length($buf);
                my $out = $client->mem_send;
                $client_sock->syswrite($out) if length($out);
            }
            last if $done && $done->() && ++$after > 20;
        }
    });
}

sub h2_submit {
    my ($client, $client_sock, $kind, $path) = @_;
    $path //= '/r';
    if ($kind eq 'websocket') {
        return $client->submit_request(
            method => 'CONNECT', path => $path, scheme => 'https', authority => 'localhost',
            headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
            body => sub { return undef },
        );
    }
    if ($kind eq 'sse') {
        return $client->submit_request(
            method => 'GET', path => $path, scheme => 'http', authority => 'localhost',
            headers => [['accept', 'text/event-stream']],
        );
    }
    return $client->submit_request(
        method => 'GET', path => $path, scheme => 'http', authority => 'localhost',
        headers => [],
    );
}

# ============================================================
# Applications
# ============================================================

# Bring the scope to the point where its next receive() can only ever be
# answered with the disconnect event: an accepted socket, a started stream,
# or a completed HTTP response.
my $start_scope = async sub {
    my ($kind, $receive, $send) = @_;
    if ($kind eq 'websocket') {
        await $receive->();                       # websocket.connect
        await $send->({ type => 'websocket.accept' });
        return;
    }
    if ($kind eq 'sse') {
        await $receive->();                       # sse.request
        await $send->({ type => 'sse.start' });
        return;
    }
    await $receive->();                           # http.request
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain'],
                                ['content-length', 2]] });
    await $send->({ type => 'http.response.body', body => 'ok' });
    return;
};

# The same, but leaving the http response open. A receive() on a completed
# http response is answered at once, so only a still-running response gives
# the case that needs receives genuinely parked when the client leaves.
my $start_open_scope = async sub {
    my ($kind, $receive, $send) = @_;
    return await $start_scope->($kind, $receive, $send) if $kind ne 'http';

    await $receive->();                           # http.request
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
    return;
};

# The starvation case needs a second, unrelated request answered on the same
# server while the first scope is being drowned in disconnect events. It
# reaches the same application, so the application answers /second itself.
my $answer_second = async sub {
    my ($receive, $send) = @_;
    await $receive->();
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain'],
                                ['content-length', 2]] });
    await $send->({ type => 'http.response.body', body => 'ok' });
    return;
};

my @KINDS = qw(websocket sse http);

my %DISCONNECT_TYPE = (
    websocket => 'websocket.disconnect',
    sse       => 'sse.disconnect',
    http      => 'http.disconnect',
);

# How many of a sloppy loop's answers are ordinary deliveries rather than
# capped ones, per scope, once the client has gone. A websocket or sse loop
# is parked on its first post-start receive when the disconnect is detected,
# so that one call is answered from the scope's queued event. An h1 http loop
# never gets that far: its first receive after a completed response is
# answered from the completed-response branch, before the client has even
# left, so every one of its answers is a capped one.
my %DELIVERED_H1 = (websocket => 1, sse => 1, http => 0);
my %DELIVERED_H2 = (websocket => 1, sse => 1, http => 1);

# The line the existing app-exception path logs when the failed receive ends
# the coroutine. The cap does not suppress it and does not replace it.
my %APP_ERROR_H1 = (
    websocket => 'PAGI application error (WebSocket)',
    sse       => 'PAGI application error (SSE)',
    http      => 'PAGI application error (after response complete)',
);

# On HTTP/2 the exception reaches an application whose scope the peer already
# reset, and the existing client-already-gone carve-out swallows it -- except
# on a plain http stream, which logs it.
my %APP_ERROR_H2 = (
    websocket => undef,
    sse       => undef,
    http      => 'PAGI application error after response started (HTTP/2 stream 1)',
);

# ============================================================
# 1. The default cap stops a sloppy loop and un-starves the process
# ============================================================

for my $kind (@KINDS) {
    subtest "h1 $kind: a sloppy receive loop is capped, and the process keeps serving" => sub {
        reset_case();
        my $n      = 0;
        my @types;
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            return await $answer_second->($receive, $send)
                if ($scope->{path} // '') eq '/second';

            await $start_scope->($kind, $receive, $send);
            while (1) {
                my $ev = await $receive->();
                $n++;
                push @types, $ev->{type} if $n <= 3;
            }
        };
        my $server = create_server($app);
        my $mark   = scalar @LOG;
        my $port   = $server->port;

        my ($sock) = h1_connect($port, h1_request($kind));
        pump(10);
        close $sock;                                      # the client drops

        my ($sock2, $wire2) = h1_connect($port, h1_request('http', '/second'));
        # The second client is answered only once the cap has handed the event
        # loop back, which is after the capped loop has stopped and both error
        # lines are out.
        pump(40, sub { $$wire2 =~ m{^HTTP/1\.1 200} });

        my $expected = $DEFAULT_CAP + $DELIVERED_H1{$kind};
        ok(!$ALARM_FIRED, 'the cap stopped the loop; the test alarm never fired');
        is($n, $expected, "loop ran $DEFAULT_CAP capped answers plus its deliveries");
        is([map { $_ } @types], [($DISCONNECT_TYPE{$kind}) x 3],
            "every answer was $DISCONNECT_TYPE{$kind}");

        my $message = cap_message($DEFAULT_CAP + 1, $DEFAULT_CAP);
        is([map { $_->{message} } grep { $_->{level} eq 'error' } @LOG],
            [ "$kind scope on HTTP/1.1: $message",
              "$APP_ERROR_H1{$kind}: $message" ],
            'the cap line, then the app exception the failed receive raised');
        is(log_levels_since($mark), { error => 2 },
            'those two error lines are everything the scope logged, at any level');

        like($$wire2, qr{^HTTP/1\.1 200 }, 'a second client was answered while the scope spun');

        close $sock2;
        shutdown_server($server);
    };
}

# ============================================================
# 2. max_disconnect_receives => 0 restores the spec's unbounded re-delivery
# ============================================================

for my $kind (@KINDS) {
    subtest "h1 $kind: max_disconnect_receives => 0 re-delivers without limit" => sub {
        reset_case();
        my $n     = 0;
        my $died  = '';
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            await $start_scope->($kind, $receive, $send);
            for my $i (1 .. 150) {
                my $ev = await $receive->();
                $n++ if ($ev->{type} // '') eq $DISCONNECT_TYPE{$kind};
            }
            return;
        };
        my $server = create_server($app, max_disconnect_receives => 0);
        my $mark   = scalar @LOG;
        my $port   = $server->port;

        my ($sock) = h1_connect($port, h1_request($kind));
        pump(10);
        close $sock;
        pump(30, sub { $n == 150 });

        ok(!$ALARM_FIRED, 'the bounded application loop finished on its own');
        is($n, 150, 'all 150 receives resolved with the disconnect event');
        is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

        shutdown_server($server);
    };
}

# ============================================================
# 3. Receives pending at detection are deliveries, not capped answers
# ============================================================

for my $kind (@KINDS) {
    subtest "h1 $kind: receives pending at disconnect do not count against the cap" => sub {
        reset_case();
        my @pending_types;
        my $after   = 0;
        my $failure = '';
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            await $start_open_scope->($kind, $receive, $send);

            my @f = map { $receive->() } 1 .. 3;
            for my $f (@f) { push @pending_types, (await $f)->{type} }

            for my $i (1 .. $DEFAULT_CAP) {
                await $receive->();
                $after++;
            }
            my $ok = eval { await $receive->(); 1 };
            $failure = $ok ? '' : "$@";
            return;
        };
        my $server = create_server($app);
        my $mark   = scalar @LOG;
        my $port   = $server->port;

        my ($sock) = h1_connect($port, h1_request($kind));
        pump(10);
        close $sock;
        pump(30, sub { length $failure });

        ok(!$ALARM_FIRED, 'the application finished without the test alarm');
        is(\@pending_types, [($DISCONNECT_TYPE{$kind}) x 3],
            'all three pending receives resolved with the disconnect event');
        is($after, $DEFAULT_CAP, "the next $DEFAULT_CAP receives resolved");
        like($failure, qr/\Q@{[ cap_message($DEFAULT_CAP + 1, $DEFAULT_CAP) ]}\E/,
            'the call after the cap failed');
        is(log_levels_since($mark), { error => 1 },
            'the cap line alone: the application caught the failure itself');

        shutdown_server($server);
    };
}

# ============================================================
# 4. The abandoned-Future race is untouched by the default cap
# ============================================================

for my $kind (@KINDS) {
    subtest "h1 $kind: an abandoned receive Future still leaves the next one answerable" => sub {
        reset_case();
        my $late_type = '';
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            await $start_scope->($kind, $receive, $send);

            # The application races a receive against a timer, keeps the
            # loser alive, and asks again after the client has gone.
            my $old   = $receive->()->without_cancel;
            my $timer = $loop->delay_future(after => 0.05);
            await Future->wait_any($timer, $old);

            $late_type = (await $receive->())->{type};
            return;
        };
        my $server = create_server($app);
        my $mark   = scalar @LOG;
        my $port   = $server->port;

        my ($sock) = h1_connect($port, h1_request($kind));
        pump(10);
        close $sock;
        pump(30, sub { length $late_type });

        ok(!$ALARM_FIRED, 'the application finished without the test alarm');
        is($late_type, $DISCONNECT_TYPE{$kind}, 'the later receive resolved with the event');
        is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

        shutdown_server($server);
    };
}

# ============================================================
# 5. The option itself
# ============================================================

subtest 'max_disconnect_receives as a constructor and configure parameter' => sub {
    reset_case();
    my $app = async sub { return };

    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    is($server->{max_disconnect_receives}, $DEFAULT_CAP, 'default is 100');

    $server->configure(max_disconnect_receives => 7);
    is($server->{max_disconnect_receives}, 7, 'configure changes the cap');

    $server->configure(max_disconnect_receives => 0);
    is($server->{max_disconnect_receives}, 0, 'configure accepts 0 (unlimited)');

    like(dies { PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0,
                                  quiet => 1, max_disconnect_receives => -1) },
        qr/max_disconnect_receives/, 'a negative cap dies at construction');

    like(dies { $server->configure(max_disconnect_receives => -1) },
        qr/max_disconnect_receives/, 'a negative cap dies in configure');
};

subtest 'a Connection built by the server carries the cap' => sub {
    reset_case();
    my $app = async sub { return };
    my $server = create_server($app, max_disconnect_receives => 42);
    my $mark   = scalar @LOG;
    my $port   = $server->port;

    my ($sock) = h1_connect($port, undef);
    pump(5);

    my ($conn) = values %{ $server->{connections} };
    ok(!$ALARM_FIRED, 'the connection was accepted without the test alarm');
    ok($conn, 'the server tracked the connection');
    is($conn->{max_disconnect_receives}, 42, 'the connection carries the configured cap');
    is(log_levels_since($mark), {}, 'accepting the connection logged nothing at any level');

    close $sock;
    shutdown_server($server);
};

# ============================================================
# 6. A receive() the application kept past the connection object
# ============================================================
# L<PAGI::Spec::Www/"SSE Disconnect - receive event"> (and the http and
# websocket twins) require such a call to resolve with the scope's disconnect
# event. The receive closures hold the connection weakly, so an application
# that hands $receive to a background task can call it once nothing else
# refers to the connection -- and the cap's gate must answer there too rather
# than call a method on the undefined reference.
#
# Built from the closures directly, as the HTTP/2 cases build their
# connection: a Connection reached through a listening server stays
# referenced for as long as the application it dispatched can still be
# running, so no socket-driven shape can free it while $receive is callable.
subtest 'a receive() made after the connection object is gone still answers' => sub {
    reset_case();
    my $app = async sub { return };
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    $loop->add($server);

    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    my $request = { content_length => undef, chunked => 0, expect_continue => 0,
                    method => 'GET', path => '/r' };

    for my $kind (@KINDS) {
        my $stream = IO::Async::Stream->new(
            read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
        my $conn = PAGI::Server::Connection->new(
            stream => $stream, app => $app, protocol => $protocol, server => $server,
            max_disconnect_receives => $DEFAULT_CAP,
        );
        my $receive = $kind eq 'websocket' ? $conn->_create_websocket_receive($request)
                    : $kind eq 'sse'       ? $conn->_create_sse_receive($request)
                    :                        $conn->_create_receive($request);
        undef $conn;

        my $type = eval { $receive->()->get->{type} };
        is($type, $DISCONNECT_TYPE{$kind},
            "h1 $kind: the freed connection's receive resolved with the event");
    }

    $loop->remove($server);
};

# ============================================================
# HTTP/2
# ============================================================

SKIP: {
    skip 'HTTP/2 not available', 12 unless $have_h2;

    for my $kind (@KINDS) {
        subtest "h2 $kind: a sloppy receive loop is capped" => sub {
            reset_case();
            my $n = 0;
            my $app = async sub {
                my ($scope, $receive, $send) = @_;
                await $start_scope->($kind, $receive, $send);

                # The scope's own disconnect event, then a loop that never
                # checks for it -- no yield in between, the same shape as the
                # h1 arm. Every answer after the first is a capped one.
                await $receive->();
                $n++;

                while (1) { await $receive->(); $n++ }
            };
            # No second stream here: the h1 arm already pins that the cap
            # hands the event loop back while another client is waiting, and
            # the cap is the same per-scope counter on both transports. This
            # arm pins the counting and the log line for an HTTP/2 scope.
            my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
            my $mark   = scalar @LOG;
            my $client = create_client();
            complete_h2_handshake($client, $client_sock);
            my $sid = h2_submit($client, $client_sock, $kind);
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 10);

            $client->submit_rst_stream($sid, 8);          # CANCEL
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 30,
                            sub { $n >= $DEFAULT_CAP + $DELIVERED_H2{$kind} });

            ok(!$ALARM_FIRED, 'the cap stopped the loop; the test alarm never fired');
            is($n, $DEFAULT_CAP + $DELIVERED_H2{$kind},
                "loop ran $DEFAULT_CAP capped answers plus its deliveries");

            my $message = cap_message($DEFAULT_CAP + 1, $DEFAULT_CAP);
            is([map { $_->{message} } grep { $_->{level} eq 'error' } @LOG],
                [ "$kind scope on HTTP/2 stream $sid: $message",
                  $APP_ERROR_H2{$kind} ? "$APP_ERROR_H2{$kind}: $message" : () ],
                'the cap line, then whatever the app-exception path logs');
            is(log_levels_since($mark), { error => 1 + ($APP_ERROR_H2{$kind} ? 1 : 0) },
                'those error lines are everything the scope logged, at any level');

            $stream_io->close_now;
            shutdown_server($server);
        };
    }

    for my $kind (@KINDS) {
        subtest "h2 $kind: max_disconnect_receives => 0 re-delivers without limit" => sub {
            reset_case();
            my $n = 0;
            my $app = async sub {
                my ($scope, $receive, $send) = @_;
                await $start_scope->($kind, $receive, $send);
                await $loop->delay_future(after => 0.05)
                    if ($kind eq 'http');   # let the completed stream be reclaimed
                for my $i (1 .. 150) {
                    my $ev = await $receive->();
                    $n++ if ($ev->{type} // '') eq $DISCONNECT_TYPE{$kind};
                    await $loop->delay_future(after => 0.05) if $i == 1;
                }
                return;
            };
            my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(
                app => $app, server_opts => { max_disconnect_receives => 0 });
            my $mark   = scalar @LOG;
            my $client = create_client();
            complete_h2_handshake($client, $client_sock);
            my $sid = h2_submit($client, $client_sock, $kind);
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 10);

            $client->submit_rst_stream($sid, 8);
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 40, sub { $n == 150 });

            ok(!$ALARM_FIRED, 'the bounded application loop finished on its own');
            is($n, 150, 'all 150 receives resolved with the disconnect event');
            is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

            $stream_io->close_now;
            shutdown_server($server);
        };
    }

    for my $kind (@KINDS) {
        subtest "h2 $kind: receives pending at disconnect do not count against the cap" => sub {
            reset_case();

            # Three receives held parked across the scope's ending, and the
            # capped loop that follows them made in the same turn as their
            # delivery, exactly as the h1 arm does. A websocket or sse scope
            # ends here with the connection and the close sweep answers all
            # three; an http scope is ended by RST_STREAM and has one queued
            # http.disconnect, so the other two are answered from the ended
            # stream (t/http2/43). None of the three counts against the cap.
            my $pending = 3;

            my @pending_types;
            my $after   = 0;
            my $failure = '';
            my $app = async sub {
                my ($scope, $receive, $send) = @_;
                await $start_open_scope->($kind, $receive, $send);

                my @f = map { $receive->() } 1 .. $pending;
                for my $f (@f) { push @pending_types, (await $f)->{type} }

                for my $i (1 .. $DEFAULT_CAP) {
                    await $receive->();
                    $after++;
                }
                my $ok = eval { await $receive->(); 1 };
                $failure = $ok ? '' : "$@";
                return;
            };
            my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
            my $mark   = scalar @LOG;
            my $client = create_client();
            complete_h2_handshake($client, $client_sock);
            my $sid = h2_submit($client, $client_sock, $kind);
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 10);

            if ($kind eq 'http') {
                $client->submit_rst_stream($sid, 8);      # CANCEL
                $client_sock->syswrite($client->mem_send);
            }
            else {
                $client_sock->close;                      # the connection goes
            }
            exchange_frames($client, $client_sock, 20);

            ok(!$ALARM_FIRED, 'the application finished without the test alarm');
            is(\@pending_types, [($DISCONNECT_TYPE{$kind}) x $pending],
                "all $pending pending receives resolved with the disconnect event");
            is($after, $DEFAULT_CAP, "the next $DEFAULT_CAP receives resolved");
            like($failure, qr/\Q@{[ cap_message($DEFAULT_CAP + 1, $DEFAULT_CAP) ]}\E/,
                'the call after the cap failed');
            is(log_levels_since($mark), { error => 1 },
                'the cap line alone: the application caught the failure itself');

            eval { $stream_io->close_now };
            shutdown_server($server);
        };
    }

    for my $kind (@KINDS) {
        subtest "h2 $kind: an abandoned receive Future still leaves the next one answerable" => sub {
            reset_case();
            my $late_type = '';
            my $resume    = $loop->new_future;
            my $app = async sub {
                my ($scope, $receive, $send) = @_;
                await $start_scope->($kind, $receive, $send);

                # The application races a receive against a timer, keeps the
                # loser alive, and asks again after the client has gone. The
                # receive Future is held in its own variable as well: an
                # HTTP/2 connection keeps no reference of its own to a receive
                # in flight the way the HTTP/1.1 connection's receive_futures
                # does, so letting go of it before the stream ends would
                # collect the suspended receive instead of racing it.
                my $abandoned = $receive->();
                my $old       = $abandoned->without_cancel;
                my $timer     = $loop->delay_future(after => 0.05);
                await Future->wait_any($timer, $old);

                # $resume is how the test says "the client has gone": the
                # timer wins the race above long before the RST arrives, so
                # without it the late receive below would be made while the
                # stream was still open and would pin nothing.
                await $resume;

                $late_type = (await $receive->())->{type};
                return;
            };
            my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
            my $mark   = scalar @LOG;
            my $client = create_client();
            complete_h2_handshake($client, $client_sock);
            my $sid = h2_submit($client, $client_sock, $kind);
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 10);

            $client->submit_rst_stream($sid, 8);          # CANCEL
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 10);

            $resume->done;
            exchange_frames($client, $client_sock, 20);

            ok(!$ALARM_FIRED, 'the application finished without the test alarm');
            is($late_type, $DISCONNECT_TYPE{$kind}, 'the later receive resolved with the event');
            is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

            eval { $stream_io->close_now };
            shutdown_server($server);
        };
    }
}

done_testing;
