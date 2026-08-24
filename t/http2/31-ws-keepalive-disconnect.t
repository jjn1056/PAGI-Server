use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use Time::HiRes ();

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# ============================================================
# Test: exactly-one h2 WebSocket disconnect (design section 10.3)
# ============================================================
# Before this task, a single h2 WebSocket close could enqueue up to
# three websocket.disconnect events for one scope: the peer close-frame
# path (correct code/reason), the bare END_STREAM eof path (wrongly
# 1005, ''), and the on_close ws branch (wrongly 1006, ''). This file
# drives each closure shape and verifies the app's receive() stream
# carries exactly one disconnect, paired with the code/reason Www.pod
# "Disconnect - receive event" mandates:
#   - peer Close frame  -> the peer's own code (1005 when the frame
#     carried none) and the peer's reason text;
#   - close WITHOUT a close handshake (bare END_STREAM, RST_STREAM,
#     timeouts) -> 1006 and the 'client_closed' token.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

use constant H2_CANCEL_CODE => 8;   # RST_STREAM error code CANCEL (RFC 9113)

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (lifted from t/http2/07-websocket.t)
# ============================================================

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app   => $args{app} // sub { },
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
        http2 => 1,
        %args,
    );
    $loop->add($server);
    return $server;
}

sub create_h2_connection {
    my (%overrides) = @_;

    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0)
        or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);

    my $app = $overrides{app} // sub { };
    my $server = $overrides{server} // create_test_server(app => $app);

    my $stream = IO::Async::Stream->new(
        read_handle  => $sock_a,
        write_handle => $sock_a,
        on_read => sub { 0 },
    );

    my $conn = PAGI::Server::Connection->new(
        stream        => $stream,
        app           => $app,
        protocol      => $protocol,
        server        => $server,
        h2_protocol   => $server->{http2_protocol},
        alpn_protocol => 'h2',
        max_body_size => $server->{max_body_size},
    );

    $server->add_child($stream);
    $conn->start;

    return ($conn, $stream, $sock_b, $server);
}

sub create_client {
    my (%overrides) = @_;
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(
        callbacks => {
            on_begin_headers   => $overrides{on_begin_headers}   // sub { 0 },
            on_header          => $overrides{on_header}          // sub { 0 },
            on_frame_recv      => $overrides{on_frame_recv}      // sub { 0 },
            on_data_chunk_recv => $overrides{on_data_chunk_recv} // sub { 0 },
            on_stream_close    => $overrides{on_stream_close}    // sub { 0 },
        },
    );
}

sub complete_h2_handshake {
    my ($client, $client_sock) = @_;

    $loop->loop_once(0.1);
    my $server_settings = '';
    $client_sock->sysread($server_settings, 4096);

    $client->send_connection_preface;
    my $data = $client->mem_send;
    $client_sock->syswrite($data);
    $loop->loop_once(0.1);

    $client->mem_recv($server_settings);

    $loop->loop_once(0.1);
    my $ack = '';
    $client_sock->sysread($ack, 4096);
    $client->mem_recv($ack) if length($ack);

    my $client_ack = $client->mem_send;
    $client_sock->syswrite($client_ack) if length($client_ack);
    $loop->loop_once(0.1);

    my $extra = '';
    $client_sock->sysread($extra, 4096);
    $client->mem_recv($extra) if length($extra);
}

sub send_stream_data {
    my ($client, $client_sock, $stream_id, $data, $end_stream) = @_;
    $end_stream //= 0;
    $client->submit_data($stream_id, $data, $end_stream);
    my $out = $client->mem_send;
    $client_sock->syswrite($out) if length($out);
}

sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    $rounds //= 10;
    for (1..$rounds) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

sub open_ws_stream {
    my ($client, $client_sock, $path) = @_;
    $path //= '/ws';

    my $ws_stream_id = $client->submit_request(
        method    => 'CONNECT',
        path      => $path,
        scheme    => 'https',
        authority => 'localhost',
        headers   => [
            [':protocol', 'websocket'],
            ['sec-websocket-version', '13'],
        ],
        body      => sub { return undef },   # streaming: keep open
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock);

    return $ws_stream_id;
}

# Collected receive() events for the app under test; reset per subtest.
our @WS_EVENTS;

# Build an app that accepts, drains receive() into @WS_EVENTS until a
# websocket.disconnect arrives, then makes ONE bounded extra receive()
# call to catch a second queued event -- the queue property that proves
# "exactly one". A synthesized fallback 1006 surfacing here only happens
# if the connection was already torn down, which none of these subtests
# do before this check, so any second event caught here is a genuine
# duplicate-enqueue regression.
sub make_ws_app {
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';

        await $send->({ type => 'websocket.accept' });

        my $event = await $receive->();
        while ($event->{type} ne 'websocket.disconnect') {
            push @WS_EVENTS, $event;
            $event = await $receive->();
        }
        push @WS_EVENTS, $event;

        my $extra = await Future->wait_any($receive->(), $loop->delay_future(after => 0.3));
        push @WS_EVENTS, $extra if ref $extra eq 'HASH';
    };
}

sub disconnects_seen {
    return grep { $_->{type} eq 'websocket.disconnect' } @WS_EVENTS;
}

# ============================================================
# Test: per-stream WebSocket keepalive on h2 (design section 10.2)
# ============================================================
# The h2 WS send closure validates+advances 'websocket.keepalive' (the
# machine already permits it in 'accepted') but had no dispatch branch, so
# the event silently did nothing. These subtests drive the app's
# websocket.keepalive event end-to-end: ping frames delivered as h2 DATA,
# pong-answered survival, pong-withheld disconnect (1006/'keepalive_timeout',
# exactly once) with a sibling stream unaffected, per-stream isolation
# between two concurrent WS streams, and interval=>0 stopping the timer.
#
# Timing: every scenario below uses interval => 0.2s / timeout => 0.3s.
# exchange_frames($client, $sock, 1) advances one 0.1s round, so
# interval == 2 rounds and timeout == 3 rounds. Ceilings are stated in
# rounds with the computed margin against the expected round count, per
# the timing-measurement rule (an intermittent failure gets a measured
# margin, not a shrug).

# Track stream ids by reference so the on_data_chunk_recv callback (wired
# up BEFORE any stream exists) can dispatch by id once one is opened.
sub open_ws_stream_tracked {
    my ($client, $client_sock, $path, $id_ref) = @_;
    $path //= '/ws';

    # Assign the id synchronously (nghttp2 allocates it immediately) BEFORE
    # exchange_frames runs, so the data-capture callback can already route
    # by id for anything that arrives during the handshake round-trip.
    $$id_ref = $client->submit_request(
        method    => 'CONNECT',
        path      => $path,
        scheme    => 'https',
        authority => 'localhost',
        headers   => [
            [':protocol', 'websocket'],
            ['sec-websocket-version', '13'],
        ],
        body      => sub { return undef },   # streaming: keep open
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock);

    return $$id_ref;
}

sub open_get_stream_tracked {
    my ($client, $client_sock, $path, $id_ref) = @_;
    $path //= '/get';

    $$id_ref = $client->submit_request(
        method    => 'GET',
        path      => $path,
        scheme    => 'https',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock);

    return $$id_ref;
}

# Re-parses the FULL accumulated raw DATA buffer for a WS stream on every
# call (a fresh Protocol::WebSocket::Frame parser each time) and returns
# every complete frame seen so far as {opcode, bytes}. Idempotent and
# cumulative by design -- callers compare counts across checkpoints rather
# than tracking incremental parser state themselves.
sub extract_ws_frames {
    my ($raw) = @_;
    my @frames;
    my $parser = Protocol::WebSocket::Frame->new;
    $parser->append($raw);
    while (defined(my $bytes = $parser->next_bytes)) {
        push @frames, { opcode => $parser->opcode, bytes => $bytes };
    }
    return @frames;
}

sub ping_count {
    my ($raw) = @_;
    return scalar grep { $_->{opcode} == 9 } extract_ws_frames($raw);
}

sub pong_count {
    my ($raw) = @_;
    return scalar grep { $_->{opcode} == 10 } extract_ws_frames($raw);
}

# Every status code carried by a Close frame (opcode 8) seen on the wire so
# far. A Close frame with a 0-byte payload carries no code and contributes
# nothing.
sub close_codes {
    my ($raw) = @_;
    return map  { unpack('n', substr($_->{bytes}, 0, 2)) }
           grep { $_->{opcode} == 8 && length($_->{bytes}) >= 2 }
           extract_ws_frames($raw);
}

# App router used by all keepalive subtests below. A single app instance
# serves every stream on the test connection (the server takes one app per
# connection), so streams are routed to the caller's event array by path:
#
#   %path_events = ( '/ws' => \@events )                      -- one WS stream
#   %path_events = ( '/ws/a' => \@a, '/ws/b' => \@b )          -- two WS streams
#
# Keepalive is driven ENTIRELY by an in-band text control message,
# "keepalive:INTERVAL[,TIMEOUT]", rather than being auto-started at accept
# time. This is deliberate: open_ws_stream_tracked's handshake wait (10
# rounds / 1s, the same default the rest of this file uses) is far longer
# than the 0.2s/0.3s interval/timeout under test, so auto-starting keepalive
# before the handshake settles would let ping+timeout fire (and disconnect
# the stream) before a test's polling loop even begins. Deferring the start
# to an explicit control message lets each subtest establish a clean timing
# baseline itself, right after confirming the stream is open.
#
# A '/get' (or any non-websocket) path gets an immediate 200 "sibling-ok" --
# used to prove the h2 connection/session keeps serving other streams
# regardless of what happens to a WS stream's keepalive.
sub make_keepalive_router_app {
    my (%path_events) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;

        if ($scope->{type} eq 'http') {
            await $send->({
                type    => 'http.response.start',
                status  => 200,
                headers => [['content-type', 'text/plain']],
            });
            await $send->({ type => 'http.response.body', body => 'sibling-ok', more => 0 });
            return;
        }

        return unless $scope->{type} eq 'websocket';
        my $events = $path_events{$scope->{path}};
        return unless $events;

        await $send->({ type => 'websocket.accept' });

        while (1) {
            my $event = await $receive->();
            if ($event->{type} eq 'websocket.receive'
                && defined $event->{text}
                && $event->{text} =~ /^keepalive:([\d.]+)(?:,([\d.]+))?$/) {
                my %ka = (type => 'websocket.keepalive', interval => $1 + 0);
                $ka{timeout} = $2 + 0 if defined $2;
                await $send->(\%ka);
                next;
            }
            # "close:CODE,REASON" -- app-initiated closure, the send-closure
            # 'websocket.close' branch.
            if ($event->{type} eq 'websocket.receive'
                && defined $event->{text}
                && $event->{text} =~ /^close:(\d+),(.*)$/) {
                await $send->({
                    type   => 'websocket.close',
                    code   => $1 + 0,
                    reason => $2,
                });
                return;
            }
            # "echo:TEXT" -- bounce TEXT straight back, so a test can prove a
            # stream still exchanges messages after some other event.
            if ($event->{type} eq 'websocket.receive'
                && defined $event->{text}
                && $event->{text} =~ /^echo:(.*)$/) {
                await $send->({ type => 'websocket.send', text => $1 });
                next;
            }
            push @$events, $event;
            last if $event->{type} eq 'websocket.disconnect';
        }
    };
}

# Sends an in-band "keepalive:..." text control frame and runs a few settle
# rounds so the server has processed it (and started/stopped its timer)
# before the caller establishes a timing baseline.
# Pump exchange rounds for a WALL-CLOCK duration rather than a round count.
# Keepalive intervals are wall-clock, and an exchange round is only *at
# most* 0.1s (loop_once returns early when the socket already has data), so
# a cadence measurement has to be timed, not counted.
sub run_for_seconds {
    my ($client, $client_sock, $seconds) = @_;
    my $deadline = Time::HiRes::time() + $seconds;
    while (Time::HiRes::time() < $deadline) {
        exchange_frames($client, $client_sock, 1);
    }
}

# Pong-answer every ping seen so far on a stream's accumulated raw DATA that
# has not been answered yet. $answered is the running count of pings already
# answered; the new count is returned.
sub answer_new_pings {
    my ($client, $client_sock, $stream_id, $raw, $answered) = @_;
    my @pings = grep { $_->{opcode} == 9 } extract_ws_frames($raw);
    while ($answered < @pings) {
        my $ping = $pings[$answered++];
        my $pong = Protocol::WebSocket::Frame->new(
            type   => 'pong',
            buffer => $ping->{bytes},
            masked => 1,
        );
        send_stream_data($client, $client_sock, $stream_id, $pong->to_bytes);
    }
    return $answered;
}

sub send_ws_text {
    my ($client, $client_sock, $stream_id, $text) = @_;
    my $frame = Protocol::WebSocket::Frame->new(
        type   => 'text',
        buffer => $text,
        masked => 1,
    );
    send_stream_data($client, $client_sock, $stream_id, $frame->to_bytes);
}

sub start_keepalive_control {
    my ($client, $client_sock, $stream_id, $control_text) = @_;
    send_ws_text($client, $client_sock, $stream_id, $control_text);
    # 2 rounds (0.2s) is a full interval's worth of headroom for the server
    # to receive+dispatch the control event and (re)start the timer.
    exchange_frames($client, $client_sock, 2);
}

# ============================================================
# Ping observed as a ws ping frame in h2 DATA; answered pong survives >= 2
# intervals (no disconnect)
# ============================================================
subtest 'keepalive ping arrives as h2 DATA; answered pong survives >= 2 intervals' => sub {
    my @events;
    my $app = make_keepalive_router_app('/ws' => \@events);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my $ws_stream_id;
    my $ws_data = '';
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            $ws_data .= $data if defined $ws_stream_id && $sid == $ws_stream_id;
            return 0;
        },
    );

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);
    start_keepalive_control($client, $client_sock, $ws_stream_id, 'keepalive:0.2,0.3');

    # First ping is expected at ~2 rounds (interval 0.2s / 0.1s per round),
    # counted from the settled baseline start_keepalive_control establishes.
    # Ceiling: 50 rounds (5s) is a ~25x margin over the expected 2 rounds.
    my $seen_first_ping = 0;
    for (1 .. 50) {
        exchange_frames($client, $client_sock, 1);
        if (ping_count($ws_data) >= 1) { $seen_first_ping = 1; last; }
    }
    ok($seen_first_ping, 'a ws ping frame (opcode 9) arrived within the bounded window');

    # Answer every ping seen so far, then keep answering new ones as they
    # arrive for a further bounded window. 3 total pings observed proves the
    # connection survived >= 2 full intervals (0.4s) while the app kept
    # answering -- comfortably inside the 45-round (4.5s) ceiling, an ~11x
    # margin over the 4 rounds (0.4s) needed for 2 intervals.
    my $answered = 0;
    for (1 .. 45) {
        my @frames = extract_ws_frames($ws_data);
        my @pings  = grep { $_->{opcode} == 9 } @frames;
        while ($answered < @pings) {
            my $ping = $pings[$answered++];
            my $pong = Protocol::WebSocket::Frame->new(
                type   => 'pong',
                buffer => $ping->{bytes},
                masked => 1,
            );
            send_stream_data($client, $client_sock, $ws_stream_id, $pong->to_bytes);
        }
        last if $answered >= 3;
        exchange_frames($client, $client_sock, 1);
    }

    ok($answered >= 3, 'observed and answered >= 3 pings (>= 2 full intervals of liveness)')
        or diag("answered: $answered");
    is(scalar(grep { $_->{type} eq 'websocket.disconnect' } @events), 0,
        'no disconnect event while pongs were answered');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Pong withheld -> exactly one disconnect 1006/'keepalive_timeout'; a
# sibling plain-GET stream on the same connection still serves
# ============================================================
subtest 'withheld pong times out exactly one disconnect; sibling GET still serves' => sub {
    my @events;
    my $app = make_keepalive_router_app('/ws' => \@events);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my $ws_stream_id;
    my $ws_data = '';
    my $get_stream_id;
    my %get_headers;
    my $get_body = '';
    my $client = create_client(
        on_header => sub {
            my ($sid, $name, $value) = @_;
            $get_headers{$name} = $value if defined $get_stream_id && $sid == $get_stream_id;
            return 0;
        },
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            if (defined $ws_stream_id && $sid == $ws_stream_id) { $ws_data .= $data; }
            elsif (defined $get_stream_id && $sid == $get_stream_id) { $get_body .= $data; }
            return 0;
        },
    );

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);
    start_keepalive_control($client, $client_sock, $ws_stream_id, 'keepalive:0.2,0.3');

    # Never answer any ping. Disconnect is expected once ping (2 rounds) +
    # timeout (3 rounds) = 5 rounds have elapsed. Ceiling: 50 rounds (5s) is
    # a 10x margin over the expected 5 rounds (0.5s).
    my $saw_disconnect = 0;
    for (1 .. 50) {
        exchange_frames($client, $client_sock, 1);
        if (grep { $_->{type} eq 'websocket.disconnect' } @events) { $saw_disconnect = 1; last; }
    }
    ok($saw_disconnect, 'disconnect event delivered within the bounded window');

    my @disconnects = grep { $_->{type} eq 'websocket.disconnect' } @events;
    is(scalar @disconnects, 1, 'exactly one disconnect event');
    if (@disconnects) {
        is($disconnects[0]{code}, 1006, 'code is 1006 (abnormal closure)');
        is($disconnects[0]{reason}, 'keepalive_timeout', "reason is 'keepalive_timeout'");
    }

    # RFC 6455 section 7.4.1: 1006 MUST NOT appear as the status code of a
    # Close control frame -- it is reserved for an abnormal drop with no
    # close handshake, which is precisely what a keepalive timeout is. The
    # app-facing event above still reports 1006/'keepalive_timeout' (that is
    # the API contract); the WIRE must carry no Close frame saying 1006.
    # 10 settle rounds (1s) after the disconnect is a ~10x margin over the
    # single round it takes a flushed frame to reach the client.
    exchange_frames($client, $client_sock, 10);
    my @wire_close_codes = close_codes($ws_data);
    is(scalar(grep { $_ == 1006 } @wire_close_codes), 0,
        'no ws Close frame carrying code 1006 was written to the wire')
        or diag('close codes on the wire: ' . join(', ', @wire_close_codes));

    # Sibling: opened AFTER the ws stream's forced teardown, on the SAME h2
    # connection -- proves the connection (and other streams on it) are
    # unaffected by one stream's keepalive-timeout close.
    open_get_stream_tracked($client, $client_sock, '/get', \$get_stream_id);
    exchange_frames($client, $client_sock, 5);

    is($get_headers{':status'}, '200', 'sibling GET stream still gets a response');
    is($get_body, 'sibling-ok', 'sibling GET stream still gets its body');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Two concurrent WS streams, keepalive on stream A only -> stream B never
# receives pings
# ============================================================
subtest 'keepalive on one stream does not leak pings to a sibling WS stream' => sub {
    my @events_a;
    my @events_b;
    my $app = make_keepalive_router_app('/ws/a' => \@events_a, '/ws/b' => \@events_b);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my ($a_stream_id, $b_stream_id);
    my $a_data = '';
    my $b_data = '';
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            if (defined $a_stream_id && $sid == $a_stream_id) { $a_data .= $data; }
            elsif (defined $b_stream_id && $sid == $b_stream_id) { $b_data .= $data; }
            return 0;
        },
    );

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws/a', \$a_stream_id);
    open_ws_stream_tracked($client, $client_sock, '/ws/b', \$b_stream_id);
    # Interval only, no timeout: no dead-connection detection needed for A
    # to stay alive, so the test never has to answer A's pongs.
    start_keepalive_control($client, $client_sock, $a_stream_id, 'keepalive:0.2');

    # Window: 25 rounds (2.5s) covers >= 2 of A's 0.2s intervals (4 rounds /
    # 0.4s) with a ~6x margin.
    my $a_saw_ping = 0;
    for (1 .. 25) {
        exchange_frames($client, $client_sock, 1);
        if (ping_count($a_data) >= 1) { $a_saw_ping = 1; }
    }
    ok($a_saw_ping, 'stream A (keepalive enabled) received at least one ping');
    is(ping_count($b_data), 0, 'stream B (no keepalive) received zero ping frames');
    is(length($b_data), 0, 'stream B received no DATA at all (nothing else to send)');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# interval => 0 after starting stops further pings
# ============================================================
subtest 'interval => 0 stops further pings' => sub {
    my @events;
    my $app = make_keepalive_router_app('/ws' => \@events);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my $ws_stream_id;
    my $ws_data = '';
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            $ws_data .= $data if defined $ws_stream_id && $sid == $ws_stream_id;
            return 0;
        },
    );

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);
    start_keepalive_control($client, $client_sock, $ws_stream_id, 'keepalive:0.2');

    # First ping expected at ~2 rounds; 50-round (5s) ceiling is a ~25x margin.
    my $seen_first_ping = 0;
    for (1 .. 50) {
        exchange_frames($client, $client_sock, 1);
        if (ping_count($ws_data) >= 1) { $seen_first_ping = 1; last; }
    }
    ok($seen_first_ping, 'saw the first ping before disabling keepalive');

    # Disable keepalive; settle rounds let the server process the control
    # message and stop the timer before establishing the "no more pings"
    # baseline used below.
    start_keepalive_control($client, $client_sock, $ws_stream_id, 'keepalive:0');
    my $baseline = ping_count($ws_data);

    # Window: 30 rounds (3s) covers >= 2 more 0.2s intervals (4 rounds /
    # 0.4s) with a ~7x margin -- long enough that a still-running timer
    # would have fired again.
    for (1 .. 30) {
        exchange_frames($client, $client_sock, 1);
    }
    is(ping_count($ws_data), $baseline, 'no further pings arrived after interval => 0');

    # No-timeout mode: this subtest configured an interval with no timeout
    # and never answered a single ping. With no timeout there is no
    # dead-connection check at all, so the stream must survive untouched --
    # no disconnect, and in fact nothing beyond the scope's opening
    # websocket.connect (the only other traffic was the two keepalive
    # control messages, which the router consumes).
    is(scalar(grep { $_->{type} eq 'websocket.disconnect' } @events), 0,
        'no-timeout keepalive delivered zero disconnect events');
    is([map { $_->{type} } @events], ['websocket.connect'],
        'the only app-visible event was the opening websocket.connect');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# A second keepalive event supersedes the first (last wins)
# ============================================================
subtest 'a second keepalive event supersedes the first: the ping cadence changes' => sub {
    my @events;
    my $app = make_keepalive_router_app('/ws' => \@events);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my $ws_stream_id;
    my $ws_data = '';
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            $ws_data .= $data if defined $ws_stream_id && $sid == $ws_stream_id;
            return 0;
        },
    );

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);

    # Both settings use interval-only (no timeout), so an unanswered ping
    # can never tear the stream down mid-measurement and the two phases
    # differ in exactly one thing: the interval.
    start_keepalive_control($client, $client_sock, $ws_stream_id, 'keepalive:0.5');

    # Wait for the first slow ping so the measurement window starts on a
    # settled timer. Expected at ~0.5s; ceiling 6s is a 12x margin.
    my $deadline = Time::HiRes::time() + 6;
    exchange_frames($client, $client_sock, 1)
        while ping_count($ws_data) < 1 && Time::HiRes::time() < $deadline;
    ok(ping_count($ws_data) >= 1, 'saw the first ping at the initial 0.5s interval');

    # Measure both cadences over the SAME 1.5s wall-clock window. Measured
    # on this box: 2 pings at the 0.5s interval, 15 at the 0.1s one (the
    # fast count is capped by the ~0.1s exchange-round poll resolution).
    my $before_slow = ping_count($ws_data);
    run_for_seconds($client, $client_sock, 1.5);
    my $slow_pings = ping_count($ws_data) - $before_slow;

    start_keepalive_control($client, $client_sock, $ws_stream_id, 'keepalive:0.1');
    my $before_fast = ping_count($ws_data);
    run_for_seconds($client, $client_sock, 1.5);
    my $fast_pings = ping_count($ws_data) - $before_fast;

    # A superseded timer would leave the cadence unchanged ($fast == $slow).
    # Threshold 2x the slow count: with the measured 2 vs 15 that puts the
    # bar at 4, a 3.75x margin below the observed fast count.
    note("cadence over 1.5s: 0.5s interval -> $slow_pings pings; "
       . "0.1s interval -> $fast_pings pings");
    ok($fast_pings > 2 * $slow_pings,
        'the second keepalive event replaced the cadence (last wins)')
        or diag("slow window: $slow_pings pings; fast window: $fast_pings pings");

    is(scalar(grep { $_->{type} eq 'websocket.disconnect' } @events), 0,
        'no disconnect while re-arming keepalive (old pong-timer state did not wedge)');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Inbound ws PING (opcode 9) is answered with a PONG (opcode 10)
# ============================================================
subtest 'an inbound ws ping frame is answered with a pong carrying its payload' => sub {
    my @events;
    my $app = make_keepalive_router_app('/ws' => \@events);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my $ws_stream_id;
    my $ws_data = '';
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            $ws_data .= $data if defined $ws_stream_id && $sid == $ws_stream_id;
            return 0;
        },
    );

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);

    # No keepalive is armed on this stream, so the server sends no pings of
    # its own: any opcode-10 frame on the wire can only be the answer to
    # this client ping.
    my $ping = Protocol::WebSocket::Frame->new(
        type   => 'ping',
        buffer => 'probe',
        masked => 1,
    );
    send_stream_data($client, $client_sock, $ws_stream_id, $ping->to_bytes);
    # 10 rounds (up to 1s) is a ~10x margin over the single round a
    # feed()-processed answer needs to reach the client.
    exchange_frames($client, $client_sock, 10);

    my @pongs = grep { $_->{opcode} == 10 } extract_ws_frames($ws_data);
    is(scalar @pongs, 1, 'server answered the inbound ping with exactly one pong');
    is($pongs[0]{bytes}, 'probe', 'the pong echoes the ping payload')
        if @pongs;
    is(ping_count($ws_data), 0, 'server sent no pings of its own (no keepalive armed)');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Two keepalive-armed WS streams: the silent one times out, the ponging one
# survives and keeps exchanging messages
# ============================================================
subtest 'a keepalive timeout on one armed stream leaves its ponging sibling working' => sub {
    my @events_a;
    my @events_b;
    my $app = make_keepalive_router_app('/ws/a' => \@events_a, '/ws/b' => \@events_b);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my ($a_stream_id, $b_stream_id);
    my $a_data = '';
    my $b_data = '';
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            if (defined $a_stream_id && $sid == $a_stream_id) { $a_data .= $data; }
            elsif (defined $b_stream_id && $sid == $b_stream_id) { $b_data .= $data; }
            return 0;
        },
    );

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws/a', \$a_stream_id);
    open_ws_stream_tracked($client, $client_sock, '/ws/b', \$b_stream_id);

    # BOTH streams arm the same interval/timeout -- the only difference is
    # that B's pings get answered and A's never do. B is armed first so its
    # pong-answering loop is already the thing under way when A's timeout
    # (interval 0.2 + timeout 0.3 = ~0.5s) expires.
    start_keepalive_control($client, $client_sock, $b_stream_id, 'keepalive:0.2,0.3');
    start_keepalive_control($client, $client_sock, $a_stream_id, 'keepalive:0.2,0.3');

    # Ceiling 60 rounds (up to 6s) is a >= 12x margin over A's expected
    # ~0.5s teardown.
    my $b_answered = 0;
    my $saw_a_disconnect = 0;
    for (1 .. 60) {
        $b_answered = answer_new_pings($client, $client_sock, $b_stream_id,
                                       $b_data, $b_answered);
        if (grep { $_->{type} eq 'websocket.disconnect' } @events_a) {
            $saw_a_disconnect = 1;
            last;
        }
        exchange_frames($client, $client_sock, 1);
    }
    ok($saw_a_disconnect, 'stream A (pong withheld) disconnected within the bounded window');

    my @a_disconnects = grep { $_->{type} eq 'websocket.disconnect' } @events_a;
    is(scalar @a_disconnects, 1, 'stream A got exactly one disconnect');
    if (@a_disconnects) {
        is($a_disconnects[0]{code}, 1006, 'stream A code is 1006');
        is($a_disconnects[0]{reason}, 'keepalive_timeout', "stream A reason is 'keepalive_timeout'");
    }
    is(scalar(grep { $_->{type} eq 'websocket.disconnect' } @events_b), 0,
        'stream B (ponging) was not disconnected by its sibling timing out');
    ok($b_answered >= 1, 'stream B answered at least one of its own keepalive pings')
        or diag("b_answered: $b_answered");

    # B still exchanges application messages AFTER A's teardown: round-trip
    # a text frame through the router's echo control, answering B's pings
    # throughout so its own keepalive stays satisfied.
    send_ws_text($client, $client_sock, $b_stream_id, 'echo:still-here');
    my $echoed = 0;
    for (1 .. 30) {
        $b_answered = answer_new_pings($client, $client_sock, $b_stream_id,
                                       $b_data, $b_answered);
        exchange_frames($client, $client_sock, 1);
        if (grep { $_->{opcode} == 1 && $_->{bytes} eq 'still-here' }
                 extract_ws_frames($b_data)) {
            $echoed = 1;
            last;
        }
    }
    ok($echoed, 'stream B round-tripped a text message after A timed out');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Every WebSocket closure path releases the stream's keepalive timer
# ============================================================
# A closure that leaves the periodic ping timer armed leaves it ticking on
# a stream that no longer exists, pinging into a half-closed stream for the
# life of the connection. Two paths used to do exactly that: the app's own
# 'websocket.close' send event, and the server-initiated protocol close for
# an invalid-UTF-8 TEXT frame.
#
# These are white-box on purpose: the leak has no observable wire symptom
# other than stray pings, and asserting on the timer itself is what pins
# the release down. Keepalive is armed with an interval and NO timeout, so
# nothing ELSE can tear the stream down and clean the timer up incidentally
# -- the release under test is the only thing that can clear it.
#
# The stream-state hashref is captured before the close and asserted on
# afterwards, because h2_streams drops its entry once the stream is
# reclaimed and the question is what happened to that stream's timer.

subtest 'app-initiated websocket.close releases the keepalive timer' => sub {
    my @events;
    my $app = make_keepalive_router_app('/ws' => \@events);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my $ws_stream_id;
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);
    start_keepalive_control($client, $client_sock, $ws_stream_id, 'keepalive:0.2');

    my $ss = $conn->{h2_streams}{$ws_stream_id};
    ok($ss, 'server holds stream state for the accepted ws stream');
    my $timer = $ss && $ss->{ws_ka_timer};
    ok($timer && $timer->is_running, 'keepalive timer armed before the close');

    # App closes; the client deliberately never ends its own side, so the
    # stream stays half-closed(local) and on_stream_close cannot fire and
    # sweep the timer for us.
    send_ws_text($client, $client_sock, $ws_stream_id, 'close:1000,bye');
    # 10 rounds (1s) is a 5x margin over the 0.2s interval -- a still-armed
    # timer would have ticked several times by here.
    exchange_frames($client, $client_sock, 10);

    ok(!$ss->{ws_ka_timer}, 'stream keepalive timer released after websocket.close');
    ok($timer && !$timer->is_running, 'the armed timer itself is stopped');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest 'invalid-UTF-8 protocol close releases the keepalive timer' => sub {
    my @events;
    my $app = make_keepalive_router_app('/ws' => \@events);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my $ws_stream_id;
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);
    start_keepalive_control($client, $client_sock, $ws_stream_id, 'keepalive:0.2');

    my $ss = $conn->{h2_streams}{$ws_stream_id};
    ok($ss, 'server holds stream state for the accepted ws stream');
    my $timer = $ss && $ss->{ws_ka_timer};
    ok($timer && $timer->is_running, 'keepalive timer armed before the close');

    # Invalid UTF-8 in a TEXT frame: the server sends Close(1007) itself.
    my $bad = Protocol::WebSocket::Frame->new(
        type   => 'text',
        buffer => "\xFF\xFE",
        masked => 1,
    );
    send_stream_data($client, $client_sock, $ws_stream_id, $bad->to_bytes);
    exchange_frames($client, $client_sock, 10);

    my @disconnects = grep { $_->{type} eq 'websocket.disconnect' } @events;
    is(scalar @disconnects, 1, 'exactly one disconnect event');
    is($disconnects[0]{code}, 1007, 'code is 1007 (invalid payload data)')
        if @disconnects;

    ok(!$ss->{ws_ka_timer}, 'stream keepalive timer released after the 1007 close');
    ok($timer && !$timer->is_running, 'the armed timer itself is stopped');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Peer Close(4321, "bye") -> exactly one disconnect, 4321/"bye"
# ============================================================
subtest 'peer Close(4321, "bye") delivers exactly one disconnect with the peer code/reason' => sub {
    @WS_EVENTS = ();
    my $app = make_ws_app();
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id = open_ws_stream($client, $client_sock);

    my $close_frame = Protocol::WebSocket::Frame->new(
        type   => 'close',
        buffer => pack('n', 4321) . 'bye',
        masked => 1,
    );
    send_stream_data($client, $client_sock, $ws_stream_id, $close_frame->to_bytes, 1);
    exchange_frames($client, $client_sock, 20);

    my @disconnects = disconnects_seen();
    is(scalar @disconnects, 1, 'exactly one websocket.disconnect event');
    if (@disconnects) {
        is($disconnects[0]{code}, 4321, 'code is the peer\'s code');
        is($disconnects[0]{reason}, 'bye', 'reason is the peer\'s reason text');
    }

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Peer Close with empty payload -> exactly one disconnect, code 1005
# ============================================================
subtest 'peer Close with empty payload delivers exactly one disconnect, code 1005' => sub {
    @WS_EVENTS = ();
    my $app = make_ws_app();
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id = open_ws_stream($client, $client_sock);

    my $close_frame = Protocol::WebSocket::Frame->new(
        type   => 'close',
        buffer => '',
        masked => 1,
    );
    send_stream_data($client, $client_sock, $ws_stream_id, $close_frame->to_bytes, 1);
    exchange_frames($client, $client_sock, 20);

    my @disconnects = disconnects_seen();
    is(scalar @disconnects, 1, 'exactly one websocket.disconnect event');
    if (@disconnects) {
        is($disconnects[0]{code}, 1005, 'code is 1005 (no status received)');
        is($disconnects[0]{reason}, '', 'reason is empty');
    }

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Bare END_STREAM (no close frame) -> exactly one disconnect, 1006/client_closed
# ============================================================
subtest 'bare END_STREAM delivers exactly one disconnect, 1006/client_closed' => sub {
    @WS_EVENTS = ();
    my $app = make_ws_app();
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id = open_ws_stream($client, $client_sock);

    # No close frame at all -- just end the client's side of the stream.
    send_stream_data($client, $client_sock, $ws_stream_id, '', 1);
    exchange_frames($client, $client_sock, 20);

    my @disconnects = disconnects_seen();
    is(scalar @disconnects, 1, 'exactly one websocket.disconnect event')
        or diag("saw: " . join(', ', map { "$_->{code}/'$_->{reason}'" } @disconnects));
    if (@disconnects) {
        is($disconnects[0]{code}, 1006, 'code is 1006 (abnormal closure)');
        is($disconnects[0]{reason}, 'client_closed', "reason is 'client_closed'");
    }

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# RST_STREAM -> exactly one disconnect, 1006/client_closed
# ============================================================
subtest 'peer RST_STREAM delivers exactly one disconnect, 1006/client_closed' => sub {
    @WS_EVENTS = ();
    my $app = make_ws_app();
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id = open_ws_stream($client, $client_sock);

    $client->submit_rst_stream($ws_stream_id, H2_CANCEL_CODE);
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    my @disconnects = disconnects_seen();
    is(scalar @disconnects, 1, 'exactly one websocket.disconnect event');
    if (@disconnects) {
        is($disconnects[0]{code}, 1006, 'code is 1006 (abnormal closure)');
        is($disconnects[0]{reason}, 'client_closed', "reason is 'client_closed'");
    }

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# receive() fallback prefers a recorded server_close_reason over ''
# (design section 6.1's transport-neutral disconnect reasons)
# ============================================================
# White-box: exercises _h2_create_websocket_receive's OWN fallback branch
# directly -- the code path a receive() call takes when it resolves after
# $weak_self->{closed} is already true (e.g. a whole-connection teardown,
# such as a server shutdown, racing an already-pending receive()), rather
# than through the primary enqueue+dedup path the subtests above already
# cover. The fallback must report whatever server_close_reason a
# server-initiated per-stream teardown recorded, as long as the stream's
# own state (its h2_streams entry) is still reachable, defaulting to ''
# when it is not.
subtest 'receive() fallback after connection close reports the recorded server_close_reason, not empty' => sub {
    my $conn = PAGI::Server::Connection->new(
        app      => sub { },
        protocol => $protocol,
    );
    $conn->{h2_streams}{7} = { server_close_reason => 'keepalive_timeout' };
    $conn->{closed} = 1;

    my $receive = $conn->_h2_create_websocket_receive(7, $conn->{h2_streams}{7});
    my $event = $receive->()->get;

    is($event->{type}, 'websocket.disconnect', 'fallback event type is websocket.disconnect');
    is($event->{code}, 1006, 'fallback code is still 1006 (abnormal closure)');
    is($event->{reason}, 'keepalive_timeout',
        "fallback reason is the recorded token, not ''");
};

subtest 'receive() fallback with no reachable stream state still falls back to empty reason' => sub {
    my $conn = PAGI::Server::Connection->new(
        app      => sub { },
        protocol => $protocol,
    );
    $conn->{closed} = 1;
    # Deliberately no h2_streams entry for stream 7 -- its state is gone.

    my $receive = $conn->_h2_create_websocket_receive(7, undef);
    my $event = $receive->()->get;

    is($event->{code}, 1006, 'fallback code is still 1006');
    is($event->{reason}, '', "fallback reason is '' when no stream state is reachable");
};

# ============================================================
# max_body_size 413 overrun on a PRE-ACCEPT h2 WebSocket stream (Www.pod
# "Disconnect - receive event" field definitions: code 1478-1480, reason
# 1482 -- a server-detected abnormal close reports 1006 plus the matching
# standard token, and 'body_too_large' is one of the Standard Disconnect
# Reasons).
# ============================================================
# _h2_on_body's max_body_size overrun branch delivers a scope-appropriate
# disconnect for the sse and http arms, but excluded pre-accept websocket
# streams: an accepted ws stream returns early at the top of _h2_on_body
# (the `$stream->{is_websocket} && $stream->{ws_accepted}` guard) before
# ever reaching this size check, so `is_websocket` true at the overrun
# branch always means pre-accept. Before this fix, that pre-accept case got
# its 413 and stream-state deletion but no receive_queue event, so an app
# parked on receive() -- having seen websocket.connect but not yet called
# websocket.accept -- hung until connection teardown.
# ============================================================
subtest 'max_body_size 413 overrun on a pre-accept ws stream wakes pending receive() with websocket.disconnect' => sub {
    my ($receive_resolved, $connect_event, $disconnect_event);
    my @app_warnings;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';

        # Deliberately do NOT send websocket.accept -- the app awaits
        # receive() twice (connect, then a second call that parks) without
        # ever accepting, reproducing the pre-accept overrun scenario.
        $connect_event = await $receive->();
        $disconnect_event = await $receive->();
        $receive_resolved = 1;
    };

    my $server = create_test_server(app => $app, max_body_size => 40);
    my ($conn, $stream_io, $client_sock) = create_h2_connection(app => $app, server => $server);

    my %response_headers;
    my $client = create_client(
        on_header => sub {
            my ($sid, $name, $value) = @_;
            $response_headers{$name} = $value;
            return 0;
        },
    );

    complete_h2_handshake($client, $client_sock);
    local $SIG{__WARN__} = sub { push @app_warnings, $_[0] };

    my $ws_stream_id = open_ws_stream($client, $client_sock);

    # Poll (bounded) until the app has consumed websocket.connect and is
    # parked on its second, pre-accept receive() (body_pending armed but not
    # yet resolved), so the overrun below is guaranteed to land on a
    # PENDING receive, not one the app hasn't reached yet.
    my $dispatched = 0;
    for (1..20) {
        exchange_frames($client, $client_sock, 1);
        my $ss = $conn->{h2_streams}{$ws_stream_id};
        if ($connect_event && $ss && $ss->{body_pending} && !$ss->{body_pending}->is_ready) {
            $dispatched = 1;
            last;
        }
    }
    ok($dispatched, 'app consumed websocket.connect and parked on a second, pre-accept receive()')
        or diag('app never reached the parked-receive state -- cannot exercise wake-on-overrun');
    ok(!$conn->{h2_streams}{$ws_stream_id}{ws_accepted}, 'stream is still pre-accept (ws_accepted false)');

    # Push body data past max_body_size (40 bytes) as raw h2 DATA -- for a
    # pre-accept ws stream this is accumulated as an ordinary request body
    # (the ws-frame-parsing branch in _h2_on_body only runs once ws_accepted
    # is true), so it drives the SAME max_body_size overrun branch the
    # sse/http arms use.
    send_stream_data($client, $client_sock, $ws_stream_id, ('X' x 100));

    # Bounded wait (never an unbounded hang) for the parked receive() to
    # resolve. TODAY (RED): this never becomes true and the loop exhausts.
    my $settled = 0;
    for (1..20) {
        exchange_frames($client, $client_sock, 1);
        if ($receive_resolved) {
            $settled = 1;
            last;
        }
    }
    ok($settled, 'pending pre-accept receive() resolved within the bounded wait')
        or diag('receive_resolved=' . ($receive_resolved // 0));

    is($connect_event->{type}, 'websocket.connect', 'first receive() was the opening websocket.connect');
    is($disconnect_event->{type}, 'websocket.disconnect', 'second receive() resolved to websocket.disconnect');
    if ($disconnect_event && ($disconnect_event->{type} // '') eq 'websocket.disconnect') {
        is($disconnect_event->{code}, 1006, 'code is 1006 (abnormal closure)');
        is($disconnect_event->{reason}, 'body_too_large', "reason is 'body_too_large'");
    }
    ok(!exists $conn->{h2_streams}{$ws_stream_id},
        'server-side stream state reclaimed after the overrun (no leaked h2_streams entry)');
    ok(!(grep { /returned without starting a response/ } @app_warnings),
        'dispatch wrapper did not synthesize a spurious 500 warning for this client-gone (413) stream')
        or diag(join('', @app_warnings));

    # 413 observed client-side if still observable pre-RST: give a few more
    # settle rounds for the response headers nghttp2 already submitted to
    # reach the client before asserting on them.
    exchange_frames($client, $client_sock, 10);
    is($response_headers{':status'}, '413', 'client still sees the 413');

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
