use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.010+ required)');
}

# ============================================================
# Test: a receive() made after the stream ended is answered, not parked
# ============================================================
# L<PAGI::Spec::Www> "Disconnect - receive event" (websocket) and "SSE
# Disconnect - receive event": "Once this event has been delivered the scope
# is over, and a further receive() resolves with the same event again." For
# http the disconnect event is "sent to the application if receive is called
# after a response has been sent or after the HTTP connection has been
# closed". L<PAGI::Spec> "Cancellation and Disconnects" adds that every
# receive Future pending when the disconnect is detected must be resolved
# with the disconnect event.
#
# On HTTP/2 the stream's state lives in h2_streams, and the entry is dropped
# one turn of the event loop after the stream closes so pending futures can
# resolve first. A receive() made in that window -- which is exactly where an
# application that calls receive() again from the turn its disconnect event
# arrived in lands -- must be answered from the scope's ending, not parked on
# a Future the drop then orphans.
#
# Each case makes three receives: one parked across the scope's ending, one
# made in the same turn as that delivery with no yield in between, and one
# made after a yielded tick (by which time the h2_streams entry is gone).
# The h1 twins of these paths are covered by t/61 and t/70.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use Protocol::WebSocket::Frame;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# Every server built here logs into this collector rather than STDERR, so a
# case can assert the exact set of lines its scope produced.
my @LOG;

# A receive() that is answered from an already-resolved Future never returns
# control to the event loop, so an alarm is the only bound a pump can impose
# on an application that spins. Every pump carries it; no case here expects
# it to fire.
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

sub reset_case {
    @LOG         = ();
    $ALARM_FIRED = 0;
}

# Every log line the case's scope produced, counted by level. Asserted at
# every level, not just error: these servers log into a collector, so the
# suite's stderr diff never sees what they write.
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
# HTTP/2 harness (shape borrowed from t/71 and t/75)
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
        sse_idle_timeout        => $server->{sse_idle_timeout},
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
    return bounded(sub {
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
    });
}

sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    return bounded(sub {
        for (1 .. ($rounds // 20)) {
            $loop->loop_once(0.05);
            next unless defined fileno($client_sock);   # the client may have left
            my $buf = '';
            $client_sock->sysread($buf, 16384);
            $client->mem_recv($buf) if length($buf);
            my $out = $client->mem_send;
            $client_sock->syswrite($out) if length($out);
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

sub send_stream_data {
    my ($client, $client_sock, $stream_id, $data, $end_stream) = @_;
    $client->submit_data($stream_id, $data, $end_stream // 0);
    my $out = $client->mem_send;
    $client_sock->syswrite($out) if length($out);
}

# ============================================================
# Applications
# ============================================================

my @KINDS = qw(websocket sse http);

# The one event each scope's ending delivers, whether it comes from the
# scope's queue or is synthesized for a receive that arrives after it. Both
# spellings must agree (Www.pod "Agreement with disconnect events"): a bare
# transport drop is 1006/client_closed on websocket.
my %EXPECTED = (
    websocket => { type => 'websocket.disconnect', code => 1006, reason => 'client_closed' },
    sse       => { type => 'sse.disconnect', reason => 'client_closed' },
    http      => { type => 'http.disconnect' },
);

# Bring the scope to the point where its next receive() can only be answered
# by the scope's ending, leaving the http response open: a receive on a
# completed http response is answered at once, and these cases need the first
# receive genuinely parked when the client leaves.
my $open_scope = async sub {
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
                    headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
    return;
};

# One receive parked across the scope's ending, a second made in the same
# turn as that delivery, a third after a yielded tick. Results land in $out.
sub three_receives {
    my ($kind, $out) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        await $open_scope->($kind, $receive, $send);

        # Recorded one at a time, so a receive that never resolves leaves the
        # ones before it in the record and the failure names which call hung.
        my $parked = await $receive->();          # pending when the scope ends
        push @{ $out->{events} }, $parked;
        my $same = await $receive->();            # same turn, no yield
        push @{ $out->{events} }, $same;
        await $loop->delay_future(after => 0);    # one turn of the event loop
        my $later = await $receive->();
        push @{ $out->{events} }, $later;

        $out->{reason}   = $cs->disconnect_reason;
        $out->{returned} = 1;
        return;
    };
}

# ============================================================
# The client resets the stream
# ============================================================

for my $kind (@KINDS) {
    subtest "h2 $kind: a receive made in the same turn as the RST_STREAM delivery is answered" => sub {
        reset_case();
        my %out = (events => [], returned => 0, reason => undef);
        my ($conn, $stream_io, $client_sock, $server) =
            create_h2_connection(app => three_receives($kind, \%out));
        my $mark   = scalar @LOG;
        my $client = create_client();
        complete_h2_handshake($client, $client_sock);
        my $sid = h2_submit($client, $client_sock, $kind);
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 10);

        $client->submit_rst_stream($sid, 8);          # CANCEL
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 20);

        ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
        is($out{events}, [ ($EXPECTED{$kind}) x 3 ],
            'all three receives resolved with the scope\'s disconnect event');
        is($out{returned}, 1, 'the application returned');
        is($out{reason}, 'client_closed', 'the object reports the same ending');
        is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

        eval { $stream_io->close_now };
        shutdown_server($server);
    };
}

# ============================================================
# An http scope that ended cleanly
# ============================================================

subtest 'h2 http: both receives after a complete response are answered' => sub {
    reset_case();
    my @events;
    my $returned = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();                       # http.request
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type', 'text/plain'],
                                    ['content-length', 2]] });
        await $send->({ type => 'http.response.body', body => 'ok' });

        # The response is complete and the stream ends with it. Both of
        # these are made in the same turn of the event loop.
        push @events, await $receive->();
        push @events, await $receive->();
        $returned = 1;
        return;
    };
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $mark   = scalar @LOG;
    my $client = create_client();
    complete_h2_handshake($client, $client_sock);
    my $sid = h2_submit($client, $client_sock, 'http');
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    is(\@events, [ ({ type => 'http.disconnect' }) x 2 ],
        'both receives after the response resolved with http.disconnect');
    is($returned, 1, 'the application returned');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    eval { $stream_io->close_now };
    shutdown_server($server);
};

# ============================================================
# The client closes the connection
# ============================================================

for my $kind (@KINDS) {
    subtest "h2 $kind: a receive made in the same turn as the connection-close delivery is answered" => sub {
        reset_case();
        my %out = (events => [], returned => 0, reason => undef);
        my ($conn, $stream_io, $client_sock, $server) =
            create_h2_connection(app => three_receives($kind, \%out));
        my $mark   = scalar @LOG;
        my $client = create_client();
        complete_h2_handshake($client, $client_sock);
        my $sid = h2_submit($client, $client_sock, $kind);
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 10);

        $client_sock->close;                          # the connection goes
        exchange_frames($client, $client_sock, 20);

        ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
        is($out{events}, [ ($EXPECTED{$kind}) x 3 ],
            'all three receives resolved with the scope\'s disconnect event');
        is($out{returned}, 1, 'the application returned');
        is($out{reason}, 'client_closed', 'the object reports the same ending');
        is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

        eval { $stream_io->close_now };
        shutdown_server($server);
    };
}

# ============================================================
# The peer's own close code and reason outlive the stream entry
# ============================================================
# Www.pod "Agreement with disconnect events": a peer's Close frame names its
# own RFC code and its own reason TEXT, and the event carries them. The
# ending record cannot stand in for that -- its vocabulary is the standard
# reason tokens -- so the scope's delivered event is kept and re-delivered
# verbatim. The h2_streams entry is dropped a turn after the stream closes,
# so this case reads the event two turns later, when the receive closure's
# own captured stream state is the only place left holding it.

subtest 'h2 websocket: a further receive reports the peer close code after the stream entry is dropped' => sub {
    reset_case();
    my @events;
    my ($returned, $entry_gone) = (0, 0);
    my ($conn, $sid);

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();                       # websocket.connect
        await $send->({ type => 'websocket.accept' });

        my $event = await $receive->();
        $event = await $receive->() while $event->{type} ne 'websocket.disconnect';
        push @events, $event;

        await $loop->delay_future(after => 0);    # the close's deferred delete
        await $loop->delay_future(after => 0);    # and a turn past it
        $entry_gone = !exists $conn->{h2_streams}{$sid};

        push @events, await $receive->();
        $returned = 1;
        return;
    };

    my ($stream_io, $client_sock, $server);
    ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $mark   = scalar @LOG;
    my $client = create_client();
    complete_h2_handshake($client, $client_sock);
    $sid = h2_submit($client, $client_sock, 'websocket');
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 10);

    my $close = Protocol::WebSocket::Frame->new(
        type => 'close', buffer => pack('n', 4321) . 'bye', masked => 1);
    send_stream_data($client, $client_sock, $sid, $close->to_bytes, 1);
    exchange_frames($client, $client_sock, 20);

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    ok($entry_gone, 'the stream entry was already dropped when the further receive was made');
    is(\@events,
        [ ({ type => 'websocket.disconnect', code => 4321, reason => 'bye' }) x 2 ],
        'the further receive carries the peer\'s own code and reason text');
    is($returned, 1, 'the application returned');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    eval { $stream_io->close_now };
    shutdown_server($server);
};

# ============================================================
# An SSE scope that ends while its own stream stays open
# ============================================================
# Www.pod "SSE Disconnect - receive event": "Once this event has been
# delivered the scope is over, and a further receive() resolves with the same
# sse.disconnect again." The scope's end and the stream's end are not the same
# moment on HTTP/2. A server-decided end -- the idle timeout here -- delivers
# the event and only marks the stream ending; the final END_STREAM is left to
# the data callback, which emits it once the stream's send queue has drained.
# So the further receive below is made with the h2 stream still open, which is
# the shape the disconnect-wait loop's sse_disconnect_delivered clause answers.

subtest 'h2 sse: a further receive after the idle timeout is answered while the stream is open' => sub {
    reset_case();
    my @events;
    my $returned = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();                       # sse.request
        await $send->({ type => 'sse.start' });

        push @events, await $receive->();         # parked until the timeout
        push @events, await $receive->();         # same turn, no yield
        $returned = 1;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(
        app => $app, server_opts => { sse_idle_timeout => 0.3 });
    my $mark   = scalar @LOG;
    my $client = create_client();
    complete_h2_handshake($client, $client_sock);
    my $sid = h2_submit($client, $client_sock, 'sse');
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 60);   # long enough for the 0.3s timeout

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    is(\@events, [ ({ type => 'sse.disconnect', reason => 'idle_timeout' }) x 2 ],
        'both receives resolved with the same idle-timeout disconnect');
    is($returned, 1, 'the application returned');
    is(log_levels_since($mark), { warn => 1 }, 'the scope logged one warn line');
    like([ map { $_->{message} } @LOG[$mark .. $#LOG] ],
        [ qr{^HTTP/2 SSE stream $sid idle timeout \(0\.3s\) - closing stream$} ],
        'that line is the idle timeout on this stream');

    eval { $stream_io->close_now };
    shutdown_server($server);
};

done_testing;
