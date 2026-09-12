use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use Scalar::Util ();

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
        on_frame_recv      => $o{on_frame_recv}      // sub { 0 },
        on_data_chunk_recv => $o{on_data_chunk_recv} // sub { 0 },
        on_stream_close    => $o{on_stream_close}    // sub { 0 },
    });
}

sub complete_h2_handshake {
    my ($client, $client_sock, %settings) = @_;
    return bounded(sub {
        $loop->loop_once(0.1);
        my $settings = '';
        $client_sock->sysread($settings, 4096);
        $client->send_connection_preface(%settings);
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

# ============================================================
# The terminal response event ends the http scope, request body read or not
# ============================================================
# L<PAGI::Spec::Www> "Meaning per scope": the http scope's clean end is its
# terminal body or trailers, and response_complete is true "once the response
# body completed" -- not once the request finished arriving. "Receiving after
# the scope's end" then reports that end to every receive, pending or later.
# The client here POSTs and never finishes its request half, which is the
# shape a 401 or a 413 answers without reading anything.
#
# RFC 9113 section 8.1 names the wire form: a server that has sent a complete
# response "MAY request that the client abort transmission of a request
# without error by sending a RST_STREAM with an error code of NO_ERROR".
# Measured before this was built (task-B14-report.md, step 0): nghttp2 sends
# no such reset of its own, and without one the finished stream stays open
# for as long as the client's request half does.

# RFC 9113 section 6.4. The binding exports no frame-type constant for it.
use constant RST_STREAM_FRAME => 0x3;

# The frames this stream carried, in order, named the way the RFC names them.
# Only the three that decide the shape of an ending: everything else on the
# connection (SETTINGS, WINDOW_UPDATE) is noise here.
sub wire_for {
    my ($frames, $sid) = @_;
    my @out;
    for my $f (@$frames) {
        next unless ($f->{stream_id} // 0) == $sid;
        my $end = ($f->{flags} & Net::HTTP2::nghttp2::NGHTTP2_FLAG_END_STREAM()) ? '+END_STREAM' : '';
        push @out, 'RST_STREAM'          if $f->{type} == RST_STREAM_FRAME;
        push @out, "DATA$end"            if $f->{type} == Net::HTTP2::nghttp2::NGHTTP2_DATA();
        push @out, "HEADERS$end"         if $f->{type} == Net::HTTP2::nghttp2::NGHTTP2_HEADERS();
    }
    return \@out;
}

# A client whose request half stays open: the body provider defers forever,
# so nghttp2 sends HEADERS without END_STREAM and nothing after it.
sub submit_open_request {
    my ($client, $client_sock, $method) = @_;
    my $sid = $client->submit_request(
        method => $method, path => '/r', scheme => 'http', authority => 'localhost',
        headers => [['content-type', 'text/plain']],
        body    => sub { return undef },
    );
    $client_sock->syswrite($client->mem_send);
    return $sid;
}

# The object readings the spec pins at a clean end, in one hash so a failure
# names every one of them at once.
sub readings_of {
    my ($cs) = @_;
    return {
        is_connected      => $cs->is_connected,
        response_started  => $cs->response_started,
        response_complete => $cs->response_complete,
        disconnect_reason => $cs->disconnect_reason,
    };
}

# status => the terminal send that ends the response. Each case answers
# without ever calling receive() for the body.
my %TERMINAL = (
    'inline body' => [
        { type => 'http.response.start', status => 401,
          headers => [['content-type', 'text/plain'], ['content-length', 4]] },
        { type => 'http.response.body', body => 'nope' },
    ],
    'trailers' => [
        { type => 'http.response.start', status => 401, trailers => 1,
          headers => [['content-type', 'text/plain'], ['trailer', 'x-why']] },
        { type => 'http.response.body', body => 'nope' },
        { type => 'http.response.trailers', headers => [['x-why', 'unauthorized']] },
    ],
);

for my $shape (sort keys %TERMINAL) {
    subtest "h2 http: a $shape response ends the scope with the request body still open" => sub {
        reset_case();
        my (@events, @completes, @disconnects);
        my ($returned, $readings) = (0, undef);
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            my $cs = $scope->{'pagi.connection'};
            $cs->on_complete(sub { push @completes, 1 });
            $cs->on_disconnect(sub { push @disconnects, 1 });
            for my $event (@{ $TERMINAL{$shape} }) { await $send->($event) }
            push @events, await $receive->();
            $readings = readings_of($cs);
            $returned = 1;
            return;
        };

        my (@frames, @closes);
        my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
        my $mark   = scalar @LOG;
        my $client = create_client(
            on_frame_recv   => sub { push @frames, { %{ $_[0] } }; 0 },
            on_stream_close => sub { push @closes, { id => $_[0], code => $_[1] }; 0 },
        );
        complete_h2_handshake($client, $client_sock);
        my $sid = submit_open_request($client, $client_sock, 'POST');
        exchange_frames($client, $client_sock, 20);

        ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
        is(\@events, [ { type => 'http.disconnect' } ],
            'the receive made after the terminal event resolved with http.disconnect');
        is($readings, {
            is_connected => 0, response_started => 1,
            response_complete => 1, disconnect_reason => undef,
        }, 'the object reads complete, with no disconnect reason');
        is(scalar @completes, 1, 'on_complete fired once');
        is(scalar @disconnects, 0, 'on_disconnect never fired');
        is($returned, 1, 'the application returned');
        is(wire_for(\@frames, $sid), [
            'HEADERS',
            ($shape eq 'trailers' ? ('DATA', 'HEADERS+END_STREAM') : ('DATA+END_STREAM')),
            'RST_STREAM',
        ], 'the wire is the complete response and then RST_STREAM (RFC 9113 8.1)');
        is(\@closes, [ { id => $sid, code => 0 } ],
            'the client saw the stream close with NO_ERROR');
        is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

        eval { $stream_io->close_now };
        shutdown_server($server);
    };
}

subtest 'h2 http: a HEAD response ends the scope with the request body still open' => sub {
    reset_case();
    my (@events, @completes);
    my ($returned, $readings) = (0, undef);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { push @completes, 1 });
        await $send->({ type => 'http.response.start', status => 401,
                        headers => [['content-type', 'text/plain'], ['content-length', 4]] });
        await $send->({ type => 'http.response.body', body => 'nope' });
        push @events, await $receive->();
        $readings = readings_of($cs);
        $returned = 1;
        return;
    };

    my (@frames, @closes);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $mark   = scalar @LOG;
    my $client = create_client(
        on_frame_recv   => sub { push @frames, { %{ $_[0] } }; 0 },
        on_stream_close => sub { push @closes, { id => $_[0], code => $_[1] }; 0 },
    );
    complete_h2_handshake($client, $client_sock);
    my $sid = submit_open_request($client, $client_sock, 'HEAD');
    exchange_frames($client, $client_sock, 20);

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    is(\@events, [ { type => 'http.disconnect' } ],
        'the receive made after the terminal event resolved with http.disconnect');
    is($readings, {
        is_connected => 0, response_started => 1,
        response_complete => 1, disconnect_reason => undef,
    }, 'the object reads complete, with no disconnect reason');
    is(scalar @completes, 1, 'on_complete fired once');
    is($returned, 1, 'the application returned');
    is(wire_for(\@frames, $sid), [ 'HEADERS', 'DATA+END_STREAM', 'RST_STREAM' ],
        'the suppressed body still ends the stream, and the reset follows it');
    is(\@closes, [ { id => $sid, code => 0 } ],
        'the client saw the stream close with NO_ERROR');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    eval { $stream_io->close_now };
    shutdown_server($server);
};

subtest 'h2 http: a receive parked on the body is answered by the terminal event' => sub {
    reset_case();
    my (@events, @completes);
    my ($returned, $readings) = (0, undef);
    # The parked receive and the response come from the same application:
    # the receive is armed but never awaited until after the send, so it is
    # genuinely pending on the request body when the scope ends.
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { push @completes, 1 });
        my $parked = $receive->();
        await $send->({ type => 'http.response.start', status => 401,
                        headers => [['content-type', 'text/plain'], ['content-length', 4]] });
        await $send->({ type => 'http.response.body', body => 'nope' });
        push @events, await $parked;
        $readings = readings_of($cs);
        $returned = 1;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $mark   = scalar @LOG;
    my $client = create_client();
    complete_h2_handshake($client, $client_sock);
    my $sid = submit_open_request($client, $client_sock, 'POST');
    # One body chunk, no END_STREAM: the receive is parked waiting for more.
    send_stream_data($client, $client_sock, $sid, 'hello', 0);
    exchange_frames($client, $client_sock, 20);

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    is(\@events, [ { type => 'http.disconnect' } ],
        'the pending receive resolved with the scope\'s end, not the unread body');
    is($readings, {
        is_connected => 0, response_started => 1,
        response_complete => 1, disconnect_reason => undef,
    }, 'the object reads complete, with no disconnect reason');
    is(scalar @completes, 1, 'on_complete fired once');
    is($returned, 1, 'the application returned');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    eval { $stream_io->close_now };
    shutdown_server($server);
};

subtest 'h2 http: a client RST after the complete response leaves the scope ended' => sub {
    reset_case();
    my (@completes, @disconnects);
    my ($returned, $readings) = (0, undef);
    my $released;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { push @completes, 1 });
        $cs->on_disconnect(sub { push @disconnects, 1 });
        await $send->({ type => 'http.response.start', status => 401,
                        headers => [['content-type', 'text/plain'], ['content-length', 4]] });
        await $send->({ type => 'http.response.body', body => 'nope' });
        # The scope, and the object with it, must survive only as long as the
        # application does (t/73's technique): nothing here may keep it alive.
        $released = $scope;
        Scalar::Util::weaken($released);
        await $loop->delay_future(after => 0);
        $readings = readings_of($cs);
        $returned = 1;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $mark   = scalar @LOG;
    my $client = create_client();
    complete_h2_handshake($client, $client_sock);
    my $sid = submit_open_request($client, $client_sock, 'POST');
    exchange_frames($client, $client_sock, 10);

    $client->submit_rst_stream($sid, Net::HTTP2::nghttp2::NGHTTP2_CANCEL());
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    is($readings, {
        is_connected => 0, response_started => 1,
        response_complete => 1, disconnect_reason => undef,
    }, 'the later reset did not reopen the terminal state');
    is(scalar @completes, 1, 'on_complete fired once');
    is(scalar @disconnects, 0, 'on_disconnect never fired');
    is($returned, 1, 'the application returned');
    ok(!defined $released, 'the scope was released when the application returned');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    eval { $stream_io->close_now };
    shutdown_server($server);
};

# The cap counts only the answers the server has to invent. The stream closing
# behind the terminal event queues this scope's own http.disconnect, and the
# first receive is handed that one -- a delivery, free, as t/75 pins for every
# scope. With the cap at 1 it therefore takes three calls to reach it.
subtest 'h2 http: receives after the scope ended are capped' => sub {
    reset_case();
    my (@events, @errors);
    my $returned = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'http.response.start', status => 401,
                        headers => [['content-type', 'text/plain'], ['content-length', 4]] });
        await $send->({ type => 'http.response.body', body => 'nope' });
        for my $try (1 .. 3) {
            my $ok = eval { push @events, await $receive->(); 1 };
            push @errors, $@ unless $ok;
        }
        $returned = 1;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(
        app => $app, server_opts => { max_disconnect_receives => 1 });
    my $mark   = scalar @LOG;
    my $client = create_client();
    complete_h2_handshake($client, $client_sock);
    my $sid = submit_open_request($client, $client_sock, 'POST');
    exchange_frames($client, $client_sock, 20);

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    is(\@events, [ ({ type => 'http.disconnect' }) x 2 ],
        'the queued delivery and one capped answer were both handed over');
    is(scalar @errors, 1, 'the receive past the cap failed');
    like($errors[0], qr/max_disconnect_receives/,
        'the failure names the cap that produced it');
    is($returned, 1, 'the application returned');
    is(log_levels_since($mark), { error => 1 }, 'the cap logged one error line');

    eval { $stream_io->close_now };
    shutdown_server($server);
};

subtest 'h2 http: a request the client finished is unchanged, and draws no reset' => sub {
    reset_case();
    my @events;
    my $returned = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        push @events, await $receive->();         # http.request, body complete
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type', 'text/plain'], ['content-length', 2]] });
        await $send->({ type => 'http.response.body', body => 'ok' });
        push @events, await $receive->();
        $returned = 1;
        return;
    };

    my (@frames, @closes);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $mark   = scalar @LOG;
    my $client = create_client(
        on_frame_recv   => sub { push @frames, { %{ $_[0] } }; 0 },
        on_stream_close => sub { push @closes, { id => $_[0], code => $_[1] }; 0 },
    );
    complete_h2_handshake($client, $client_sock);
    my $sid = $client->submit_request(
        method => 'POST', path => '/r', scheme => 'http', authority => 'localhost',
        headers => [['content-type', 'text/plain']], body => 'hello');
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    is(\@events, [
        { type => 'http.request', body => 'hello', more => 0 },
        { type => 'http.disconnect' },
    ], 'the body arrived and the receive after the response reported the end');
    is($returned, 1, 'the application returned');
    is(wire_for(\@frames, $sid), [ 'HEADERS', 'DATA+END_STREAM' ],
        'a request the client finished draws no reset');
    is(\@closes, [ { id => $sid, code => 0 } ], 'the stream closed with NO_ERROR');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    eval { $stream_io->close_now };
    shutdown_server($server);
};

subtest 'h2 http: chunks still in flight do not end the scope' => sub {
    reset_case();
    my (@events, @mid);
    my $returned = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'part', more => 1 });

        # Made between chunks: the scope has not ended, so this one parks.
        my $between = $receive->();
        await $loop->delay_future(after => 0);
        push @mid, {
            resolved          => $between->is_ready ? 1 : 0,
            response_started  => $cs->response_started,
            response_complete => $cs->response_complete,
        };

        await $send->({ type => 'http.response.body', body => 'done' });
        push @events, await $between;
        $returned = 1;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $mark   = scalar @LOG;
    my $client = create_client();
    complete_h2_handshake($client, $client_sock);
    my $sid = submit_open_request($client, $client_sock, 'POST');
    exchange_frames($client, $client_sock, 20);

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    is(\@mid, [ { resolved => 0, response_started => 1, response_complete => 0 } ],
        'a receive between chunks parked, and the object was not complete');
    is(\@events, [ { type => 'http.disconnect' } ],
        'the terminal chunk ended the scope and answered that receive');
    is($returned, 1, 'the application returned');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    eval { $stream_io->close_now };
    shutdown_server($server);
};



# The clean end is the application's terminal event, not the wire's. Under a
# peer window too small for the body, the data callback never reaches its
# terminal chunk, so no END_STREAM is serialized, no reset goes out and the
# stream stays open -- and the scope has still ended. This case is what
# separates the terminal-event mark from the stream close that, in every other
# case here, the wire rule triggers inside the same send.
subtest 'h2 http: a terminal body the peer cannot take still ends the scope' => sub {
    reset_case();
    my (@events, @completes, @closes);
    my ($returned, $readings) = (0, undef);
    my $body = 'x' x 40000;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { push @completes, 1 });
        await $send->({ type => 'http.response.start', status => 401,
                        headers => [['content-type', 'text/plain']] });
        # Streaming, so the body goes out through the data callback, which is
        # where the peer's per-stream window actually bites.
        await $send->({ type => 'http.response.body', body => 'x', more => 1 });
        await $send->({ type => 'http.response.body', body => $body });
        $readings = readings_of($cs);
        push @events, await $receive->();
        $returned = 1;
        return;
    };

    my %bytes;
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $mark   = scalar @LOG;
    my $client = create_client(
        on_data_chunk_recv => sub { $bytes{ $_[0] } += length $_[1]; 0 },
        on_stream_close    => sub { push @closes, { id => $_[0], code => $_[1] }; 0 },
    );
    complete_h2_handshake($client, $client_sock, initial_window_size => 2000);
    my $sid = submit_open_request($client, $client_sock, 'POST');
    exchange_frames($client, $client_sock, 20);

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    ok(($bytes{$sid} // 0) < length $body,
        'the peer\'s window stopped the body short of its end');
    is(\@closes, [], 'the stream never closed: no END_STREAM, so no reset');
    is($readings, {
        is_connected => 0, response_started => 1,
        response_complete => 1, disconnect_reason => undef,
    }, 'the object was complete at the terminal event, not at the stream close');
    is(scalar @completes, 1, 'on_complete fired once');
    is(\@events, [ { type => 'http.disconnect' } ],
        'the receive after it reported the scope\'s end');
    is($returned, 1, 'the application returned');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    eval { $stream_io->close_now };
    shutdown_server($server);
};

# A refused handshake is not a tunnel. The application answered the extended
# CONNECT with an ordinary HTTP response, so the request half it leaves open
# is an unfinished request and RFC 9113 section 8.1's NO_ERROR reset applies
# to it exactly as it does to an http scope. Only an accepted socket is
# exempt (RFC 8441 section 5); that one is pinned by t/http2/41 and t/http2/42.
subtest 'h2 websocket: a refused handshake ends with END_STREAM and a NO_ERROR reset' => sub {
    reset_case();
    my (@events, @completes, @disconnects);
    my ($returned, $readings) = (0, undef);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { push @completes, 1 });
        $cs->on_disconnect(sub { push @disconnects, 1 });
        await $receive->();                       # websocket.connect
        await $send->({ type => 'http.response.start', status => 403,
                        headers => [['content-type', 'text/plain'], ['content-length', 2]] });
        await $send->({ type => 'http.response.body', body => 'no' });
        $readings = readings_of($cs);
        push @events, await $receive->();
        $returned = 1;
        return;
    };

    my (@frames, @closes);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $mark   = scalar @LOG;
    my $client = create_client(
        on_frame_recv   => sub { push @frames, { %{ $_[0] } }; 0 },
        on_stream_close => sub { push @closes, { id => $_[0], code => $_[1] }; 0 },
    );
    complete_h2_handshake($client, $client_sock);
    my $sid = h2_submit($client, $client_sock, 'websocket');
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    ok(!$ALARM_FIRED, 'the pump finished without the test alarm');
    is(wire_for(\@frames, $sid), [ 'HEADERS', 'DATA+END_STREAM', 'RST_STREAM' ],
        'the refusal ended the stream and then reset it (RFC 9113 8.1)');
    is(\@closes, [ { id => $sid, code => 0 } ],
        'the client saw the stream close with NO_ERROR');
    is($readings, {
        is_connected => 0, response_started => 1,
        response_complete => 1, disconnect_reason => undef,
    }, 'the object reads complete, with no disconnect reason');
    is(scalar @completes, 1, 'on_complete fired once');
    is(scalar @disconnects, 0, 'on_disconnect never fired');
    is(\@events, [ { type => 'http.disconnect' } ],
        'a receive after the refusal reports the HTTP exchange it was');
    is($returned, 1, 'the application returned');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    eval { $stream_io->close_now };
    shutdown_server($server);
};

done_testing;
