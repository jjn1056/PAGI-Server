#!/usr/bin/env perl

# =============================================================================
# Test: an exception escaping a connection's own handler ends that connection,
#       not the process
#
# The application's exceptions are already caught, on both transports, by the
# eval each dispatch wrapper puts around the app call, and they keep their own
# log lines ("PAGI application error ..."). This file is about the OTHER
# exception: one thrown by the server's own code on the request tail -- a bug
# here, a croak out of the HTTP/2 binding, IO::Async::Stream::write croaking on
# a handle that is already gone. That exception belongs to one connection and
# must end only that connection.
#
# Neither transport had that boundary where it mattered. Both run the request
# under an adopted Future, and an exception failing an adopted Future reaches
# IO::Async::Notifier's invoke_error, which -- with no on_error registered on
# PAGI::Server and no parent notifier -- dies out of $loop->run and takes the
# whole process with it. HTTP/1.1's read-handler eval covered its request tail
# only while the application had not suspended: Future::AsyncAwait runs the
# request's async sub synchronously until the first await, and every
# application that does I/O passes one, after which the tail resumes on the
# event loop's own stack, outside that eval. Each handler now carries the same
# eval around its own tail, reporting through one helper, so the fault ends the
# connection and never the process on either transport and whether or not the
# application suspended.
#
# Both halves of the contract are pinned here, because they differ in what the
# connection object is allowed to say:
#   * the tail fails BEFORE the scope completed -> server_error with the
#     exception as disconnect_detail, on_disconnect fires, on_complete never.
#   * the tail fails AFTER the response was fully delivered -> delivery defines
#     completion (Www.pod), so the scope is complete, on_complete fires and
#     disconnect_reason stays undef. The connection still closes and the line is
#     still logged; the exception cannot retroactively un-complete a delivered
#     response.
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

my $have_h2 = do {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available ? 1 : 0;
};

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# Every case pumps a loop that a fault injection is about to disturb; a wedged
# pump must fail this file rather than hang the suite. The mutation check (the
# boundary removed) relies on this too: without the boundary the pump dies, and
# that death must be reported, not waited out.
$SIG{ALRM} = sub { die "t/81 exceeded its time bound\n" };
alarm 120;

# Nothing here may write to stderr unasked.
my @warnings;
$SIG{__WARN__} = sub { push @warnings, $_[0] };

sub assert_no_warnings {
    my ($label) = @_;
    my @w = @warnings;
    @warnings = ();
    is(scalar(@w), 0, "$label: nothing was warned to stderr") or diag("unexpected: @w");
    return;
}

# -----------------------------------------------------------------------------
# Fault injection
# -----------------------------------------------------------------------------
# The same technique on both transports: redefine one of the server's own
# methods so that it throws once, on the connection under test. Each target is a
# call the request TAIL makes -- after the application returned, outside the
# eval that catches application exceptions -- so what it throws is the server's
# exception, not the application's.
#
# $ARMED holds a predicate called with the short name of the injected method
# followed by that call's own arguments; the replacement throws for the first
# call the predicate accepts and then disarms itself, so an injection aimed at
# one site on one connection cannot leak into another site, another connection
# or a later request.
our $ARMED;
my $INJECTED  = 'injected: server tail failure';   # as the log and the detail carry it
my $INJECTED_DIE = "$INJECTED\n";

sub inject_once {
    my ($full_name) = @_;
    my ($short) = $full_name =~ /([^:]+)$/;
    no strict 'refs';
    no warnings 'redefine';
    my $original = \&{$full_name};
    *{$full_name} = sub {
        if ($ARMED && $ARMED->($short, @_)) {
            $ARMED = undef;
            die $INJECTED_DIE;
        }
        goto &$original;
    };
    return;
}

# HTTP/2: the first thing _h2_dispatch_stream's tail asks after the application
# returns, ahead of every log line on that path -- so a throw here is the tail
# failing before anything else has been said about the stream.
inject_once('PAGI::Server::Connection::_h2_stream_alive');

# HTTP/2: the first of the three builders _h2_dispatch_stream runs BEFORE the
# application is called at all, so a throw here is the server failing while it
# is still constructing the stream's scope -- earlier than any Future the
# dispatch could hang a failure handler on.
inject_once('PAGI::Server::Connection::_h2_create_scope');

# HTTP/1.1: the tail's transition out of the scope, reached on the path where
# the application returned without starting a response -- the h1 twin of the
# above: the server's own code failing while the scope is still open.
inject_once('PAGI::Server::Connection::_end_scope');

# Both transports: the last call of either tail, made once the response has
# been fully delivered. This is the "after delivery" half of the contract.
inject_once('PAGI::Server::_on_request_complete');

# -----------------------------------------------------------------------------
# The application every case runs
# -----------------------------------------------------------------------------
# /boom returns without starting a response, so the tail is still holding an
# open scope when the injected throw lands. Every other path answers 200 "ok".
my (@log, %seen);

sub reset_observations { @log = (); %seen = (); return }

sub app_log_errors {
    return [grep { $_->{level} eq 'error' } @log];
}

sub log_lines {
    return join "\n", map { "[$_->{level}] $_->{message}" } @log;
}

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    # This application serves requests only; declining the listening server's
    # lifespan scope by raising on it is the interoperable default (auto mode)
    # and is not logged.
    my $type = $scope->{type} // '';
    die "no lifespan handler\n"
        unless $type eq 'http' || $type eq 'sse' || $type eq 'websocket';
    my $path = $scope->{path};
    my $cs   = $scope->{'pagi.connection'};
    my $obs  = $seen{$path} = { complete => 0, disconnect => [], state => $cs };
    $cs->on_complete(sub { $obs->{complete}++ });
    $cs->on_disconnect(sub { push @{$obs->{disconnect}}, $_[1] });

    # Both /boom paths return without having started a response, so the scope
    # is still open when the injected exception lands on the tail. They differ
    # in where that tail runs. /boom suspends first, as any application that
    # does I/O does, and a suspended application resumes on the event loop's
    # own stack -- so its tail is outside the eval around the dispatch call and
    # needs the boundary the dispatch wrapper puts around itself. /boom-sync
    # never suspends, so its tail is still inside the caller that started the
    # dispatch.
    if ($path eq '/boom') {
        await $loop->delay_future(after => 0);
        return;
    }
    return if $path eq '/boom-sync';
    die "application blew up\n" if $path eq '/throw';
    # /slow is the in-flight sibling: it is still parked inside the
    # application when the boundary ends the connection under it, and returns
    # afterwards without ever having started a response.
    if ($path eq '/slow') {
        await $loop->delay_future(after => 0.5);
        return;
    }

    await $receive->();
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain'], ['content-length', 2]] });
    await $send->({ type => 'http.response.body', body => 'ok' });
};

sub create_server {
    my (%opt) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1,
        access_log => undef, shutdown_timeout => 1,
        logger => sub { push @log, { level => $_[0]{level}, message => $_[0]{message} } },
        %opt,
    );
    $loop->add($server);
    return $server;
}

# -----------------------------------------------------------------------------
# HTTP/2 harness (socketpair, shape borrowed from t/http2/10-h2c.t)
# -----------------------------------------------------------------------------

sub h2_connect {
    my ($server) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;

    require Net::HTTP2::nghttp2::Session;
    my $peer = { body => {}, closed => {}, status => {} };
    $peer->{session} = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { my ($sid, $n, $v) = @_;
                                    $peer->{status}{$sid} = $v if $n eq ':status'; 0 },
        on_frame_recv      => sub { 0 },
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $peer->{body}{$sid} .= $d; 0 },
        on_stream_close    => sub { my ($sid, $c) = @_; $peer->{closed}{$sid} = $c // 0; 0 },
    });
    $peer->{sock}   = $sock_b;
    $peer->{conn}   = $conn;
    $peer->{stream} = $stream;
    return $peer;
}

# Pump the loop and move bytes for every peer given. Returns the exception if
# the loop itself threw -- which is the pre-fix behaviour this file exists to
# rule out -- and undef when it ran normally.
sub pump {
    my ($rounds, @peers) = @_;
    for (1 .. $rounds) {
        eval { $loop->loop_once(0.05); 1 } or return $@;
        for my $p (@peers) {
            my $in = '';
            $p->{sock}->sysread($in, 16384);
            $p->{session}->mem_recv($in) if length $in;
            my $out = $p->{session}->mem_send;
            $p->{sock}->syswrite($out) if length $out;
        }
    }
    return undef;
}

sub h2_handshake {
    my ($peer) = @_;
    $loop->loop_once(0.1);
    my $settings = '';
    $peer->{sock}->sysread($settings, 4096);
    $peer->{session}->send_connection_preface;
    $peer->{sock}->syswrite($peer->{session}->mem_send);
    $loop->loop_once(0.1);
    $peer->{session}->mem_recv($settings);
    $loop->loop_once(0.1);
    my $ack = '';
    $peer->{sock}->sysread($ack, 4096);
    $peer->{session}->mem_recv($ack) if length $ack;
    my $out = $peer->{session}->mem_send;
    $peer->{sock}->syswrite($out) if length $out;
    $loop->loop_once(0.1);
    my $extra = '';
    $peer->{sock}->sysread($extra, 4096);
    $peer->{session}->mem_recv($extra) if length $extra;
    return;
}

sub h2_get {
    my ($peer, $path) = @_;
    my $sid = $peer->{session}->submit_request(
        method => 'GET', path => $path, scheme => 'http',
        authority => 'localhost', headers => []);
    $peer->{sock}->syswrite($peer->{session}->mem_send);
    return $sid;
}

# -----------------------------------------------------------------------------
# HTTP/1.1 harness (real listening server, raw socket)
# -----------------------------------------------------------------------------

sub h1_connect {
    my ($server) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1',
        PeerPort => $server->port, Proto => 'tcp', Timeout => 5) or die "connect: $!";
    $sock->blocking(0);
    return $sock;
}

# Send $request and pump, accumulating the wire. Returns (wire, loop exception).
sub h1_exchange {
    my ($sock, $request, $rounds) = @_;
    print $sock $request;
    my $wire = '';
    for (1 .. ($rounds // 25)) {
        eval { $loop->loop_once(0.05); 1 } or return ($wire, $@);
        my $buf;
        my $n = sysread($sock, $buf, 65536);
        $wire .= $buf if $n;
    }
    return ($wire, undef);
}

# =============================================================================
# (1) HTTP/2: the tail throws while the scope is still open
# =============================================================================
SKIP: {
    skip "HTTP/2 not available", 1 unless $have_h2;

    subtest 'HTTP/2: an exception on the dispatch tail ends that connection' => sub {
        reset_observations();
        my $server = create_server(http2 => 1);
        my $a = h2_connect($server);
        h2_handshake($a);

        # Aimed at this connection's tail and nothing else.
        my $conn = $a->{conn};
        $ARMED = sub { $_[0] eq '_h2_stream_alive' && $_[1] == $conn };
        h2_get($a, '/boom');
        my $loop_error = pump(30, $a);

        is($loop_error, undef, 'the event loop kept running: the exception never reached invoke_error');
        is($ARMED, undef, 'the injected failure did fire');

        is(app_log_errors(), [
            { level => 'error',
              message => "PAGI connection handler error (HTTP/2 stream 1): $INJECTED" },
        ], 'exactly one error line, naming the connection handler and the stream')
            or diag(log_lines());

        my $obs = $seen{'/boom'};
        ok($obs, 'the application ran') or return;
        is($obs->{state}->disconnect_reason, 'server_error', 'the scope ended with server_error');
        is($obs->{state}->disconnect_detail, 'injected: server tail failure',
            'the exception is the disconnect detail');
        is($obs->{state}->is_connected, 0, 'the connection object reports the connection gone');
        is($obs->{complete}, 0, 'on_complete never fired: nothing was delivered');
        is($obs->{disconnect}, ['injected: server tail failure'],
            'on_disconnect fired once, with the same detail');

        # The process is alive and serving: a new connection to the same server
        # completes a request.
        my $b = h2_connect($server);
        h2_handshake($b);
        my $sid = h2_get($b, '/after');
        is(pump(30, $b), undef, 'the loop still runs for the next connection');
        is($b->{status}{$sid}, '200', 'a connection opened afterwards is served');
        is($b->{body}{$sid}, 'ok', 'and gets its body');

        $_->{stream}->close_now for $a, $b;
        $loop->remove($server);
        assert_no_warnings('h2 tail exception');
    };
}

# =============================================================================
# (1b) HTTP/2: the exception lands while the stream's scope is being built
# =============================================================================
# _h2_create_scope and the two builders beside it run before the application is
# called, so at that moment there is no dispatch Future in existence and
# nothing for a failure handler to hang off. The boundary therefore has to sit
# around the deferred dispatch call itself, and it must answer a throw from
# there with the same one line and the same connection-level ending as a throw
# from the tail.
SKIP: {
    skip "HTTP/2 not available", 1 unless $have_h2;

    subtest 'HTTP/2: an exception while the scope is built ends that connection' => sub {
        reset_observations();
        my $server = create_server(http2 => 1);
        my $a = h2_connect($server);
        h2_handshake($a);

        my $conn = $a->{conn};
        $ARMED = sub { $_[0] eq '_h2_create_scope' && $_[1] == $conn };
        h2_get($a, '/boom');
        my $loop_error = pump(30, $a);

        is($loop_error, undef, 'the event loop kept running: the exception never reached invoke_error');
        is($ARMED, undef, 'the injected failure did fire');
        ok(!$seen{'/boom'}, 'the application was never reached');

        is(app_log_errors(), [
            { level => 'error',
              message => "PAGI connection handler error (HTTP/2 stream 1): $INJECTED" },
        ], 'exactly one error line, the same boundary line the tail produces')
            or diag(log_lines());

        my $b = h2_connect($server);
        h2_handshake($b);
        my $sid = h2_get($b, '/after');
        is(pump(30, $b), undef, 'the loop still runs for the next connection');
        is($b->{status}{$sid}, '200', 'a connection opened afterwards is served');
        is($b->{body}{$sid}, 'ok', 'and gets its body');

        $_->{stream}->close_now for $a, $b;
        $loop->remove($server);
        assert_no_warnings('h2 scope construction exception');
    };
}

# =============================================================================
# (2) HTTP/2: a second connection open at the time is unaffected
# =============================================================================
SKIP: {
    skip "HTTP/2 not available", 1 unless $have_h2;

    subtest 'HTTP/2: a concurrent connection is unaffected' => sub {
        reset_observations();
        my $server = create_server(http2 => 1);
        my ($a, $b) = (h2_connect($server), h2_connect($server));
        h2_handshake($_) for $a, $b;

        # Both requests are in flight together; only $a's tail is rigged.
        my $conn = $a->{conn};
        $ARMED = sub { $_[0] eq '_h2_stream_alive' && $_[1] == $conn };
        h2_get($a, '/boom');
        my $sid_b = h2_get($b, '/concurrent');
        my $loop_error = pump(40, $a, $b);

        is($loop_error, undef, 'the event loop kept running');
        is($b->{status}{$sid_b}, '200', "the concurrent connection's request was answered");
        is($b->{body}{$sid_b}, 'ok', 'with its body intact');
        is($seen{'/concurrent'}{complete}, 1, 'and its scope completed');
        is($seen{'/concurrent'}{disconnect}, [], 'with no disconnect on that connection');
        is($seen{'/boom'}{state}->disconnect_reason, 'server_error',
            'while the rigged connection ended with server_error');

        is(app_log_errors(), [
            { level => 'error',
              message => "PAGI connection handler error (HTTP/2 stream 1): $INJECTED" },
        ], 'still exactly one error line') or diag(log_lines());

        $_->{stream}->close_now for $a, $b;
        $loop->remove($server);
        assert_no_warnings('h2 concurrent connection');
    };
}

# =============================================================================
# (2b) HTTP/2: an in-flight sibling stream on the same connection stays quiet
# =============================================================================
# Ending the connection ends every stream on it: _handle_disconnect stamps each
# one server_error with the same detail before any of their applications
# resume. When a sibling's application then returns into a connection that no
# longer exists, that is a connection-level end like every other one, and a
# connection-level end is quiet for the scopes it catches in flight. The
# alternative is one line for the real fault plus one line per open stream
# accusing the application of returning without a response it was never given
# the chance to start.
SKIP: {
    skip "HTTP/2 not available", 1 unless $have_h2;

    subtest 'HTTP/2: an in-flight sibling stream is not blamed for the fault' => sub {
        reset_observations();
        my $server = create_server(http2 => 1);
        my $a = h2_connect($server);
        h2_handshake($a);

        my $slow = h2_get($a, '/slow');    # stream 1, parked in the application
        my $sid  = h2_get($a, '/boom');    # stream 3, whose tail is rigged
        my $conn = $a->{conn};
        $ARMED = sub { $_[0] eq '_h2_stream_alive' && $_[1] == $conn };
        my $loop_error = pump(40, $a);

        is($loop_error, undef, 'the event loop kept running');
        is($ARMED, undef, 'the injected failure did fire');

        is(app_log_errors(), [
            { level => 'error',
              message => "PAGI connection handler error (HTTP/2 stream 3): $INJECTED" },
        ], 'one error line for the connection, none for the sibling')
            or diag(log_lines());

        my $sib = $seen{'/slow'};
        ok($sib, 'the sibling application ran and returned') or return;
        is($sib->{state}->disconnect_reason, 'server_error',
            'the sibling stream carries the connection-level ending');
        is($sib->{state}->disconnect_detail, $INJECTED,
            'with the same detail the connection recorded');
        is($sib->{complete}, 0, 'and never completed');
        is($a->{status}{$slow}, undef, 'the sibling was never answered');

        $a->{stream}->close_now;
        $loop->remove($server);
        assert_no_warnings('h2 in-flight sibling');
    };
}

# =============================================================================
# (3) Control: an application exception keeps its own path
# =============================================================================
SKIP: {
    skip "HTTP/2 not available", 1 unless $have_h2;

    subtest 'HTTP/2: an application exception is not the connection boundary' => sub {
        reset_observations();
        my $server = create_server(http2 => 1);
        my $a = h2_connect($server);
        h2_handshake($a);

        $ARMED = undef;   # nothing injected: the application itself throws
        my $sid = h2_get($a, '/throw');
        is(pump(30, $a), undef, 'the event loop kept running');

        is($a->{status}{$sid}, '500', 'the application error is answered with 500');
        my $errors = app_log_errors();
        is(scalar(@$errors), 1, 'one error line') or diag(log_lines());
        like($errors->[0]{message},
            qr/^PAGI application error \(HTTP\/2 stream 1\): application blew up/,
            'it is the application-error line, unchanged');
        unlike($errors->[0]{message}, qr/connection handler error/,
            'the connection boundary did not claim an application exception');
        is($seen{'/throw'}{state}->disconnect_reason, 'server_error',
            'the application error still ends the scope with server_error');

        $a->{stream}->close_now;
        $loop->remove($server);
        assert_no_warnings('h2 application exception');
    };
}

# =============================================================================
# (4) The HTTP/1.1 twin: a tail that never left the read handler's own stack
# =============================================================================
subtest 'HTTP/1.1: an exception on the request tail ends that connection' => sub {
    reset_observations();
    my $server = create_server;
    $server->listen->get;

    my $idle = h1_connect($server);   # a second connection, open the whole time
    my $sock = h1_connect($server);

    $ARMED = sub { $_[0] eq '_end_scope' && ($_[1]{current_request}{path} // '') eq '/boom-sync' };
    my ($wire, $loop_error) = h1_exchange($sock, "GET /boom-sync HTTP/1.1\r\nHost: x\r\n\r\n");

    is($loop_error, undef, 'the event loop kept running');
    is($ARMED, undef, 'the injected failure did fire');

    is(app_log_errors(), [
        { level => 'error', message => 'PAGI application returned without starting a response' },
        { level => 'error', message => "PAGI connection handler error (HTTP/1.1): $INJECTED" },
    ], 'the existing no-response line, then the boundary line') or diag(log_lines());

    my $obs = $seen{'/boom-sync'};
    is($obs->{state}->disconnect_reason, 'server_error', 'the scope ended with server_error');
    is($obs->{state}->disconnect_detail, 'injected: server tail failure',
        'the exception is the disconnect detail');
    is($obs->{complete}, 0, 'on_complete never fired');
    is($obs->{disconnect}, ['injected: server tail failure'], 'on_disconnect fired once');

    # The connection that was open the whole time still works.
    my ($idle_wire, $idle_error) = h1_exchange($idle, "GET /concurrent HTTP/1.1\r\nHost: x\r\n\r\n");
    is($idle_error, undef, 'the loop still runs');
    like($idle_wire, qr{^HTTP/1\.1 200 OK\r\n}, 'the connection open at the time is still served');
    like($idle_wire, qr{ok\z}, 'and gets its body');

    close $_ for $sock, $idle;
    $loop->remove($server);
    assert_no_warnings('h1 tail exception');
};

# =============================================================================
# (5) Both transports: an exception AFTER the response was delivered
# =============================================================================
# Delivery defines completion (Www.pod): the scope is complete before the tail's
# last call, so the exception closes the connection and is logged, but it cannot
# turn a delivered response into a disconnect.
subtest 'a tail exception after delivery closes the connection without un-completing it' => sub {
    reset_observations();
    my $server = create_server;
    $server->listen->get;
    my $other = h1_connect($server);
    my $sock  = h1_connect($server);

    $ARMED = sub { $_[0] eq '_on_request_complete' };
    my ($wire, $loop_error) = h1_exchange($sock, "GET /delivered HTTP/1.1\r\nHost: x\r\n\r\n");

    is($loop_error, undef, 'HTTP/1.1: the event loop kept running');
    like($wire, qr{^HTTP/1\.1 200 OK\r\n}, 'the response the client already had is intact');
    is(app_log_errors(), [
        { level => 'error', message => "PAGI connection handler error (HTTP/1.1): $INJECTED" },
    ], 'one error line, the HTTP/1.1 boundary') or diag(log_lines());
    is($seen{'/delivered'}{complete}, 1, 'on_complete fired: the response was delivered');
    is($seen{'/delivered'}{state}->disconnect_reason, undef,
        'and disconnect_reason stays undef');

    my ($other_wire, $other_error) = h1_exchange($other, "GET /next HTTP/1.1\r\nHost: x\r\n\r\n");
    is($other_error, undef, 'the process survived');
    like($other_wire, qr{^HTTP/1\.1 200 OK\r\n}, 'another connection is still served');

    close $_ for $sock, $other;
    $loop->remove($server);
    assert_no_warnings('h1 after delivery');

    skip_all_h2: {
        last skip_all_h2 unless $have_h2;
        reset_observations();
        my $h2_server = create_server(http2 => 1);
        my $a = h2_connect($h2_server);
        h2_handshake($a);
        $ARMED = sub { $_[0] eq '_on_request_complete' };
        my $sid = h2_get($a, '/delivered');
        is(pump(30, $a), undef, 'HTTP/2: the event loop kept running');
        is($a->{status}{$sid}, '200', 'the delivered response is intact');
        is($a->{body}{$sid}, 'ok', 'with its body');
        is(app_log_errors(), [
            { level => 'error',
              message => "PAGI connection handler error (HTTP/2 stream 1): $INJECTED" },
        ], 'one error line, the HTTP/2 boundary') or diag(log_lines());
        is($seen{'/delivered'}{complete}, 1, 'on_complete fired');
        is($seen{'/delivered'}{state}->disconnect_reason, undef, 'disconnect_reason stays undef');
        $a->{stream}->close_now;
        $loop->remove($h2_server);
        assert_no_warnings('h2 after delivery');
    }
};

# =============================================================================
# (6) HTTP/1.1: the same tail, once the application has SUSPENDED
# =============================================================================
# Subtest (4) above pins the tail of an application that never suspends, which
# still runs inside the read handler's own eval. Every real application does
# I/O: it suspends, and it resumes on the event loop's own stack, outside that
# eval. Its request Future is adopted exactly as HTTP/2's dispatch Future is,
# so before the boundary in the request path's own async sub a throw from the
# server's tail reached invoke_error and died out of $loop->run.
#
# The three h1 scope kinds are dispatched from the same place and adopted the
# same way, so each one's tail is pinned here.
subtest 'HTTP/1.1: an exception on a suspended application\'s tail ends that connection' => sub {
    reset_observations();
    my $server = create_server;
    $server->listen->get;

    my $idle = h1_connect($server);   # a second connection, open the whole time
    my $sock = h1_connect($server);

    $ARMED = sub { $_[0] eq '_end_scope' && ($_[1]{current_request}{path} // '') eq '/boom' };
    my ($wire, $loop_error) = h1_exchange($sock, "GET /boom HTTP/1.1\r\nHost: x\r\n\r\n");

    is($loop_error, undef, 'the event loop kept running');
    is($ARMED, undef, 'the injected failure did fire');

    is(app_log_errors(), [
        { level => 'error', message => 'PAGI application returned without starting a response' },
        { level => 'error', message => "PAGI connection handler error (HTTP/1.1): $INJECTED" },
    ], 'the existing no-response line, then exactly one boundary line') or diag(log_lines());

    my $obs = $seen{'/boom'};
    is($obs->{state}->disconnect_reason, 'server_error', 'the scope ended with server_error');
    is($obs->{state}->disconnect_detail, $INJECTED, 'the exception is the disconnect detail');
    is($obs->{complete}, 0, 'on_complete never fired');
    is($obs->{disconnect}, [$INJECTED], 'on_disconnect fired once');

    my ($idle_wire, $idle_error) = h1_exchange($idle, "GET /concurrent HTTP/1.1\r\nHost: x\r\n\r\n");
    is($idle_error, undef, 'the loop still runs');
    like($idle_wire, qr{^HTTP/1\.1 200 OK\r\n}, 'a second connection is still served');
    like($idle_wire, qr{ok\z}, 'and gets its body');

    close $_ for $sock, $idle;
    $loop->remove($server);
    assert_no_warnings('h1 suspended tail exception');
};

subtest 'SSE: an exception on a suspended application\'s tail ends that connection' => sub {
    reset_observations();
    my $server = create_server;
    $server->listen->get;

    my $idle = h1_connect($server);
    my $sock = h1_connect($server);

    $ARMED = sub { $_[0] eq '_end_scope' && ($_[1]{current_request}{path} // '') eq '/boom' };
    my ($wire, $loop_error) = h1_exchange($sock,
        "GET /boom HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n");

    is($loop_error, undef, 'the event loop kept running');
    is($ARMED, undef, 'the injected failure did fire');

    is(app_log_errors(), [
        { level => 'error',
          message => 'PAGI application returned without starting an SSE stream or a response' },
        { level => 'error', message => "PAGI connection handler error (HTTP/1.1): $INJECTED" },
    ], 'the existing no-response line, then exactly one boundary line') or diag(log_lines());

    my $obs = $seen{'/boom'};
    is($obs->{state}->disconnect_reason, 'server_error', 'the scope ended with server_error');
    is($obs->{state}->disconnect_detail, $INJECTED, 'the exception is the disconnect detail');
    is($obs->{complete}, 0, 'on_complete never fired');

    my ($idle_wire, $idle_error) = h1_exchange($idle, "GET /concurrent HTTP/1.1\r\nHost: x\r\n\r\n");
    is($idle_error, undef, 'the loop still runs');
    like($idle_wire, qr{^HTTP/1\.1 200 OK\r\n}, 'a second connection is still served');

    close $_ for $sock, $idle;
    $loop->remove($server);
    assert_no_warnings('sse suspended tail exception');
};

subtest 'WebSocket: an exception on a suspended application\'s tail ends that connection' => sub {
    reset_observations();
    my $server = create_server;
    $server->listen->get;

    my $idle = h1_connect($server);
    my $sock = h1_connect($server);

    $ARMED = sub { $_[0] eq '_end_scope' && ($_[1]{current_request}{path} // '') eq '/boom' };
    my ($wire, $loop_error) = h1_exchange($sock,
          "GET /boom HTTP/1.1\r\nHost: x\r\n"
        . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
        . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n");

    is($loop_error, undef, 'the event loop kept running');
    is($ARMED, undef, 'the injected failure did fire');

    is(app_log_errors(), [
        { level => 'error',
          message => 'PAGI application returned without accepting the WebSocket or refusing the handshake' },
        { level => 'error', message => "PAGI connection handler error (HTTP/1.1): $INJECTED" },
    ], 'the existing no-response line, then exactly one boundary line') or diag(log_lines());

    my $obs = $seen{'/boom'};
    is($obs->{state}->disconnect_reason, 'server_error', 'the scope ended with server_error');
    is($obs->{state}->disconnect_detail, $INJECTED, 'the exception is the disconnect detail');
    is($obs->{complete}, 0, 'on_complete never fired');

    my ($idle_wire, $idle_error) = h1_exchange($idle, "GET /concurrent HTTP/1.1\r\nHost: x\r\n\r\n");
    is($idle_error, undef, 'the loop still runs');
    like($idle_wire, qr{^HTTP/1\.1 200 OK\r\n}, 'a second connection is still served');

    close $_ for $sock, $idle;
    $loop->remove($server);
    assert_no_warnings('websocket suspended tail exception');
};

alarm 0;
done_testing;
