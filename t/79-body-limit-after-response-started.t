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

# ============================================================
# Test: a request body that crosses max_body_size AFTER the response started
# ============================================================
# PAGI::Spec::Www 0.6, "Application Left a Response Incomplete": once a
# response has started the server MUST NOT synthesize terminal framing for it
# and MUST leave it observable as truncated. A 413 submitted at that point is
# a SECOND response on the same exchange, which the spec forbids and which
# HTTP/2 cannot even carry (nghttp2 refuses a second data provider on a live
# stream and the whole connection dies).
#
# So max_body_size has two answers, split on whether this scope has started
# its response:
#
#   not started  ->  413 Payload Too Large, exactly as before.
#   started      ->  no second response. The scope ends abnormally with the
#                    Standard Disconnect Reason body_too_large, the
#                    application is told through its connection object, its
#                    disconnect event and one log line, and the wire is
#                    truncated: RST_STREAM INTERNAL_ERROR on HTTP/2 (the rest
#                    of the connection lives), a close with no terminator on
#                    HTTP/1.1.
#
# A declared Content-Length over the limit never reaches either branch: it is
# answered 413 before the application runs. That is pinned elsewhere --
# t/22-content-length-keepalive.t (HTTP/1.1 parse layer),
# t/http2/21-http-date-header.t "content-length precheck rejected with 413"
# and t/http2/12-error-handling.t "Server responded with 413 based on
# content-length" (HTTP/2) -- so this file covers only the undeclared-length
# case, which is the one that can arrive after the response has started.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

my $have_h2 = do {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available ? 1 : 0;
};

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

my $LIMIT = 1000;

# Every case here pumps a loop against a peer that may never speak again; a
# wedged pump must fail the file rather than hang the suite.
$SIG{ALRM} = sub { die "t/79 exceeded its time bound\n" };
alarm 300;

# Nothing here may write to stderr unasked, and the one thing that does is
# pinned rather than hidden (see h1_teardown_warning below).
my @warnings;
$SIG{__WARN__} = sub { push @warnings, $_[0] };
sub drain_warnings { my @w = @warnings; @warnings = (); return @w }

sub assert_no_warnings {
    my ($label) = @_;
    my @w = drain_warnings();
    is(scalar(@w), 0, "$label: nothing was warned to stderr")
        or diag("unexpected: @w");
}

# A defect this file is the first to reach, and NOT this ending's doing: it
# fires identically on the unchanged pre-response 413 path (the control case
# below). _close settles every tracked receive Future, including the one the
# receive() call now on the stack is still producing, so Future::AsyncAwait
# sees that call's returning Future resolved out from under it. Every
# HTTP/1.1 path that closes from inside a receive has it; the fix belongs to
# the receive/close ordering, not to max_body_size (tracking ledger item
# L11; remove this pin with that fix). Pinned here so it cannot
# change or multiply unnoticed.
sub assert_h1_teardown_warning {
    my ($label) = @_;
    my @w = drain_warnings();
    is(scalar(@w), 1, "$label: exactly one stderr warning, the known h1 close-from-receive one");
    like($w[0], qr/lost its returning future/,
        "$label: and it is that one, not an application or protocol error")
        if @w;
}

# The two sentences the server says for this ending, spelled out once so the
# assertions pin the exact text an application author reads.
my $DETAIL = "request body exceeded max_body_size ($LIMIT bytes)"
           . " after the response had started; response truncated";
sub log_line {
    my ($where) = @_;
    return "request body exceeded max_body_size ($LIMIT bytes)"
         . " after the response had started ($where); response truncated";
}

# ============================================================
# HTTP/1.1 harness
# ============================================================

sub h1_server {
    my (%o) = @_;
    my $server = PAGI::Server->new(
        app              => $o{app},
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        access_log       => undef,
        shutdown_timeout => 1,
        max_body_size    => $LIMIT,
        ($o{log} ? (logger => sub { push @{$o{log}}, $_[0] }) : ()),
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

# Open a socket, send $head, and hand back a pump that accumulates everything
# the server writes plus whether the peer has closed.
sub h1_open {
    my ($port, $head) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5)
        or die "connect: $!";
    syswrite($sock, $head);
    $sock->blocking(0);
    my ($wire, $eof) = ('', 0);
    my $pump = sub {
        my ($rounds) = @_;
        for (1 .. ($rounds // 30)) {
            $loop->loop_once(0.02);
            my $buf;
            my $n = sysread($sock, $buf, 65536);
            if (defined $n) { $n ? ($wire .= $buf) : ($eof = 1) }
        }
        return $wire;
    };
    return ($sock, $pump, \$wire, \$eof);
}

sub chunk { my ($d) = @_; return sprintf("%x\r\n%s\r\n", length($d), $d) }

# ============================================================
# HTTP/2 harness (shape borrowed from t/71; its copy does not pass
# max_body_size through to Connection->new, which is the limit this file
# needs the connection itself to enforce)
# ============================================================

sub h2_conn {
    my (%o) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app    = $o{app};
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, http2 => 1,
        access_log => undef,
        max_body_size => $LIMIT,
        ($o{log} ? (logger => sub { push @{$o{log}}, $_[0] }) : ()),
    );
    $loop->add($server);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
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

sub h2_client {
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

sub h2_handshake {
    my ($client, $sock) = @_;
    $loop->loop_once(0.1);
    my $settings = '';
    $sock->sysread($settings, 4096);
    $client->send_connection_preface;
    $sock->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($settings);
    $loop->loop_once(0.1);
    my $ack = '';
    $sock->sysread($ack, 4096);
    $client->mem_recv($ack) if length($ack);
    my $out = $client->mem_send;
    $sock->syswrite($out) if length($out);
    $loop->loop_once(0.1);
    my $extra = '';
    $sock->sysread($extra, 4096);
    $client->mem_recv($extra) if length($extra);
}

sub h2_exchange {
    my ($client, $sock, $rounds) = @_;
    for (1 .. ($rounds // 25)) {
        $loop->loop_once(0.03);
        my $buf = '';
        $sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $sock->syswrite($out) if length($out);
    }
}

# ============================================================
# The applications under test
# ============================================================

# What every case records about the scope it ran in.
sub watch {
    my ($scope, $r) = @_;
    my $conn = $scope->{'pagi.connection'};
    $conn->on_disconnect(sub { push @{$r->{disconnects}}, [@_] });
    $conn->on_complete(sub { $r->{completes}++ });
    return $conn;
}

# A streaming response that starts at once and is still open when the client's
# upload crosses the limit. $read says whether it also consumes the request
# body: HTTP/1.1 parses a chunked body only from inside $receive, so that is
# the only shape in which its limit can be reached at all.
sub streaming_app {
    my ($r, %o) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        if (($scope->{path} // '') eq '/ok') {
            await $send->({ type => 'http.response.start', status => 200,
                            headers => [['content-type', 'text/plain']] });
            await $send->({ type => 'http.response.body', body => 'second' });
            return;
        }
        my $conn = watch($scope, $r);
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => "progress 1\n", more => 1 });
        $r->{started} = 1;
        if ($o{read}) {
            while (1) {
                my $ev = await $receive->();
                push @{$r->{events}}, $ev;
                last if $ev->{type} =~ /\.disconnect$/;
            }
        }
        else {
            await Future->wait_any(
                $conn->disconnect_future->without_cancel,
                $loop->delay_future(after => 5)->then(sub { Future->done('timeout') }),
            );
        }
        $r->{woke}     = 1;
        $r->{reason}   = $conn->disconnect_reason;
        $r->{detail}   = $conn->disconnect_detail;
        $r->{future}   = $conn->disconnect_future->is_ready ? 1 : 0;
        $r->{returned} = 1;
    };
}

# The same shape on an sse scope.
sub sse_app {
    my ($r, %o) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        my $conn = watch($scope, $r);
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', data => 'hello' });
        $r->{started} = 1;
        if ($o{read}) {
            while (1) {
                my $ev = await $receive->();
                push @{$r->{events}}, $ev;
                last if $ev->{type} =~ /\.disconnect$/;
            }
        }
        else {
            await Future->wait_any(
                $conn->disconnect_future->without_cancel,
                $loop->delay_future(after => 5)->then(sub { Future->done('timeout') }),
            );
        }
        $r->{woke}     = 1;
        $r->{reason}   = $conn->disconnect_reason;
        $r->{detail}   = $conn->disconnect_detail;
        $r->{future}   = $conn->disconnect_future->is_ready ? 1 : 0;
        $r->{returned} = 1;
    };
}

# An application that never sends anything: the control for the branch that
# still answers 413.
sub silent_app {
    my ($r) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        my $conn = watch($scope, $r);
        while (1) {
            my $ev = await $receive->();
            push @{$r->{events}}, $ev;
            last if $ev->{type} =~ /\.disconnect$/;
        }
        $r->{reason} = $conn->disconnect_reason;
        $r->{detail} = $conn->disconnect_detail;
        $r->{returned} = 1;
    };
}

# Exactly the lines this ending is allowed to produce.
sub assert_one_log {
    my ($log, $where, $label) = @_;
    is(scalar(@$log), 1, "$label: the scope logged exactly one line");
    is($log->[0]{level}, 'error', "$label: the line is at error level")
        if @$log;
    is($log->[0]{message}, log_line($where), "$label: the line names the limit, the transport and the truncation")
        if @$log;
    ok(!(grep { $_->{message} =~ /incomplete response/ } @$log),
        "$label: the dispatch wrapper's incomplete-response line did not fire as well");
}

# ============================================================
# 1. HTTP/2: a plain http streaming response, body never read
# ============================================================

SKIP: {
    skip 'HTTP/2 not available', 18 unless $have_h2;

    my (%r, @log);
    my ($conn, $stream_io, $sock, $server) = h2_conn(app => streaming_app(\%r), log => \@log);

    my (%headers, %data, %closed);
    my $client = h2_client(
        on_header          => sub { my ($s, $n, $v) = @_; $headers{$s}{lc $n} = $v; 0 },
        on_data_chunk_recv => sub { my ($s, $d) = @_; $data{$s} .= $d; 0 },
        on_stream_close    => sub { my ($s, $c) = @_; $closed{$s} = $c; 0 },
    );
    h2_handshake($client, $sock);

    my $sid = $client->submit_request(
        method => 'POST', path => '/upload', scheme => 'http', authority => 'localhost',
        headers => [['content-type', 'application/octet-stream']],
        body    => sub { return undef },          # undeclared length, kept open
    );
    $sock->syswrite($client->mem_send);
    h2_exchange($client, $sock, 20);
    is($r{started}, 1, 'h2 http: the response started before the overrun');

    $client->submit_data($sid, ('X' x (2 * $LIMIT)), 0);
    $sock->syswrite($client->mem_send);
    h2_exchange($client, $sock, 60);

    is($r{reason}, 'body_too_large', 'h2 http: the object reports body_too_large');
    is($r{detail}, $DETAIL,          'h2 http: the detail names the limit and the truncation');
    is($r{future}, 1,                'h2 http: disconnect_future resolved');
    is(scalar(@{$r{disconnects} // []}), 1, 'h2 http: on_disconnect fired once');
    is($r{disconnects}[0], ['body_too_large', $DETAIL],
        'h2 http: on_disconnect carries the same reason and detail as the object');
    is($r{completes} // 0, 0,        'h2 http: on_complete never fired');

    assert_one_log(\@log, "HTTP/2 stream $sid", 'h2 http');

    is($headers{$sid}{':status'}, '200', 'h2 http: the client got the 200, not a 413');
    is($data{$sid}, "progress 1\n",      'h2 http: the started chunk was delivered');
    is($closed{$sid}, 2,                 'h2 http: the stream ended with RST_STREAM INTERNAL_ERROR');

    # The truncation is per-stream: the connection keeps serving.
    ok(!$conn->{closed}, 'h2 http: the connection survived the reset');
    my $sid2 = $client->submit_request(
        method => 'GET', path => '/ok', scheme => 'http', authority => 'localhost',
        headers => []);
    $sock->syswrite($client->mem_send);
    h2_exchange($client, $sock, 30);
    is($headers{$sid2}{':status'}, '200',
        'h2 http: a second stream on the same connection still gets a response');
    is($data{$sid2}, 'second', 'h2 http: the second stream got its body');
    assert_no_warnings('h2 http');

    $stream_io->close_now;
    $loop->remove($server);
}

# ============================================================
# 2. HTTP/2: the same overrun lands while a receive is pending
# ============================================================

SKIP: {
    skip 'HTTP/2 not available', 10 unless $have_h2;

    my (%r, @log);
    my ($conn, $stream_io, $sock, $server)
        = h2_conn(app => streaming_app(\%r, read => 1), log => \@log);

    my $client = h2_client();
    h2_handshake($client, $sock);
    my $sid = $client->submit_request(
        method => 'POST', path => '/upload', scheme => 'http', authority => 'localhost',
        headers => [['content-type', 'application/octet-stream']],
        body    => sub { return undef },
    );
    $sock->syswrite($client->mem_send);
    h2_exchange($client, $sock, 20);
    is($r{started}, 1, 'h2 pending receive: the response started before the overrun');

    $client->submit_data($sid, ('X' x (2 * $LIMIT)), 0);
    $sock->syswrite($client->mem_send);
    h2_exchange($client, $sock, 60);

    my $last = ($r{events} // [])->[-1];
    is($last->{type}, 'http.disconnect',
        'h2 pending receive: the parked receive resolved with the scope disconnect event');
    # Www.pod "Agreement with disconnect events": http.disconnect carries no
    # reason of its own, so the token lives on the object alone.
    ok(!exists $last->{reason}, 'h2 pending receive: http.disconnect carries no reason');
    is($r{reason}, 'body_too_large', 'h2 pending receive: the object reports body_too_large');
    is($r{detail}, $DETAIL,          'h2 pending receive: the detail is the same sentence');

    assert_one_log(\@log, "HTTP/2 stream $sid", 'h2 pending receive');
    assert_no_warnings('h2 pending receive');

    $stream_io->close_now;
    $loop->remove($server);
}

# ============================================================
# 3. sse scope, both transports
# ============================================================

SKIP: {
    skip 'HTTP/2 not available', 13 unless $have_h2;

    my (%r, @log);
    my ($conn, $stream_io, $sock, $server) = h2_conn(app => sse_app(\%r), log => \@log);

    my (%headers, $data, $closed);
    $data = '';
    my $client = h2_client(
        on_header          => sub { my (undef, $n, $v) = @_; $headers{lc $n} = $v; 0 },
        on_data_chunk_recv => sub { $data .= $_[1]; 0 },
        on_stream_close    => sub { $closed = $_[1]; 0 },
    );
    h2_handshake($client, $sock);
    my $sid = $client->submit_request(
        method => 'GET', path => '/events', scheme => 'http', authority => 'localhost',
        headers => [['accept', 'text/event-stream']],
        body    => sub { return undef },
    );
    $sock->syswrite($client->mem_send);
    h2_exchange($client, $sock, 20);
    is($r{started}, 1, 'h2 sse: the stream started before the overrun');

    $client->submit_data($sid, ('X' x (2 * $LIMIT)), 0);
    $sock->syswrite($client->mem_send);
    h2_exchange($client, $sock, 60);

    is($r{reason}, 'body_too_large', 'h2 sse: the object reports body_too_large');
    is($r{detail}, $DETAIL,          'h2 sse: the detail is the same sentence');
    is(scalar(@{$r{disconnects} // []}), 1, 'h2 sse: on_disconnect fired once');
    is($r{completes} // 0, 0,        'h2 sse: on_complete never fired');
    assert_one_log(\@log, "HTTP/2 stream $sid", 'h2 sse');
    is($headers{':status'}, '200', 'h2 sse: the client kept the 200, not a 413');
    like($data, qr/data: hello/,   'h2 sse: the started event was delivered');
    is($closed, 2,                 'h2 sse: the stream ended with RST_STREAM INTERNAL_ERROR');
    assert_no_warnings('h2 sse');

    $stream_io->close_now;
    $loop->remove($server);
}

subtest 'HTTP/1.1 sse: the overrun ends the scope with body_too_large' => sub {
    my (%r, @log);
    my $server = h1_server(app => sse_app(\%r, read => 1), log => \@log);
    my ($sock, $pump, $wire, $eof) = h1_open($server->port,
        "GET /events HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n"
      . "Transfer-Encoding: chunked\r\n\r\n");
    $pump->(30);
    is($r{started}, 1, 'the stream started before the overrun');

    syswrite($sock, chunk('X' x (2 * $LIMIT)));
    $pump->(40);

    is($r{reason}, 'body_too_large', 'the object reports body_too_large');
    is($r{detail}, $DETAIL,          'the detail is the same sentence');
    my $last = ($r{events} // [])->[-1];
    is($last->{type}, 'sse.disconnect', 'the parked receive resolved with sse.disconnect');
    is($last->{reason}, 'body_too_large',
        'sse.disconnect carries the same token as the object');
    is($r{completes} // 0, 0, 'on_complete never fired');
    assert_one_log(\@log, 'HTTP/1.1', 'h1 sse');

    like($$wire, qr{^HTTP/1\.1 200 }, 'the client kept the 200, not a 413');
    like($$wire, qr/data: hello/,     'the started event was delivered');
    unlike($$wire, qr/413/,           'no 413 was written over the started response');
    is($$eof, 1, 'the socket was closed');
    assert_h1_teardown_warning('h1 sse');

    close $sock;
    $loop->remove($server);
};

# ============================================================
# 4. HTTP/1.1: a plain http streaming response
# ============================================================
# HTTP/1.1 parses a chunked request body only from inside $receive, so an
# application that never reads never reaches the limit at all; the reading
# shape is the only one on this transport, and it is also case 2's shape.

subtest 'HTTP/1.1 http: the overrun truncates the started response' => sub {
    my (%r, @log);
    my $server = h1_server(app => streaming_app(\%r, read => 1), log => \@log);
    my ($sock, $pump, $wire, $eof) = h1_open($server->port,
        "POST /upload HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n");
    $pump->(30);
    is($r{started}, 1, 'the response started before the overrun');

    syswrite($sock, chunk('X' x (2 * $LIMIT)));
    $pump->(40);

    is($r{reason}, 'body_too_large', 'the object reports body_too_large');
    is($r{detail}, $DETAIL,          'the detail names the limit and the truncation');
    is($r{future}, 1,                'disconnect_future resolved');
    is(scalar(@{$r{disconnects} // []}), 1, 'on_disconnect fired once');
    is($r{disconnects}[0], ['body_too_large', $DETAIL],
        'on_disconnect carries the same reason and detail as the object');
    is($r{completes} // 0, 0, 'on_complete never fired');
    is(($r{events} // [])->[-1]{type}, 'http.disconnect',
        'the parked receive resolved with the scope disconnect event');
    assert_one_log(\@log, 'HTTP/1.1', 'h1 http');

    like($$wire, qr{^HTTP/1\.1 200 }, 'the client got the 200, not a 413');
    like($$wire, qr/progress 1/,      'the started chunk was delivered');
    unlike($$wire, qr/413/,           'no 413 was written over the started response');
    # A chunked response the server truncates never gets its terminator, which
    # is how the client sees the response as incomplete rather than as short.
    unlike($$wire, qr/\r\n0\r\n\r\n\z/, 'the chunked terminator was never written');
    is($$eof, 1, 'the socket was closed');
    assert_h1_teardown_warning('h1 http');

    close $sock;
    $loop->remove($server);
};

# ============================================================
# 5. Control: no response started -> 413, exactly as before
# ============================================================

SKIP: {
    skip 'HTTP/2 not available', 5 unless $have_h2;

    my (%r, @log);
    my ($conn, $stream_io, $sock, $server) = h2_conn(app => silent_app(\%r), log => \@log);

    my %headers;
    my $client = h2_client(on_header => sub { my (undef, $n, $v) = @_; $headers{lc $n} = $v; 0 });
    h2_handshake($client, $sock);
    my $sid = $client->submit_request(
        method => 'POST', path => '/upload', scheme => 'http', authority => 'localhost',
        headers => [['content-type', 'application/octet-stream']],
        body    => sub { return undef },
    );
    $sock->syswrite($client->mem_send);
    h2_exchange($client, $sock, 20);
    $client->submit_data($sid, ('X' x (2 * $LIMIT)), 0);
    $sock->syswrite($client->mem_send);
    h2_exchange($client, $sock, 60);

    is($headers{':status'}, '413', 'h2 control: an overrun before any response is still a 413');
    is($r{reason}, 'body_too_large', 'h2 control: the object still reports body_too_large');
    is($r{detail}, "request body exceeded $LIMIT bytes",
        'h2 control: the pre-response detail is unchanged');
    is(scalar(@log), 0, 'h2 control: the 413 path logs nothing, as before');
    assert_no_warnings('h2 control');

    $stream_io->close_now;
    $loop->remove($server);
}

subtest 'HTTP/1.1 control: no response started -> 413' => sub {
    my (%r, @log);
    my $server = h1_server(app => silent_app(\%r), log => \@log);
    my ($sock, $pump, $wire, $eof) = h1_open($server->port,
        "POST /upload HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n");
    $pump->(20);
    syswrite($sock, chunk('X' x (2 * $LIMIT)));
    $pump->(40);

    like($$wire, qr{^HTTP/1\.1 413 }, 'an overrun before any response is still a 413');
    is($r{reason}, 'body_too_large', 'the object still reports body_too_large');
    is($r{detail}, "request body exceeded $LIMIT bytes",
        'the pre-response detail is unchanged');
    is(scalar(@log), 0, 'the 413 path logs nothing, as before');
    assert_h1_teardown_warning('h1 control');

    close $sock;
    $loop->remove($server);
};

alarm 0;
done_testing;
