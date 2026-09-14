use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Future::AsyncAwait;
use MIME::Base64 ();
use File::Temp ();
use FindBin;
use lib "$FindBin::Bin/../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Test: refusing a handshake or a stream is an ordinary HTTP response
# ============================================================
# PAGI::Spec::Www, "Refusing the handshake" and "Refusing the stream": until
# the application sends websocket.accept or sse.start, the scope is an
# ordinary HTTP exchange and the application MAY answer it with
# http.response.start / .body / .trailers, "with exactly the semantics those
# events have on an http scope". On the wire the refusal's status, headers and
# body are identical to the same response on an http scope, apart from the
# connection-lifecycle headers the server owns.
#
# Every case below runs on both transports and both scope types. The http
# scope is the reference: the same application events are replayed against a
# plain request and the two wires are compared.

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

# ============================================================
# HTTP/1.1 harness (shape borrowed from t/69)
# ============================================================

sub create_server {
    my ($app, %opts) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1,
        shutdown_timeout => 1, %opts,
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

# Open a socket, send $request, and pump the loop, accumulating everything the
# server writes. Returns the still-open socket and a reader closure so a test
# can inspect the wire part-way through.
sub h1_open {
    my ($port, $request) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5)
        or die "connect: $!";
    print $sock $request;
    $sock->blocking(0);
    my $wire = '';
    my $eof  = 0;
    # $done is what the pump is waiting for. Once it holds the pump runs on
    # for another 20 turns -- a second of settling, so an assertion that
    # nothing further arrives is made against a quiet loop -- and then stops,
    # rather than spending the whole round count on a condition already met.
    my $pump = sub {
        my ($rounds, $done) = @_;
        my $after = 0;
        for (1 .. ($rounds // 20)) {
            $loop->loop_once(0.05);
            my $buf;
            my $n = sysread($sock, $buf, 65536);
            if (defined $n) { $n ? ($wire .= $buf) : ($eof = 1) }
            last if $done && $done->() && ++$after > 20;
        }
        return $wire;
    };
    return ($sock, $pump, \$wire, \$eof);
}

sub h1_fetch {
    my ($port, $request, $rounds, $done) = @_;
    my ($sock, $pump, $wire, $eof) = h1_open($port, $request);
    # A fetch ends when the server has closed the connection, unless the case
    # is waiting for something else of its own.
    $pump->($rounds // 30, $done // sub { $$eof });
    close $sock;
    return ($$wire, $$eof);
}

# Split an h1 wire into (status line, [header lines], body).
sub h1_parse {
    my ($wire) = @_;
    my ($head, $body) = split /\r\n\r\n/, $wire, 2;
    $head //= ''; $body //= '';
    my @lines = split /\r\n/, $head;
    my $status = shift(@lines) // '';
    return ($status, \@lines, $body);
}

# The comparable part of a response: everything but the headers whose value the
# connection lifecycle owns (the spec's stated exception) and the Date header,
# whose value is a clock reading.
sub comparable_headers {
    my ($lines) = @_;
    return [sort grep { !/^(?:connection|keep-alive|date|server|transfer-encoding):/i } @$lines];
}

# ============================================================
# HTTP/2 harness (shape borrowed from t/http2/22)
# ============================================================

sub create_h2_connection {
    my (%o) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app    = $o{app} // sub { };
    my $server = $o{server} // do {
        my $s = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0,
            quiet => 1, http2 => 1);
        $loop->add($s);
        $s;
    };
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
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
    for (1 .. ($rounds // 20)) {
        $loop->loop_once(0.05);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
        last if $done && $done->() && ++$after > 20;
    }
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

# One h2 request, driven to completion. Returns headers, body and the
# RST_STREAM/close error code the client observed.
sub h2_fetch {
    my (%a) = @_;
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $a{app});
    my (%headers, $body, $close_code);
    $body = '';
    my $client = create_client(
        on_header          => sub { my (undef, $n, $v) = @_; $headers{lc $n} = $v; 0 },
        on_data_chunk_recv => sub { my (undef, $d) = @_; $body .= $d; 0 },
        on_stream_close    => sub { my (undef, $c) = @_; $close_code = $c; 0 },
    );
    complete_h2_handshake($client, $client_sock);
    h2_submit($client, $client_sock, $a{kind}, $a{path});
    $client_sock->syswrite($client->mem_send);
    # A fetch ends when the client has seen the stream close, unless the case
    # names something else of its own.
    exchange_frames($client, $client_sock, $a{rounds} // 25,
                    $a{done} // sub { defined $close_code });
    $a{after}->($client, $client_sock) if $a{after};
    $stream_io->close_now;
    $loop->remove($server);
    return (\%headers, $body, $close_code);
}

my @SCOPES = qw(websocket sse);

# Refusal statuses: a WebSocket refusal MUST be 300 or above; an SSE refusal
# may use any status. 404 works on both, so the http reference response is the
# same one for each scope.
my $STATUS = 404;

# ============================================================
# (a) complete-body refusal: identical on the wire to an http response
# ============================================================

subtest 'a complete-body refusal is the same response an http scope would send' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->() if $scope->{type} ne 'sse';
        await $send->({ type => 'http.response.start', status => $STATUS,
                        headers => [['content-type', 'text/plain'],
                                    ['x-refusal', 'yes'],
                                    ['content-length', 6]] });
        await $send->({ type => 'http.response.body', body => 'gone.\n' =~ s/\\n/\n/r });
        return;
    };
    my $server = create_server($app);
    my $port   = $server->port;

    my ($ref_wire) = h1_fetch($port, h1_request('http'));
    my ($ref_status, $ref_headers, $ref_body) = h1_parse($ref_wire);
    is($ref_status, "HTTP/1.1 $STATUS Not Found", 'reference http response status');

    for my $kind (@SCOPES) {
        my ($wire) = h1_fetch($port, h1_request($kind));
        my ($status, $headers, $body) = h1_parse($wire);
        is($status, $ref_status, "h1 $kind refusal: same status line");
        is(comparable_headers($headers), comparable_headers($ref_headers),
            "h1 $kind refusal: same headers apart from the ones the connection owns");
        is($body, $ref_body, "h1 $kind refusal: same body");
    }
    $server->shutdown->get;

    SKIP: {
        skip 'HTTP/2 not available', 6 unless $have_h2;
        my ($ref_h, $ref_b) = h2_fetch(app => $app, kind => 'http');
        for my $kind (@SCOPES) {
            my ($h, $b) = h2_fetch(app => $app, kind => $kind);
            is($h->{':status'}, $ref_h->{':status'}, "h2 $kind refusal: same status");
            is($h->{'x-refusal'}, 'yes', "h2 $kind refusal: app header present");
            is($b, $ref_b, "h2 $kind refusal: same body");
        }
    }
};

# ============================================================
# (b) streamed refusal: nothing is buffered
# ============================================================
# "more => 1 streams under the transport's ordinary body, flow-control, and
# framing rules ... Nothing is buffered specially." The app parks between
# chunks on a Future the test resolves, so the first chunk must already be
# readable at the client when the second has not yet been sent.

subtest 'a streamed refusal delivers its first chunk before the terminal one' => sub {
    for my $kind (@SCOPES) {
        my $gate = $loop->new_future;
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->() if $scope->{type} ne 'sse';
            await $send->({ type => 'http.response.start', status => $STATUS,
                            headers => [['content-type', 'text/plain']] });
            await $send->({ type => 'http.response.body', body => 'first', more => 1 });
            await $gate;
            await $send->({ type => 'http.response.body', body => 'last', more => 0 });
            return;
        };
        my $server = create_server($app);
        my ($sock, $pump, $wire) = h1_open($server->port, h1_request($kind));
        $pump->(25, sub { $$wire =~ /\r\n\r\n5\r\nfirst\r\n/ });
        like($$wire, qr/\r\n\r\n5\r\nfirst\r\n/,
            "h1 $kind: the first chunk is on the wire before the terminal one is sent");
        unlike($$wire, qr/last/, "h1 $kind: the terminal chunk has not been sent yet");
        $gate->done;
        $pump->(25, sub { $$wire =~ /0\r\n\r\n\z/ });
        like($$wire, qr/4\r\nlast\r\n0\r\n\r\n\z/,
            "h1 $kind: the terminal chunk and the chunked terminator follow");
        close $sock;
        $server->shutdown->get;
    }

    SKIP: {
        skip 'HTTP/2 not available', 4 unless $have_h2;
        for my $kind (@SCOPES) {
            my $gate = $loop->new_future;
            my $app = async sub {
                my ($scope, $receive, $send) = @_;
                await $receive->() if $scope->{type} ne 'sse';
                await $send->({ type => 'http.response.start', status => $STATUS,
                                headers => [['content-type', 'text/plain']] });
                await $send->({ type => 'http.response.body', body => 'first', more => 1 });
                await $gate;
                await $send->({ type => 'http.response.body', body => 'last', more => 0 });
                return;
            };
            my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
            my $body = '';
            my $client = create_client(
                on_data_chunk_recv => sub { my (undef, $d) = @_; $body .= $d; 0 });
            complete_h2_handshake($client, $client_sock);
            h2_submit($client, $client_sock, $kind);
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 25, sub { $body eq 'first' });
            is($body, 'first', "h2 $kind: the first DATA frame arrived before the terminal chunk");
            $gate->done;
            exchange_frames($client, $client_sock, 25, sub { $body eq 'firstlast' });
            is($body, 'firstlast', "h2 $kind: the terminal chunk follows");
            $stream_io->close_now;
            $loop->remove($server);
        }
    }
};

# ============================================================
# (c) file refusal
# ============================================================

subtest 'a refusal may answer with a file body' => sub {
    my $tmp = File::Temp->new(SUFFIX => '.txt');
    print {$tmp} "from-a-file";
    $tmp->flush;
    my $path = $tmp->filename;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->() if $scope->{type} ne 'sse';
        await $send->({ type => 'http.response.start', status => $STATUS,
                        headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', file => $path });
        return;
    };
    my $server = create_server($app);
    for my $kind (@SCOPES) {
        my ($wire) = h1_fetch($server->port, h1_request($kind));
        like($wire, qr/from-a-file/, "h1 $kind: the file body was delivered");
    }
    $server->shutdown->get;

    SKIP: {
        skip 'HTTP/2 not available', 2 unless $have_h2;
        for my $kind (@SCOPES) {
            my (undef, $body) = h2_fetch(app => $app, kind => $kind);
            like($body, qr/from-a-file/, "h2 $kind: the file body was delivered");
        }
    }
};

# ============================================================
# (d) abandoned refusal: an incomplete response
# ============================================================
# Www.pod "Application Left a Response Incomplete" names the refusal case
# first: the terminal framing is never synthesized, the transport is
# terminated so the client observes truncation, and the object reports
# server_error.

subtest 'a refusal abandoned before its terminal event is an incomplete response' => sub {
    for my $kind (@SCOPES) {
        my %r;
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->() if $scope->{type} ne 'sse';
            my $conn = $scope->{'pagi.connection'};
            $conn->on_disconnect(sub { $r{disconnect} = [@_] });
            $conn->on_complete(sub { $r{complete}++ });
            await $send->({ type => 'http.response.start', status => $STATUS,
                            headers => [['content-type', 'text/plain']] });
            await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
            $r{returned} = 1;
            return;   # no terminal body
        };
        my @warnings;
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };
        my $server = create_server($app);
        my ($wire, $eof) = h1_fetch($server->port, h1_request($kind), 40);
        $server->shutdown->get;

        like($wire, qr/7\r\npartial\r\n/, "h1 $kind: the chunk that was sent reached the client");
        unlike($wire, qr/0\r\n\r\n\z/, "h1 $kind: no chunked terminator was synthesized");
        ok($eof, "h1 $kind: the connection was closed so the truncation is observable");
        is($r{complete}, undef, "h1 $kind: on_complete did not fire");
        is($r{disconnect}[0], 'server_error', "h1 $kind: on_disconnect reported server_error");
        ok((scalar grep { /incomplete response/i } @warnings),
            "h1 $kind: the incomplete response was logged") or diag("warnings: @warnings");
    }

    SKIP: {
        skip 'HTTP/2 not available', 12 unless $have_h2;
        for my $kind (@SCOPES) {
            my %r;
            my $app = async sub {
                my ($scope, $receive, $send) = @_;
                await $receive->() if $scope->{type} ne 'sse';
                my $conn = $scope->{'pagi.connection'};
                $conn->on_disconnect(sub { $r{disconnect} = [@_] });
                $conn->on_complete(sub { $r{complete}++ });
                await $send->({ type => 'http.response.start', status => $STATUS,
                                headers => [['content-type', 'text/plain']] });
                await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
                return;
            };
            my @warnings;
            local $SIG{__WARN__} = sub { push @warnings, $_[0] };
            my (undef, $body, $close_code) = h2_fetch(app => $app, kind => $kind, rounds => 40);
            is($body, 'partial', "h2 $kind: the chunk that was sent reached the client");
            is($close_code, 2, "h2 $kind: the stream was reset with INTERNAL_ERROR");
            is($r{complete}, undef, "h2 $kind: on_complete did not fire");
            is($r{disconnect}[0], 'server_error', "h2 $kind: on_disconnect reported server_error");
            ok((scalar grep { /incomplete|without ending the scope cleanly/i } @warnings),
                "h2 $kind: the incomplete response was logged") or diag("warnings: @warnings");
            ok(defined $r{disconnect}[1], "h2 $kind: disconnect_detail says more than the token");
        }
    }
};

# ============================================================
# (e) abandoned refusal with the client already gone
# ============================================================
# The client-gone carve-out: the request already ended abnormally with its own
# reason, so the server MUST NOT log an incomplete-response error.

subtest 'an abandoned refusal is not logged once the client has already gone' => sub {
    for my $kind (@SCOPES) {
        my %r;
        my $released = $loop->new_future;
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->() if $scope->{type} ne 'sse';
            await $send->({ type => 'http.response.start', status => $STATUS,
                            headers => [['content-type', 'text/plain']] });
            await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
            $r{parked} = 1;
            await $released;         # still running when the client drops
            $r{returned} = 1;
            return;
        };
        my @warnings;
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };
        my $server = create_server($app);
        my ($sock, $pump) = h1_open($server->port, h1_request($kind));
        $pump->(25, sub { $r{parked} });
        close $sock;                 # client gone
        $loop->loop_once(0.05) for 1 .. 10;
        $released->done;
        $loop->loop_once(0.05) for 1 .. 20;
        $server->shutdown->get;

        is($r{returned}, 1, "h1 $kind: the app returned after the client had gone");
        ok(!(scalar grep { /incomplete response/i } @warnings),
            "h1 $kind: no incomplete-response error was logged") or diag("warnings: @warnings");
    }

    SKIP: {
        skip 'HTTP/2 not available', 4 unless $have_h2;
        for my $kind (@SCOPES) {
            my %r;
            my $released = $loop->new_future;
            my $app = async sub {
                my ($scope, $receive, $send) = @_;
                await $receive->() if $scope->{type} ne 'sse';
                await $send->({ type => 'http.response.start', status => $STATUS,
                                headers => [['content-type', 'text/plain']] });
                await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
                $r{parked} = 1;
                await $released;
                $r{returned} = 1;
                return;
            };
            my @warnings;
            local $SIG{__WARN__} = sub { push @warnings, $_[0] };
            my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
            my $client = create_client();
            complete_h2_handshake($client, $client_sock);
            my $sid = h2_submit($client, $client_sock, $kind);
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 25, sub { $r{parked} });
            # The client resets the stream while the app is still parked.
            $client->submit_rst_stream($sid, 8);   # CANCEL
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 15);
            $released->done;
            exchange_frames($client, $client_sock, 20);
            $stream_io->close_now;
            $loop->remove($server);

            is($r{returned}, 1, "h2 $kind: the app returned after the client had gone");
            ok(!(scalar grep { /incomplete|without ending the scope cleanly/i } @warnings),
                "h2 $kind: no incomplete-response error was logged") or diag("warnings: @warnings");
        }
    }
};

# ============================================================
# (f) websocket.close before accept
# ============================================================

# ============================================================
# A WebSocket refusal's status must be 300 or above
# ============================================================
# Www.pod "Refusing the handshake": "A refusal's status MUST be 300 or above. A
# 1xx or 2xx status is not a refusal ... the server MUST fail the $send Future
# without transmitting anything or mutating state, on either transport." The
# fail-don't-mutate half is what makes the accept afterwards succeed, so a
# working socket is the proof that nothing was mutated.

subtest 'a websocket refusal below 300 fails the send and leaves the handshake open' => sub {
    my %r;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();                       # websocket.connect
        $r{err} = do { local $@;
            eval { await $send->({ type => 'http.response.start', status => 200,
                                   headers => [['content-type', 'text/plain']] }) }; $@ };
        $r{started} = $scope->{'pagi.connection'}->response_started;
        # Nothing was mutated, so the handshake is still open to an accept.
        await $send->({ type => 'websocket.accept' });
        my $msg = await $receive->();
        $r{echoed} = $msg->{text};
        await $send->({ type => 'websocket.send', text => "echo:" . ($msg->{text} // '') });
        await $send->({ type => 'websocket.close' });
        return;
    };

    my $server = create_server($app);
    my ($sock, $pump, $wire) = h1_open($server->port, h1_request('websocket'));
    $pump->(20, sub { $$wire =~ m{^HTTP/1\.1 101} });
    like($$wire, qr{^HTTP/1\.1 101}, 'h1: the accept still completed the handshake');
    unlike($$wire, qr{^HTTP/1\.1 200}m, 'h1: the failed start transmitted nothing');
    print $sock Protocol::WebSocket::Frame->new(
        buffer => 'ping-me', type => 'text', masked => 1)->to_bytes;
    $pump->(25, sub { $$wire =~ /echo:ping-me/ });
    close $sock;
    $server->shutdown->get;

    like($r{err}, qr/refusal status must be 300 or above/, 'h1: the send failed on the status rule');
    is($r{started}, 0, 'h1: nothing was mutated -- no response had started');
    is($r{echoed}, 'ping-me', 'h1: the accepted socket carried a real message');
    like($$wire, qr/echo:ping-me/, 'h1: and the reply reached the client');

    SKIP: {
        skip 'HTTP/2 not available', 4 unless $have_h2;
        my %h2;
        my $h2app = async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->();
            $h2{err} = do { local $@;
                eval { await $send->({ type => 'http.response.start', status => 200,
                                       headers => [['content-type', 'text/plain']] }) }; $@ };
            $h2{started} = $scope->{'pagi.connection'}->response_started;
            await $send->({ type => 'websocket.accept' });
            my $msg = await $receive->();
            $h2{echoed} = $msg->{text};
            await $send->({ type => 'websocket.send', text => "echo:" . ($msg->{text} // '') });
            # End the socket cleanly: returning from an accepted socket without
            # a closing handshake is an incomplete response and would log one.
            await $send->({ type => 'websocket.close' });
            return;
        };

        my ($conn, $stream_io, $client_sock, $server2) = create_h2_connection(app => $h2app);
        my (%headers, $data);
        $data = '';
        my $client = create_client(
            on_header          => sub { my (undef, $n, $v) = @_; $headers{lc $n} = $v; 0 },
            on_data_chunk_recv => sub { my (undef, $d) = @_; $data .= $d; 0 },
        );
        complete_h2_handshake($client, $client_sock);
        my $sid = h2_submit($client, $client_sock, 'websocket');
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 25, sub { defined $h2{started} });
        $client->submit_data($sid, Protocol::WebSocket::Frame->new(
            buffer => 'ping-me', type => 'text', masked => 1)->to_bytes, 0);
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 25, sub { $data =~ /echo:ping-me/ });
        $stream_io->close_now;
        $loop->remove($server2);

        like($h2{err}, qr/refusal status must be 300 or above/, 'h2: the send failed on the status rule');
        is($h2{started}, 0, 'h2: nothing was mutated -- no response had started');
        is($h2{echoed}, 'ping-me', 'h2: the accepted stream carried a real message');
        like($data, qr/echo:ping-me/, 'h2: and the reply reached the client');
    }
};

subtest 'websocket.close before accept fails the send and writes nothing' => sub {
    my %r;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        my $conn = $scope->{'pagi.connection'};
        $r{err} = do { local $@; eval { await $send->({ type => 'websocket.close' }) }; $@ };
        $r{started} = $conn->response_started;
        $r{done} = 1;
        return;
    };
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $server = create_server($app);
    my ($wire) = h1_fetch($server->port, h1_request('websocket'), 30);
    $server->shutdown->get;

    like($r{err}, qr/before websocket\.accept/, 'h1: the send failed as out of sequence');
    is($r{started}, 0, 'h1: nothing was mutated -- no response had started');
    unlike($wire, qr{^HTTP/1\.1 403}, 'h1: no 403 was written');
    # The app then produced no response at all, so "Application Produced No
    # Response" applies: the 500 backstop, and one error line naming it.
    like($wire, qr{^HTTP/1\.1 500}, 'h1: the no-response backstop answered instead');
    is(scalar(grep { /without accepting the WebSocket or refusing the handshake/ } @warnings), 1,
        'h1: the backstop logged exactly once') or diag("warnings: @warnings");

    SKIP: {
        skip 'HTTP/2 not available', 4 unless $have_h2;
        my @h2_warnings;
        local $SIG{__WARN__} = sub { push @h2_warnings, $_[0] };
        my %h2;
        my $h2app = async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->();
            my $conn = $scope->{'pagi.connection'};
            $h2{err} = do { local $@; eval { await $send->({ type => 'websocket.close' }) }; $@ };
            $h2{started} = $conn->response_started;
            return;
        };
        my ($headers) = h2_fetch(app => $h2app, kind => 'websocket');
        like($h2{err}, qr/before websocket\.accept/, 'h2: the send failed as out of sequence');
        is($h2{started}, 0, 'h2: nothing was mutated -- no response had started');
        isnt($headers->{':status'}, '403', 'h2: no 403 was written');
        is(scalar(grep { /returned without starting a response/ } @h2_warnings), 1,
            'h2: the backstop logged exactly once') or diag("warnings: @h2_warnings");
    }
};

# ============================================================
# (f2) no accept and no refusal: the no-response backstop
# ============================================================
# Www.pod "Meaning per scope" routes a websocket scope that neither accepts
# nor refuses to "Application Produced No Response": the 500 backstop, one
# error line, and server_error on the object -- but with the client already
# gone the server MUST NOT synthesize a 500 and MUST NOT log an error.
#
# The h1 half of both arms is B4's code, exercised here against the rule
# rather than against the websocket.close path that first needed it. The sse
# scope's own backstop line is pinned by t/69 and is not repeated here.

subtest 'websocket: an app that neither accepts nor refuses gets the 500 backstop, and nothing once the client has gone' => sub {
    my %r;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';   # decline lifespan
        my $conn = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { push @{$r{disconnect}}, [@_] });
        $conn->on_complete(sub { $r{complete}++ });
        await $receive->();                       # websocket.connect
        $r{returned} = 1;
        return;                                   # no accept, no refusal
    };
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $server = create_server($app);
    my ($wire, $eof) = h1_fetch($server->port, h1_request('websocket'), 30);
    $server->shutdown->get;

    like($wire, qr{^HTTP/1\.1 500}, 'h1: the backstop answered 500');
    ok($eof, 'h1: and closed the connection so the client is not left waiting');
    is(scalar(grep { /returned without accepting the WebSocket or refusing the handshake/ } @warnings), 1,
        'h1: the backstop logged exactly once') or diag("warnings: @warnings");
    is($r{disconnect}[0][0], 'server_error', 'h1: the object reports server_error');
    is($r{complete}, undef, 'h1: on_complete did not fire');

    # Client-gone carve-out: the app is parked when the client drops, so by
    # the time it returns the scope has already ended with its own reason.
    my %g;
    my $released = $loop->new_future;
    my $gone_app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';   # decline lifespan
        my $conn = $scope->{'pagi.connection'};
        await $receive->();                       # websocket.connect
        $g{parked} = 1;
        await $released;
        $g{reason} = $conn->disconnect_reason;
        $g{returned} = 1;
        return;                                   # still no accept, no refusal
    };
    my @gone_warnings;
    local $SIG{__WARN__} = sub { push @gone_warnings, $_[0] };
    my $server2 = create_server($gone_app);
    my ($sock, $pump, $gone_wire) = h1_open($server2->port, h1_request('websocket'));
    $pump->(20);
    close $sock;                                  # client gone before the app returns
    $loop->loop_once(0.05) for 1 .. 10;
    $released->done;
    $loop->loop_once(0.05) for 1 .. 20;
    $server2->shutdown->get;

    is($g{returned}, 1, 'h1 client gone: the app returned after the client had dropped');
    is($g{reason}, 'client_closed', 'h1 client gone: the scope kept the reason it already had');
    unlike($$gone_wire, qr{^HTTP/1\.1 500}, 'h1 client gone: no 500 was synthesized');
    is(scalar(grep { /returned without accepting the WebSocket or refusing the handshake/ } @gone_warnings), 0,
        'h1 client gone: nothing was logged') or diag("warnings: @gone_warnings");

    SKIP: {
        skip 'HTTP/2 not available', 7 unless $have_h2;

        my %h2;
        my $h2app = async sub {
            my ($scope, $receive, $send) = @_;
            my $conn = $scope->{'pagi.connection'};
            $conn->on_disconnect(sub { push @{$h2{disconnect}}, [@_] });
            $conn->on_complete(sub { $h2{complete}++ });
            await $receive->();
            return;
        };
        my @h2_warnings;
        local $SIG{__WARN__} = sub { push @h2_warnings, $_[0] };
        my ($headers) = h2_fetch(app => $h2app, kind => 'websocket');
        is($headers->{':status'}, '500', 'h2: the backstop answered 500 on the stream');
        is(scalar(grep { /returned without starting a response \(HTTP\/2 stream/ } @h2_warnings), 1,
            'h2: the backstop logged exactly once') or diag("warnings: @h2_warnings");
        is($h2{disconnect}[0][0], 'server_error', 'h2: the object reports server_error');
        is($h2{complete}, undef, 'h2: on_complete did not fire');

        # The client resets the stream while the app is parked: the scope has
        # already ended, so nothing is synthesized and nothing is logged.
        my %h2g;
        my $h2released = $loop->new_future;
        my $h2gone = async sub {
            my ($scope, $receive, $send) = @_;
            my $conn = $scope->{'pagi.connection'};
            await $receive->();
            $h2g{parked} = 1;
            await $h2released;
            $h2g{reason} = $conn->disconnect_reason;
            $h2g{returned} = 1;
            return;
        };
        my @h2g_warnings;
        local $SIG{__WARN__} = sub { push @h2g_warnings, $_[0] };
        my ($conn2, $stream_io, $client_sock, $server3) = create_h2_connection(app => $h2gone);
        my %seen_headers;
        my $client = create_client(
            on_header => sub { my (undef, $n, $v) = @_; $seen_headers{lc $n} = $v; 0 });
        complete_h2_handshake($client, $client_sock);
        my $sid = h2_submit($client, $client_sock, 'websocket');
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 25, sub { $h2g{parked} });
        $client->submit_rst_stream($sid, 8);      # CANCEL, while the app is parked
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 15);
        $h2released->done;
        exchange_frames($client, $client_sock, 20);
        $stream_io->close_now;
        $loop->remove($server3);

        is($h2g{returned}, 1, 'h2 client gone: the app returned after the reset');
        is($seen_headers{':status'}, undef, 'h2 client gone: no 500 was synthesized');
        is(scalar(grep { /returned without starting a response \(HTTP\/2 stream/ } @h2g_warnings), 0,
            'h2 client gone: nothing was logged') or diag("warnings: @h2g_warnings");
    }
};

# ============================================================
# (f3) abort before accept
# ============================================================
# Www.pod "Connection Object Interface": abort "is valid on every scope,
# before or after websocket.accept ... Before accept or start it ends the
# handshake with no HTTP response: the client observes a failed connection,
# not a status". The server "MUST NOT log an incomplete-response or
# no-response error for an aborted scope", so the backstop above must not
# fire here -- neither its 500 nor its log line.

subtest 'abort before accept ends the handshake with no response and no error line' => sub {
    my %r;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';   # decline lifespan
        my $conn = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { push @{$r{disconnect}}, [@_] });
        $conn->on_complete(sub { $r{complete}++ });
        await $receive->();                       # websocket.connect
        $conn->abort('nope');
        $r{reason}    = $conn->disconnect_reason;
        $r{detail}    = $conn->disconnect_detail;
        $r{started}   = $conn->response_started;
        $r{connected} = $conn->is_connected;
        $r{returned}  = 1;
        return;
    };
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $server = create_server($app);
    my ($wire, $eof) = h1_fetch($server->port, h1_request('websocket'), 30);
    $server->shutdown->get;

    is($r{returned}, 1, 'h1: the app returned after aborting');
    is($wire, '', 'h1: the client observes a failed connection, not a status');
    ok($eof, 'h1: the handshake was ended by closing the transport');
    is($r{reason}, 'app_abort', 'h1: disconnect_reason is app_abort');
    is($r{detail}, 'nope', 'h1: disconnect_detail carries the application string');
    is($r{started}, 0, 'h1: no response was started');
    is($r{connected}, 0, 'h1: is_connected is false');
    is(scalar @{$r{disconnect} // []}, 1, 'h1: on_disconnect fired exactly once');
    is($r{complete}, undef, 'h1: on_complete did not fire');
    is(scalar(@warnings), 0, 'h1: an aborted scope logs no error at all')
        or diag("warnings: @warnings");

    SKIP: {
        skip 'HTTP/2 not available', 7 unless $have_h2;
        my %h2;
        my $h2app = async sub {
            my ($scope, $receive, $send) = @_;
            my $conn = $scope->{'pagi.connection'};
            $conn->on_disconnect(sub { push @{$h2{disconnect}}, [@_] });
            $conn->on_complete(sub { $h2{complete}++ });
            await $receive->();
            $conn->abort('nope');
            $h2{reason} = $conn->disconnect_reason;
            $h2{detail} = $conn->disconnect_detail;
            $h2{returned} = 1;
            return;
        };
        my @h2_warnings;
        local $SIG{__WARN__} = sub { push @h2_warnings, $_[0] };
        my ($headers, $body, $close_code) = h2_fetch(app => $h2app, kind => 'websocket');

        is($h2{returned}, 1, 'h2: the app returned after aborting');
        is($close_code, 8, 'h2: the stream was reset with CANCEL, not INTERNAL_ERROR');
        is($headers->{':status'}, undef, 'h2: no status reached the client');
        is($h2{reason}, 'app_abort', 'h2: disconnect_reason is app_abort');
        is($h2{detail}, 'nope', 'h2: disconnect_detail carries the application string');
        is($h2{complete}, undef, 'h2: on_complete did not fire');
        is(scalar(@h2_warnings), 0, 'h2: an aborted scope logs no error at all')
            or diag("warnings: @h2_warnings");
    }
};

# ============================================================
# (g) HTTP response events after the scope has been established
# ============================================================

subtest 'http.response.start after accept or after sse.start fails, leaving the scope intact' => sub {
    my %r;
    my $ws_app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();                       # websocket.connect
        await $send->({ type => 'websocket.accept' });
        $r{ws_err} = do { local $@;
            eval { await $send->({ type => 'http.response.start', status => 500 }) }; $@ };
        await $send->({ type => 'websocket.send', text => 'still-here' });
        await $send->({ type => 'websocket.close' });
        return;
    };
    my $server = create_server($ws_app);
    my ($wire) = h1_fetch($server->port, h1_request('websocket'), 40);
    $server->shutdown->get;
    like($r{ws_err}, qr/after websocket\.accept/, 'h1 websocket: the send failed');
    like($wire, qr{^HTTP/1\.1 101}, 'h1 websocket: the established socket is intact');
    like($wire, qr/still-here/, 'h1 websocket: and still carries application frames');

    my $sse_app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.start' });
        $r{sse_err} = do { local $@;
            eval { await $send->({ type => 'http.response.start', status => 500 }) }; $@ };
        await $send->({ type => 'sse.send', data => 'still-here' });
        await $send->({ type => 'sse.close' });
        return;
    };
    my $server2 = create_server($sse_app);
    my ($wire2) = h1_fetch($server2->port, h1_request('sse'), 40);
    $server2->shutdown->get;
    like($r{sse_err}, qr/after sse\.start/, 'h1 sse: the send failed');
    like($wire2, qr{^HTTP/1\.1 200}, 'h1 sse: the started stream is intact');
    like($wire2, qr/data: still-here/, 'h1 sse: and still carries events');

    SKIP: {
        skip 'HTTP/2 not available', 6 unless $have_h2;
        my %h2;
        my $h2_ws_app = async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->();
            await $send->({ type => 'websocket.accept' });
            $h2{ws_err} = do { local $@;
                eval { await $send->({ type => 'http.response.start', status => 500 }) }; $@ };
            await $send->({ type => 'websocket.send', text => 'still-here' });
            await $send->({ type => 'websocket.close' });
            return;
        };
        my ($h, $b) = h2_fetch(app => $h2_ws_app, kind => 'websocket');
        like($h2{ws_err}, qr/after websocket\.accept/, 'h2 websocket: the send failed');
        is($h->{':status'}, '200', 'h2 websocket: the established stream is intact');
        like($b, qr/still-here/, 'h2 websocket: and still carries application frames');

        my $h2_sse_app = async sub {
            my ($scope, $receive, $send) = @_;
            await $send->({ type => 'sse.start' });
            $h2{sse_err} = do { local $@;
                eval { await $send->({ type => 'http.response.start', status => 500 }) }; $@ };
            await $send->({ type => 'sse.send', data => 'still-here' });
            await $send->({ type => 'sse.close' });
            return;
        };
        my ($h2h, $h2b) = h2_fetch(app => $h2_sse_app, kind => 'sse');
        like($h2{sse_err}, qr/after sse\.start/, 'h2 sse: the send failed');
        is($h2h->{':status'}, '200', 'h2 sse: the started stream is intact');
        like($h2b, qr/data: still-here/, 'h2 sse: and still carries events');
    }
};

# ============================================================
# (h) a completed refusal is a clean end
# ============================================================
# Www.pod "Receiving after the scope's end": a receive made after a clean end
# the application produced reports that end -- http.disconnect on a websocket
# scope, whose refusal was an HTTP exchange, and a reasonless sse.disconnect
# on an sse one. It never invents a reason: the object was marked complete
# with none, and "Agreement with disconnect events" binds the two.

my %END_EVENT = (websocket => { type => 'http.disconnect' },
                 sse       => { type => 'sse.disconnect' });

subtest 'a completed refusal is a clean end, and a later receive reports it' => sub {
    for my $kind (@SCOPES) {
        my %r;
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->() if $scope->{type} ne 'sse';
            my $conn = $scope->{'pagi.connection'};
            $conn->on_disconnect(sub { $r{disconnect} = [@_] });
            $conn->on_complete(sub { $r{complete}++ });
            await $send->({ type => 'http.response.start', status => $STATUS,
                            headers => [['content-type', 'text/plain'], ['content-length', 4]] });
            await $send->({ type => 'http.response.body', body => 'nope' });
            my $after = $receive->();
            await Future->wait_any($after->without_cancel, $loop->delay_future(after => 0.4));
            $r{after_event} = $after->is_ready ? $after->get : undef;
            # A receive that does not answer outlives this sub; hand it to the
            # caller so the suspended async sub behind it is not reaped
            # mid-run, and the case fails on its own assertion rather than on
            # harness noise.
            $r{unanswered} = $after unless $after->is_ready;
            # Read from out here: these assertions are about the state that
            # survives the application's return. Subtest (i) pins the
            # earlier moment, when the terminal event was accepted.
            $r{conn} = $conn;
            $r{done} = 1;
            return;
        };
        my $server = create_server($app);
        my ($wire, $eof) = h1_fetch($server->port, h1_request($kind), 40, sub { $r{done} });
        $loop->loop_once(0.05) for 1 .. 10;
        $server->shutdown->get;

        is($r{done}, 1, "h1 $kind: the app ran to completion");
        is($r{after_event}, $END_EVENT{$kind},
            "h1 $kind: a receive() after the refusal reports the scope's end, with no reason");
        is($r{conn}->response_complete, 1, "h1 $kind: response_complete is true");
        is($r{conn}->disconnect_reason, undef, "h1 $kind: disconnect_reason is undef");
        is($r{complete}, 1, "h1 $kind: on_complete fired exactly once");
        is($r{disconnect}, undef, "h1 $kind: on_disconnect never fired");
    }

    SKIP: {
        skip 'HTTP/2 not available', 12 unless $have_h2;
        for my $kind (@SCOPES) {
            my %r;
            my $app = async sub {
                my ($scope, $receive, $send) = @_;
                await $receive->() if $scope->{type} ne 'sse';
                my $conn = $scope->{'pagi.connection'};
                $conn->on_disconnect(sub { $r{disconnect} = [@_] });
                $conn->on_complete(sub { $r{complete}++ });
                await $send->({ type => 'http.response.start', status => $STATUS,
                                headers => [['content-type', 'text/plain']] });
                await $send->({ type => 'http.response.body', body => 'nope' });
                my $after = $receive->();
                await Future->wait_any($after->without_cancel, $loop->delay_future(after => 0.4));
                $r{after_event} = $after->is_ready ? $after->get : undef;
                $r{unanswered} = $after unless $after->is_ready;
                $r{conn} = $conn;
                $r{done} = 1;
                return;
            };
            h2_fetch(app => $app, kind => $kind, rounds => 40, done => sub { $r{done} });
            is($r{done}, 1, "h2 $kind: the app ran to completion");
            is($r{after_event}, $END_EVENT{$kind},
                "h2 $kind: a receive() after the refusal reports the scope's end, with no reason");
            is($r{conn}->response_complete, 1, "h2 $kind: response_complete is true");
            is($r{conn}->disconnect_reason, undef, "h2 $kind: disconnect_reason is undef");
            is($r{complete}, 1, "h2 $kind: on_complete fired exactly once");
            is($r{disconnect}, undef, "h2 $kind: on_disconnect never fired");
        }
    }
};

# ============================================================
# (i) the refusal's terminal event is the moment the scope ends
# ============================================================
# Www.pod "Meaning per scope": a protocol scope ends cleanly when "the server
# has finished its output of a refusal", and "the first to occur wins; the
# terminal state never reopens". That moment is the refusal's terminal event,
# not the application's return: an app still running after it must already see
# a completed object, and a client that goes away in between must not be able
# to re-decide the ending.

subtest 'a completed refusal is complete before the application returns, and a client close afterwards leaves it complete' => sub {
    # The application: refuse, snapshot the object while still running, then
    # park on a Future the test resolves. It never calls receive() after the
    # refusal, so nothing but the terminal event itself can have ended it.
    my $refusing_app = sub {
        my ($r, $released) = @_;
        return async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->() if $scope->{type} ne 'sse';
            my $conn = $scope->{'pagi.connection'};
            $conn->on_disconnect(sub { push @{$r->{disconnect}}, [@_] });
            $conn->on_complete(sub { $r->{complete}++ });
            await $send->({ type => 'http.response.start', status => 401,
                            headers => [['content-type', 'text/plain'],
                                        ['content-length', 4]] });
            await $send->({ type => 'http.response.body', body => 'nope' });
            $r->{at_terminal} = {
                connected  => $conn->is_connected,
                complete   => $conn->response_complete,
                fired      => $r->{complete},
                disconnects=> scalar @{$r->{disconnect} // []},
            };
            $r->{conn} = $conn;
            await $released;
            $r->{returned} = 1;
            return;
        };
    };

    for my $kind (@SCOPES) {
        my %r;
        my $released = $loop->new_future;
        my @warnings;
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };
        my $server = create_server($refusing_app->(\%r, $released));
        my ($sock, $pump) = h1_open($server->port, h1_request($kind));
        $pump->(25, sub { $r{at_terminal} });

        # Still parked: the terminal event alone drove the object terminal.
        is($r{returned}, undef, "h1 $kind: the application has not returned yet");
        is($r{at_terminal}{connected}, 0, "h1 $kind: is_connected is false at the terminal event");
        is($r{at_terminal}{complete}, 1, "h1 $kind: response_complete is true at the terminal event");
        is($r{at_terminal}{fired}, 1, "h1 $kind: on_complete had fired exactly once by then");
        is($r{at_terminal}{disconnects}, 0, "h1 $kind: on_disconnect had not fired");

        # The client goes away after the refusal but before the app returns:
        # the terminal state must not reopen.
        close $sock;
        $loop->loop_once(0.05) for 1 .. 15;
        is($r{conn}->response_complete, 1, "h1 $kind: still complete after the client closed");
        is($r{conn}->disconnect_reason, undef, "h1 $kind: disconnect_reason stayed undef");
        is(scalar @{$r{disconnect} // []}, 0, "h1 $kind: on_disconnect never fired");

        $released->done;
        $loop->loop_once(0.05) for 1 .. 20;
        $server->shutdown->get;
        is($r{returned}, 1, "h1 $kind: the application returned");
        is(scalar(@warnings), 0, "h1 $kind: nothing was logged") or diag("warnings: @warnings");
    }

    SKIP: {
        skip 'HTTP/2 not available', 40 unless $have_h2;
        for my $kind (@SCOPES) {
            # Two ways for the h2 client to go away after the terminal event:
            # a reset of this stream, and the whole connection dropping.
            for my $ending (qw(rst_stream connection_close)) {
                my %r;
                my $released = $loop->new_future;
                my @warnings;
                local $SIG{__WARN__} = sub { push @warnings, $_[0] };
                my ($conn, $stream_io, $client_sock, $server) =
                    create_h2_connection(app => $refusing_app->(\%r, $released));
                my $client = create_client;
                complete_h2_handshake($client, $client_sock);
                my $sid = h2_submit($client, $client_sock, $kind);
                $client_sock->syswrite($client->mem_send);
                exchange_frames($client, $client_sock, 25, sub { $r{at_terminal} });

                is($r{returned}, undef, "h2 $kind/$ending: the application has not returned yet");
                is($r{at_terminal}{connected}, 0, "h2 $kind/$ending: is_connected is false at the terminal event");
                is($r{at_terminal}{complete}, 1, "h2 $kind/$ending: response_complete is true at the terminal event");
                is($r{at_terminal}{fired}, 1, "h2 $kind/$ending: on_complete had fired exactly once by then");
                is($r{at_terminal}{disconnects}, 0, "h2 $kind/$ending: on_disconnect had not fired");

                my $closed_connection = 0;
                if ($ending eq 'rst_stream') {
                    $client->submit_rst_stream($sid, 8);   # CANCEL
                    $client_sock->syswrite($client->mem_send);
                    exchange_frames($client, $client_sock, 15);
                }
                else {
                    $stream_io->close_now;
                    $closed_connection = 1;
                    $loop->loop_once(0.05) for 1 .. 15;
                }

                is($r{conn}->response_complete, 1, "h2 $kind/$ending: still complete after the client went away");
                is($r{conn}->disconnect_reason, undef, "h2 $kind/$ending: disconnect_reason stayed undef");
                is(scalar @{$r{disconnect} // []}, 0, "h2 $kind/$ending: on_disconnect never fired");

                $released->done;
                if ($closed_connection) { $loop->loop_once(0.05) for 1 .. 20 }
                else { exchange_frames($client, $client_sock, 20) }
                is($r{returned}, 1, "h2 $kind/$ending: the application returned");
                is(scalar(@warnings), 0, "h2 $kind/$ending: nothing was logged") or diag("warnings: @warnings");

                $stream_io->close_now unless $closed_connection;
                $loop->remove($server);
            }
        }
    }
};

# ============================================================
# (j) a refusal terminated by http.response.trailers
# ============================================================
# "with exactly the semantics those events have on an http scope" covers
# http.response.trailers: a refusal whose start declares trailers is finished
# by the trailer section, not by the terminal body chunk. Www.pod "Receiving
# after the scope's end" then reads the same as it does in (h) -- the receive
# after the completed refusal reports the scope's end and invents no reason.

sub trailers_refusal_app {
    my ($r) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->() if $scope->{type} ne 'sse';
        my $conn = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { $r->{disconnect} = [@_] });
        $conn->on_complete(sub { $r->{complete}++ });
        # No content-length: HTTP/1.1 then frames the refusal chunked, the one
        # framing a trailer section may ride (RFC 9112 section 7.1.2).
        await $send->({ type => 'http.response.start', status => $STATUS,
                        trailers => 1,
                        headers  => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'nope' });
        await $send->({ type => 'http.response.trailers',
                        headers => [['x-refusal-trailer', 'yes']] });
        my $after = $receive->();
        await Future->wait_any($after->without_cancel, $loop->delay_future(after => 0.4));
        $r->{after_event} = $after->is_ready ? $after->get : undef;
        # As in (h): a receive that does not answer outlives this sub, so it is
        # handed to the caller rather than reaped mid-run.
        $r->{unanswered} = $after unless $after->is_ready;
        $r->{conn} = $conn;
        $r->{done} = 1;
        return;
    };
}

subtest 'a refusal terminated by http.response.trailers is a clean end' => sub {
    for my $kind (@SCOPES) {
        my %r;
        my $server = create_server(trailers_refusal_app(\%r));
        my ($wire) = h1_fetch($server->port, h1_request($kind), 40, sub { $r{done} });
        $loop->loop_once(0.05) for 1 .. 10;
        $server->shutdown->get;

        is($r{done}, 1, "h1 $kind: the app ran to completion");
        like($wire, qr/^transfer-encoding:\s*chunked\r$/mi,
            "h1 $kind: the refusal is chunked-framed");
        like($wire, qr/\r\n4\r\nnope\r\n0\r\nx-refusal-trailer: yes\r\n\r\n/,
            "h1 $kind: the trailer section, not the body chunk, terminates the refusal");
        is($r{after_event}, $END_EVENT{$kind},
            "h1 $kind: a receive() after the trailers reports the scope's end, with no reason");
        is($r{conn}->response_complete, 1, "h1 $kind: response_complete is true");
        is($r{conn}->disconnect_reason, undef, "h1 $kind: disconnect_reason is undef");
        is($r{complete}, 1, "h1 $kind: on_complete fired exactly once");
        is($r{disconnect}, undef, "h1 $kind: on_disconnect never fired");
    }

    SKIP: {
        skip 'HTTP/2 not available', 14 unless $have_h2;
        for my $kind (@SCOPES) {
            my %r;
            my ($h, $body) = h2_fetch(app => trailers_refusal_app(\%r), kind => $kind,
                                      rounds => 40, done => sub { $r{done} });
            is($r{done}, 1, "h2 $kind: the app ran to completion");
            is($body, 'nope', "h2 $kind: the refusal body arrived");
            is($h->{'x-refusal-trailer'}, 'yes',
                "h2 $kind: the trailing HEADERS carried the trailer");
            is($r{after_event}, $END_EVENT{$kind},
                "h2 $kind: a receive() after the trailers reports the scope's end, with no reason");
            is($r{conn}->response_complete, 1, "h2 $kind: response_complete is true");
            is($r{conn}->disconnect_reason, undef, "h2 $kind: disconnect_reason is undef");
            is($r{complete}, 1, "h2 $kind: on_complete fired exactly once");
            is($r{disconnect}, undef, "h2 $kind: on_disconnect never fired");
        }
    }
};

# ============================================================
# Step 8: the connection closes after a refusal (D-A-I10)
# ============================================================

subtest 'a completed h1 refusal says Connection: close and closes the socket' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->() if $scope->{type} ne 'sse';
        await $send->({ type => 'http.response.start', status => $STATUS,
                        headers => [['content-type', 'text/plain'], ['content-length', 4]] });
        await $send->({ type => 'http.response.body', body => 'nope' });
        return;
    };
    my $server = create_server($app);
    for my $kind (@SCOPES) {
        my ($sock, $pump, $wire, $eof) = h1_open($server->port, h1_request($kind));
        $pump->(25, sub { $$wire =~ /^connection:\s*close\r$/mi });
        like($$wire, qr/^connection:\s*close\r$/mi,
            "h1 $kind: the refusal carries Connection: close");
        # A pipelined follow-up request must not be served on this socket.
        print $sock h1_request('http', '/second');
        $pump->(25, sub { $$eof });
        ok($$eof, "h1 $kind: the socket reached EOF");
        my @statuses = ($$wire =~ m{^HTTP/1\.1 (\d+)}mg);
        is(scalar(@statuses), 1, "h1 $kind: exactly one response was served, not a pipelined second");
        close $sock;
    }
    $server->shutdown->get;
};

subtest 'a completed h2 refusal ends its stream and leaves the connection usable' => sub {
    skip_all 'HTTP/2 not available' unless $have_h2;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->() if $scope->{type} ne 'sse';
        await $send->({ type => 'http.response.start', status => $STATUS,
                        headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => $scope->{type} });
        return;
    };

    for my $kind (@SCOPES) {
        my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
        my (%closed, $body);
        $body = '';
        my $client = create_client(
            on_data_chunk_recv => sub { my (undef, $d) = @_; $body .= $d; 0 },
            on_stream_close    => sub { my ($sid, $c) = @_; $closed{$sid} = $c; 0 },
        );
        complete_h2_handshake($client, $client_sock);
        my $refusal_id = h2_submit($client, $client_sock, $kind);
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 25, sub { length $body });
        # No error code: the refusal ended the server's half of the stream
        # normally. A websocket refusal answers an extended CONNECT whose
        # request body this client deliberately leaves open, so nghttp2
        # reports no client-side stream close at all for it; an sse refusal
        # reports 0. Either way, nothing abnormal.
        ok(!$closed{$refusal_id}, "h2 $kind: the refusal's stream ended with no error code");

        # A sibling stream on the same connection still works.
        my $sibling_id = h2_submit($client, $client_sock, 'http', '/sibling');
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 25, sub { defined $closed{$sibling_id} });
        is($closed{$sibling_id}, 0, "h2 $kind: a sibling stream on the same connection still works");
        like($body, qr/http/, "h2 $kind: and the sibling's own response arrived");

        $stream_io->close_now;
        $loop->remove($server);
    }
};

done_testing;
