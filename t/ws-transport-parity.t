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
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# ============================================================
# Test: pagi.transport PARITY between WebSocket-over-HTTP/1.1 and
# WebSocket-over-HTTP/2 (packet 10B, John's ratified acceptance criterion)
# ============================================================
# "FROM THE APPLICATION'S PERSPECTIVE THE TRANSPORT IS INVISIBLE": a
# WebSocket app sees the same pagi.transport surface and semantics whether
# the connection is HTTP/1.1 or HTTP/2. This file runs the exact same app
# coderef against a real HTTP/1.1 WebSocket connection and a real
# WebSocket-over-HTTP/2 (RFC 8441) connection, both configured with the same
# write_high_watermark/write_low_watermark, and asserts both transports
# report identical values through the identical interface.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

my $loop = IO::Async::Loop->new;

# Configured explicitly (not left at defaults) on BOTH servers below, so a
# match here proves the values are genuinely threaded through per-connection
# config on each transport, not a coincidence of two identical defaults.
my %WATERMARKS = (write_high_watermark => 32768, write_low_watermark => 8192);

# Same idea for the websocket scope's max_frame_size/max_receive_queue keys
# (spec: PAGI::Spec::Www "WebSocket Scope").
my %WS_LIMITS = (max_ws_frame_size => 32768, max_receive_queue => 250);

# The one app both transports run. Probes pagi.transport's surface (method
# presence, configured watermark values, buffered_amount's type) and issues
# one send comfortably over the high mark -- a SINGLE call, so it queues
# without $send itself blocking (the queue was empty immediately before this
# push, on both transports), safe even though the peer may not read it back
# right away.
sub make_transport_probe_app {
    my (%opts) = @_;
    my ($tag, $results) = @opts{qw(tag results)};

    return async sub {
        my ($scope, $receive, $send) = @_;

        if ($scope->{type} eq 'lifespan') {
            while (1) {
                my $event = await $receive->();
                if ($event->{type} eq 'lifespan.startup') {
                    await $send->({ type => 'lifespan.startup.complete' });
                }
                elsif ($event->{type} eq 'lifespan.shutdown') {
                    await $send->({ type => 'lifespan.shutdown.complete' });
                    last;
                }
            }
            return;
        }

        return unless $scope->{type} eq 'websocket';

        $results->{$tag}{has_max_frame_size}    = exists $scope->{max_frame_size} ? 1 : 0;
        $results->{$tag}{max_frame_size}        = $scope->{max_frame_size};
        $results->{$tag}{has_max_receive_queue} = exists $scope->{max_receive_queue} ? 1 : 0;
        $results->{$tag}{max_receive_queue}     = $scope->{max_receive_queue};

        my $t = $scope->{'pagi.transport'};
        $results->{$tag}{has_transport} = $t ? 1 : 0;
        return unless $t;

        $results->{$tag}{high} = $t->high_water_mark;
        $results->{$tag}{low}  = $t->low_water_mark;
        $results->{$tag}{missing_methods} = [
            grep { !$t->can($_) }
                qw(buffered_amount high_water_mark low_water_mark on_high_water on_drain)
        ];
        $results->{$tag}{buffered_before_send} = $t->buffered_amount;

        $t->on_high_water(sub { $results->{$tag}{hit_high}++ });
        $t->on_drain(sub     { $results->{$tag}{hit_drain}++ });

        await $send->({ type => 'websocket.accept' });
        my $event = await $receive->();   # websocket.connect

        await $send->({ type => 'websocket.send', text => ('x' x 40_000) });
        $results->{$tag}{buffered_after_send} = $t->buffered_amount;

        while (1) {
            $event = await $receive->();
            last if $event->{type} eq 'websocket.disconnect';
        }
        $results->{$tag}{buffered_after_disconnect} = $t->buffered_amount;
    };
}

# ============================================================
# HTTP/1.1 harness: a real listening PAGI::Server and a raw TCP client
# (WebSocket-over-HTTP/1.1 is already covered end-to-end elsewhere; this
# harness only needs to get through the handshake and read frames).
# ============================================================
sub run_h1_probe {
    my ($results, %opts) = @_;
    my $tag    = $opts{tag} // 'h1';
    my %config = (%WATERMARKS, %{ $opts{config} // {} });
    my $app = make_transport_probe_app(tag => $tag, results => $results);

    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, %config,
    );
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );
    die "connect failed: $!" unless $sock;

    my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
    print $sock "GET / HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "Upgrade: websocket\r\n";
    print $sock "Connection: Upgrade\r\n";
    print $sock "Sec-WebSocket-Key: $key\r\n";
    print $sock "Sec-WebSocket-Version: 13\r\n";
    print $sock "\r\n";

    $sock->blocking(0);
    my $raw = '';
    my $deadline = time + 5;
    my $idle_rounds = 0;
    # Read the 101 response headers, then the app's single WS frame that
    # follows. Bounded by BOTH a hard 5s deadline and idle detection (headers
    # seen, then two rounds with no new bytes) -- the connection otherwise
    # stays open (this is a live WebSocket), so waiting for peer-close here
    # would just burn the whole 5s deadline every run.
    while (time < $deadline) {
        my $buf;
        my $n = sysread($sock, $buf, 65536);
        if (defined $n && $n > 0) {
            $raw .= $buf;
            $idle_rounds = 0;
        }
        elsif (defined $n && $n == 0) {
            last;   # peer closed
        }
        else {
            $idle_rounds++;
        }
        last if $raw =~ /\r\n\r\n/ && $idle_rounds >= 3;
        $loop->loop_once(0.05);
    }

    $sock->close;
    # Give the app's receive() loop a bounded chance to observe the close
    # (client_closed disconnect) and finish before tearing the server down.
    for (1 .. 20) {
        $loop->loop_once(0.05);
    }
    $loop->remove($server);
    return;
}

# ============================================================
# HTTP/2 harness (RFC 8441 Extended CONNECT), same shape as
# t/http2/36-ws-transport.t's harness.
# ============================================================
sub run_h2_probe {
    my ($results, %opts) = @_;
    my $tag    = $opts{tag} // 'h2';
    my %config = (%WATERMARKS, %{ $opts{config} // {} });
    my $app = make_transport_probe_app(tag => $tag, results => $results);
    my $protocol = PAGI::Server::Protocol::HTTP1->new;

    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, http2 => 1, %config,
    );
    $loop->add($server);

    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 },
    );
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
        write_high_watermark => $server->{write_high_watermark},
        write_low_watermark  => $server->{write_low_watermark},
        max_receive_queue => $server->{max_receive_queue},
        max_ws_frame_size => $server->{max_ws_frame_size},
    );
    $server->add_child($stream);
    $conn->start;

    require Net::HTTP2::nghttp2::Session;
    my $client = Net::HTTP2::nghttp2::Session->new_client(
        callbacks => {
            on_begin_headers   => sub { 0 },
            on_header          => sub { 0 },
            on_frame_recv      => sub { 0 },
            on_data_chunk_recv => sub { 0 },
            on_stream_close    => sub { 0 },
        },
    );

    my $exchange = sub {
        my ($rounds) = @_;
        $rounds //= 10;
        for (1 .. $rounds) {
            $loop->loop_once(0.1);
            my $buf = '';
            $sock_b->sysread($buf, 65536);
            $client->mem_recv($buf) if length($buf);
            my $out = $client->mem_send;
            $sock_b->syswrite($out) if length($out);
        }
    };

    $loop->loop_once(0.1);
    my $server_settings = '';
    $sock_b->sysread($server_settings, 4096);
    $client->send_connection_preface;
    $sock_b->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($server_settings);
    $loop->loop_once(0.1);
    my $ack = '';
    $sock_b->sysread($ack, 4096);
    $client->mem_recv($ack) if length($ack);
    my $client_ack = $client->mem_send;
    $sock_b->syswrite($client_ack) if length($client_ack);
    $loop->loop_once(0.1);
    my $extra = '';
    $sock_b->sysread($extra, 4096);
    $client->mem_recv($extra) if length($extra);

    $client->submit_request(
        method    => 'CONNECT',
        path      => '/ws',
        scheme    => 'https',
        authority => 'localhost',
        headers   => [
            [':protocol', 'websocket'],
            ['sec-websocket-version', '13'],
        ],
        body      => sub { return undef },   # streaming: keep open
    );
    $sock_b->syswrite($client->mem_send);
    $exchange->(30);   # handshake + accept + send + eventual RST teardown

    # Tear down: RST the stream so the app's receive() loop completes.
    $client->submit_rst_stream(1, 8);   # CANCEL
    $sock_b->syswrite($client->mem_send);
    $exchange->(10);

    $stream->close_now;
    $loop->remove($server);
    return;
}

my %results;
run_h1_probe(\%results, tag => 'h1', config => \%WS_LIMITS);
run_h2_probe(\%results, tag => 'h2', config => \%WS_LIMITS);

# A second pair of probes configured with max_ws_frame_size => 0 (unlimited,
# per Protocol::WebSocket::Frame's max_payload_size semantics: a server that
# does not enforce a cap must not advertise one). max_receive_queue has no
# such unlimited mode (it is a hard, always-enforced cap), so it is left at
# its default here and not probed for omission.
run_h1_probe(\%results, tag => 'h1_unlimited', config => { max_ws_frame_size => 0 });
run_h2_probe(\%results, tag => 'h2_unlimited', config => { max_ws_frame_size => 0 });

# ============================================================
# Parity assertions: the SAME app, the SAME questions, on both transports.
# ============================================================
for my $tag (qw(h1 h2)) {
    ok($results{$tag}{has_transport}, "$tag: pagi.transport is present on the websocket scope");
    is($results{$tag}{missing_methods}, [],
        "$tag: pagi.transport exposes buffered_amount/high_water_mark/low_water_mark/on_high_water/on_drain")
        or diag("$tag missing: " . join(', ', @{$results{$tag}{missing_methods} // []}));
}

is($results{h1}{high}, $WATERMARKS{write_high_watermark},
    'h1: high_water_mark reports the configured value');
is($results{h2}{high}, $WATERMARKS{write_high_watermark},
    'h2: high_water_mark reports the configured value');
is($results{h1}{high}, $results{h2}{high},
    'PARITY: high_water_mark is the identical number on both transports');

is($results{h1}{low}, $WATERMARKS{write_low_watermark},
    'h1: low_water_mark reports the configured value');
is($results{h2}{low}, $WATERMARKS{write_low_watermark},
    'h2: low_water_mark reports the configured value');
is($results{h1}{low}, $results{h2}{low},
    'PARITY: low_water_mark is the identical number on both transports');

for my $tag (qw(h1 h2)) {
    ok(defined $results{$tag}{buffered_before_send}
            && $results{$tag}{buffered_before_send} =~ /^\d+$/,
        "$tag: buffered_amount is a well-defined non-negative integer before any send");
    ok(defined $results{$tag}{buffered_after_send}
            && $results{$tag}{buffered_after_send} =~ /^\d+$/,
        "$tag: buffered_amount is a well-defined non-negative integer after a large send");
}

# ============================================================
# max_frame_size / max_receive_queue PARITY (spec: WebSocket Scope, optional
# Int keys "when the server enforces one").
# ============================================================
for my $tag (qw(h1 h2)) {
    ok($results{$tag}{has_max_frame_size},
        "$tag: max_frame_size key is present on the websocket scope when enforced");
    is($results{$tag}{max_frame_size}, $WS_LIMITS{max_ws_frame_size},
        "$tag: websocket scope max_frame_size reports the configured max_ws_frame_size");

    ok($results{$tag}{has_max_receive_queue},
        "$tag: max_receive_queue key is present on the websocket scope");
    is($results{$tag}{max_receive_queue}, $WS_LIMITS{max_receive_queue},
        "$tag: websocket scope max_receive_queue reports the configured value");
}
is($results{h1}{max_frame_size}, $results{h2}{max_frame_size},
    'PARITY: max_frame_size is the identical number on both transports');
is($results{h1}{max_receive_queue}, $results{h2}{max_receive_queue},
    'PARITY: max_receive_queue is the identical number on both transports');

for my $tag (qw(h1_unlimited h2_unlimited)) {
    ok(!$results{$tag}{has_max_frame_size},
        "$tag: max_frame_size key is absent from the websocket scope when max_ws_frame_size is 0 (unlimited)");
}

done_testing;
