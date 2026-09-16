#!/usr/bin/env perl

# =============================================================================
# Cross-transport parity: the WebSocket close-truthfulness terminal outcome is
# the SAME on HTTP/1.1 and HTTP/2. Identical application behaviour, driven
# against each transport, must yield the identical terminal CATEGORY, reason and
# close_code. Www.pod L857-869 / design spec "WebSocket Close Truthfulness": the
# terminal outcome is a portable, spec-defined fact; a consumer must not have to
# know which transport carried the socket to know how the scope ended.
#
# This is the cross-transport regression the per-transport files (h1:
# t/ws-close-deadline-h1.t, h2: t/http2/49-ws-close-deadline-h2.t) do not make:
# each proves its own transport exhaustively, neither proves the two AGREE.
#
# Five outcomes, table-driven, each run on BOTH transports:
#   clean               app close + peer Close + transport/stream close
#   close_timeout/1006  app close, peer silent, the deadline expires
#   transport-loss/1006 app close, transport/stream drops with no peer Close
#   peer-preserved      app close, peer Close (held open), deadline expires ->
#                       abnormal but the peer's own code/reason survive (cat 4)
#   server_error/1011   app returns from an accepted socket with no closing
#                       handshake (the "Application Left a Response Incomplete"
#                       path -- unchanged by this work, asserted identical here)
#
# The transport-loss token is legitimately transport-specific (h1 may report
# client_closed/read_error, h2 reports client_closed), so parity is asserted at
# the spec-defined CATEGORY level (transport_loss), with the exact reason pinned
# for the categories whose token IS transport-agnostic (clean, close_timeout,
# server_error). close_code and close_reason are asserted identical throughout.
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

my $loop = IO::Async::Loop->new;

# The app-walked-away rows deliberately trigger the server_error log on both
# transports (the "Application Left a Response Incomplete" path). Route every
# harness server's logs here so that expected error line never reaches the
# suite's stderr; the exact log text is pinned in the per-transport files
# (t/ws-close-deadline-h1.t case 6, t/http2/49 case 6), not re-asserted here.
my @captured_logs;
my $capture_logger = sub { push @captured_logs, $_[0]->{message} // '' };

# ---------------------------------------------------------------------------
# Terminal-outcome capture, shared by both transports.
# ---------------------------------------------------------------------------

# The accepted-WebSocket application, identical for both transports: register
# the terminal observers, send its own websocket.close (unless $send_close is
# false -- the app-walked-away case), then park (a peer/deadline/transport event
# ends the scope) or return.
sub build_app {
    my ($obs, $park, %o) = @_;
    $o{send_close} //= 1;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $c = $scope->{'pagi.connection'};

        await $receive->();
        await $send->({ type => 'websocket.accept' });

        $c->on_complete(sub {
            $obs->{complete}++;
            $obs->{c_reason}  = $c->disconnect_reason;
            $obs->{c_code}    = $c->close_code;
            $obs->{c_creason} = $c->close_reason;
        });
        $c->on_disconnect(sub {
            my ($r) = @_;
            $obs->{disconnect}++;
            $obs->{d_reason}  = $r;
            $obs->{d_code}    = $c->close_code;
            $obs->{d_creason} = $c->close_reason;
        });

        if ($o{send_close}) {
            await $send->({ type => 'websocket.close', code => 1000, reason => 'bye' });
            $obs->{sent} = 1;
        }
        if ($o{park}) { await $park; }
        return;
    };
}

# Normalize the observed terminal into the portable, transport-agnostic tuple
# the spec defines: which terminal fired, its spec-defined category, the reason
# token, and the peer close_code/close_reason.
sub category {
    my ($terminal, $reason) = @_;
    return 'clean' if $terminal eq 'complete';
    $reason //= '';
    return 'close_timeout'  if $reason eq 'close_timeout';
    return 'server_error'   if $reason eq 'server_error';
    return 'transport_loss' if $reason =~ /^(?:client_closed|read_error|write_error)$/;
    return "other:$reason";
}

sub normalize {
    my ($obs) = @_;
    if ($obs->{complete}) {
        return {
            terminal => 'complete',
            category => category('complete'),
            code     => $obs->{c_code},
            creason  => $obs->{c_creason},
        };
    }
    if ($obs->{disconnect}) {
        return {
            terminal => 'disconnect',
            category => category('disconnect', $obs->{d_reason}),
            code     => $obs->{d_code},
            creason  => $obs->{d_creason},
        };
    }
    return { terminal => 'none', category => 'none' };
}

# ---------------------------------------------------------------------------
# HTTP/1.1 harness (a real server + a real TCP client).
# ---------------------------------------------------------------------------

sub h1_frame {
    my ($opcode, $payload) = @_;
    my $frame = chr(0x80 | $opcode);
    my $len = length $payload;
    if ($len < 126) { $frame .= chr(0x80 | $len); }
    else            { $frame .= chr(0x80 | 126) . pack('n', $len); }
    my $mask = pack('N', int(rand(0xFFFFFFFF)));
    $frame .= $mask;
    my $masked = '';
    $masked .= chr(ord(substr($payload, $_, 1)) ^ ord(substr($mask, $_ % 4, 1)))
        for 0 .. $len - 1;
    return $frame . $masked;
}

sub h1_pump_until {
    my ($cond, $timeout) = @_;
    $timeout //= 6;
    my $deadline = time + $timeout;
    while (time < $deadline) {
        return 1 if $cond->();
        $loop->loop_once(0.02);
    }
    return $cond->() ? 1 : 0;
}

sub h1_start {
    my ($app, %opt) = @_;
    my $server = PAGI::Server->new(
        app              => $app,
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        shutdown_timeout => 1,
        access_log       => undef,
        logger           => $capture_logger,
        (exists $opt{ws_close_timeout} ? (ws_close_timeout => $opt{ws_close_timeout}) : ()),
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub h1_upgrade {
    my ($sock) = @_;
    syswrite($sock,
          "GET / HTTP/1.1\r\n"
        . "Host: localhost\r\n"
        . "Upgrade: websocket\r\n"
        . "Connection: Upgrade\r\n"
        . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        . "Sec-WebSocket-Version: 13\r\n\r\n");
    my $got = '';
    h1_pump_until(sub {
        my $buf; my $n = sysread($sock, $buf, 4096);
        $got .= $buf if defined $n && $n > 0;
        return $got =~ /HTTP\/1\.1 101/;
    }, 5);
    return $got;
}

# Drive one outcome on HTTP/1.1. $peer is a closure handed the client socket; it
# performs the transport-specific half of the scenario (send a Close frame, drop
# the socket, or nothing). Returns the normalized terminal tuple.
sub run_h1 {
    my (%o) = @_;
    my %obs;
    my $park = $loop->new_future;
    # The close scenarios park (the terminal comes from the peer/deadline/
    # transport); the app-walked-away scenario (send_close 0) must RETURN so the
    # incomplete-handshake path fires. So park iff the app sends a close.
    my $sc = defined $o{send_close} ? $o{send_close} : 1;
    my $app  = build_app(\%obs, $park, send_close => $sc, park => $sc);
    my $server = h1_start($app, ws_close_timeout => $o{ws_close_timeout});
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2,
    ) or die "connect: $!";
    $sock->blocking(0);
    h1_upgrade($sock);

    if ($o{send_close}) {
        h1_pump_until(sub { $obs{sent} }, 5);
    }
    $o{peer}->($sock) if $o{peer};
    h1_pump_until(sub { $obs{complete} || $obs{disconnect} }, 6);

    my $r = normalize(\%obs);
    $park->done unless $park->is_ready;
    close($sock);
    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
    return $r;
}

# ---------------------------------------------------------------------------
# HTTP/2 harness (a manually driven connection + an nghttp2 client), lifted
# from t/http2/49-ws-close-deadline-h2.t.
# ---------------------------------------------------------------------------

my $h1_protocol = PAGI::Server::Protocol::HTTP1->new;

sub h2_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app => $args{app} // sub { }, host => '127.0.0.1', port => 0,
        quiet => 1, http2 => 1, logger => $capture_logger,
    );
    $loop->add($server);
    return $server;
}

sub h2_connection {
    my (%o) = @_;
    socketpair(my $sa, my $sb, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sa->blocking(0); $sb->blocking(0);
    my $app    = $o{app};
    my $server = h2_server(app => $app);
    my $stream = IO::Async::Stream->new(
        read_handle => $sa, write_handle => $sa, on_read => sub { 0 },
    );
    my $conn = PAGI::Server::Connection->new(
        stream        => $stream,
        app           => $app,
        protocol      => $h1_protocol,
        server        => $server,
        h2_protocol   => $server->{http2_protocol},
        alpn_protocol => 'h2',
        ws_close_timeout => $o{ws_close_timeout} // 10,
    );
    $server->add_child($stream);
    $conn->start;
    return ($conn, $stream, $sb, $server);
}

sub h2_client {
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers => sub { 0 }, on_header => sub { 0 },
        on_frame_recv => sub { 0 }, on_data_chunk_recv => sub { 0 },
        on_stream_close => sub { 0 },
    });
}

sub h2_handshake {
    my ($client, $sock) = @_;
    $loop->loop_once(0.1);
    my $settings = ''; $sock->sysread($settings, 4096);
    $client->send_connection_preface;
    $sock->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($settings);
    $loop->loop_once(0.1);
    my $ack = ''; $sock->sysread($ack, 4096);
    $client->mem_recv($ack) if length $ack;
    my $cack = $client->mem_send;
    $sock->syswrite($cack) if length $cack;
    $loop->loop_once(0.1);
    my $extra = ''; $sock->sysread($extra, 4096);
    $client->mem_recv($extra) if length $extra;
}

sub h2_exchange {
    my ($client, $sock, $rounds) = @_;
    $rounds //= 6;
    for (1 .. $rounds) {
        $loop->loop_once(0.05);
        my $buf = ''; $sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length $buf;
        my $out = $client->mem_send;
        $sock->syswrite($out) if length $out;
    }
}

sub h2_pump_until {
    my ($client, $sock, $cond, $rounds) = @_;
    $rounds //= 80;
    for (1 .. $rounds) {
        return 1 if $cond->();
        $loop->loop_once(0.05);
        my $buf = ''; $sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length $buf;
        my $out = $client->mem_send;
        $sock->syswrite($out) if length $out;
    }
    return $cond->() ? 1 : 0;
}

sub h2_open_ws {
    my ($client, $sock) = @_;
    my $sid = $client->submit_request(
        method => 'CONNECT', path => '/ws', scheme => 'https', authority => 'localhost',
        headers => [ [':protocol', 'websocket'], ['sec-websocket-version', '13'] ],
        body    => sub { return undef },
    );
    $sock->syswrite($client->mem_send);
    h2_exchange($client, $sock, 4);
    return $sid;
}

sub h2_close_frame {
    my ($code, $reason) = @_;
    $reason //= '';
    return Protocol::WebSocket::Frame->new(
        type => 'close', buffer => pack('n', $code) . $reason, masked => 1,
    )->to_bytes;
}

sub h2_send_data {
    my ($client, $sock, $sid, $data, $end) = @_;
    $client->submit_data($sid, $data, $end // 0);
    my $out = $client->mem_send;
    $sock->syswrite($out) if length $out;
}

# Drive one outcome on HTTP/2. $peer is handed ($client, $sock, $sid) and does
# the transport-specific half. Returns the normalized terminal tuple.
sub run_h2 {
    my (%o) = @_;
    my %obs;
    my $park = $loop->new_future;
    my $sc = defined $o{send_close} ? $o{send_close} : 1;
    my $app  = build_app(\%obs, $park, send_close => $sc, park => $sc);
    my ($conn, $stream_io, $sock, $server) =
        h2_connection(app => $app, ws_close_timeout => $o{ws_close_timeout});
    my $client = h2_client();

    h2_handshake($client, $sock);
    my $sid = h2_open_ws($client, $sock);
    if ($o{send_close}) {
        h2_pump_until($client, $sock, sub { $obs{sent} });
    }
    $o{peer}->($client, $sock, $sid) if $o{peer};
    h2_pump_until($client, $sock, sub { $obs{complete} || $obs{disconnect} });

    my $r = normalize(\%obs);
    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    eval { $loop->remove($server) };
    return $r;
}

# The parity fields: the portable tuple that must match across transports.
sub parity_key {
    my ($r) = @_;
    return { terminal => $r->{terminal}, category => $r->{category},
             code => $r->{code}, creason => $r->{creason} };
}

# ---------------------------------------------------------------------------
# The parity table.
# ---------------------------------------------------------------------------

subtest 'clean: app close + peer Close + transport/stream close' => sub {
    my $r1 = run_h1(
        send_close => 1,
        peer => sub {
            my ($sock) = @_;
            syswrite($sock, h1_frame(8, pack('n', 1000) . 'bye'));
            h1_pump_until(sub { 0 }, 0.2);   # let the server read the Close
            close($sock);                     # transport closes -> clean
        },
    );
    my $r2 = run_h2(
        send_close => 1,
        peer => sub {
            my ($client, $sock, $sid) = @_;
            h2_send_data($client, $sock, $sid, h2_close_frame(1000, 'bye'), 1);
        },
    );
    is($r1->{category}, 'clean', 'h1 clean');
    is($r2->{category}, 'clean', 'h2 clean');
    is($r1->{code}, 1000, 'h1 close_code is the peer code');
    is($r1->{creason}, 'bye', 'h1 close_reason is the peer text');
    is(parity_key($r1), parity_key($r2), 'h1/h2 parity: clean');
};

subtest 'close_timeout/1006: app close, peer silent, deadline expires' => sub {
    my $r1 = run_h1(send_close => 1, ws_close_timeout => 0.3);   # no peer script
    my $r2 = run_h2(send_close => 1, ws_close_timeout => 0.3);
    is($r1->{category}, 'close_timeout', 'h1 close_timeout');
    is($r2->{category}, 'close_timeout', 'h2 close_timeout');
    is($r1->{code}, 1006, 'h1 close_code 1006');
    is($r1->{creason}, undef, 'h1 close_reason undef');
    is(parity_key($r1), parity_key($r2), 'h1/h2 parity: close_timeout/1006');
};

subtest 'transport-loss/1006: app close, transport/stream drops with no peer Close' => sub {
    my $r1 = run_h1(
        send_close => 1, ws_close_timeout => 30,
        peer => sub { my ($sock) = @_; close($sock); },   # FIN, no Close frame
    );
    my $r2 = run_h2(
        send_close => 1, ws_close_timeout => 30,
        peer => sub {
            my ($client, $sock, $sid) = @_;
            h2_send_data($client, $sock, $sid, '', 1);     # bare END_STREAM, no Close
        },
    );
    is($r1->{category}, 'transport_loss', 'h1 transport_loss');
    is($r2->{category}, 'transport_loss', 'h2 transport_loss');
    is($r1->{code}, 1006, 'h1 close_code 1006');
    is($r1->{creason}, undef, 'h1 close_reason undef');
    is(parity_key($r1), parity_key($r2), 'h1/h2 parity: transport-loss/1006');
};

subtest 'peer-preserved (cat 4): abnormal but the peer code/reason survive' => sub {
    my $r1 = run_h1(
        send_close => 1, ws_close_timeout => 0.3,
        peer => sub {
            my ($sock) = @_;
            # Peer sends a valid Close but holds the transport open; the deadline
            # then expires -> abnormal, yet the peer's own code/reason stand.
            syswrite($sock, h1_frame(8, pack('n', 1000) . 'seeya'));
        },
    );
    my $r2 = run_h2(
        send_close => 1, ws_close_timeout => 0.3,
        peer => sub {
            my ($client, $sock, $sid) = @_;
            h2_send_data($client, $sock, $sid, h2_close_frame(1000, 'seeya'), 0);  # no END_STREAM
        },
    );
    is($r1->{category}, 'close_timeout', 'h1 abnormal (close_timeout) but preserved');
    is($r2->{category}, 'close_timeout', 'h2 abnormal (close_timeout) but preserved');
    is($r1->{code}, 1000, 'h1 close_code preserved from the peer Close');
    is($r1->{creason}, 'seeya', 'h1 close_reason preserved from the peer Close');
    is(parity_key($r1), parity_key($r2), 'h1/h2 parity: peer-preserved (cat 4)');
};

subtest 'server_error/1011: app walks away from an accepted socket' => sub {
    my $r1 = run_h1(send_close => 0);   # app returns, no closing handshake
    my $r2 = run_h2(send_close => 0);
    is($r1->{category}, 'server_error', 'h1 server_error');
    is($r2->{category}, 'server_error', 'h2 server_error');
    is($r1->{code}, 1006, 'h1 close_code 1006 (no peer Close observed)');
    is(parity_key($r1), parity_key($r2), 'h1/h2 parity: server_error');
};

done_testing;
