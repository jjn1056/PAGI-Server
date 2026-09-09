use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Future::AsyncAwait;
use Future::IO::Impl::IOAsync;
use Socket qw(AF_UNIX SOCK_STREAM);
use MIME::Base64 ();
use FindBin;
use lib "$FindBin::Bin/../lib";

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# ============================================================
# Test: disconnect delivery matrix, both transports, both protocol scopes
# ============================================================
# Conformance port of the audit probe that measured S1/S2/S4/S5/S7. One
# cell per (transport x scope x point in the scope's lifetime), each one
# checking the four facts Www.pod ties together for a scope that ends:
#
#   * the disconnect receive event's type matches the scope kind
#     ("Disconnect - receive event", "SSE Disconnect - receive event"),
#   * its reason is the standard token for the condition, or the peer's own
#     text when the peer sent the Close frame ("Standard Disconnect
#     Reasons"),
#   * the pagi.connection object's disconnect_reason carries the same token
#     ("Meaning per scope", Agreement with disconnect events),
#   * a terminal send racing the disconnect resolves rather than hanging or
#     failing (the Send Completion Contract).
#
#   transports: h1 (raw TCP socket)
#               h2 (in-process Connection over socketpair, nghttp2 client)
#   scopes:     ws  (Extended CONNECT on h2, Upgrade on h1)
#               sse (Accept: text/event-stream)
#   cases:      pre  - app consumed connect/request, parked on receive(),
#                      client drops
#               mid  - app sent a refusal start + body(more=>1), parked on
#                      receive(), client drops, then sends its terminal body
#               post - app completed the refusal (more=>0), then calls
#                      receive(): a completed refusal is a clean end and
#                      delivers no disconnect event
#               acc  - app accepted the WebSocket, parked, client drops
#               peer - app accepted the WebSocket, peer sent a Close frame
#               rsv1 - app accepted the WebSocket, peer sent a frame with
#                      RSV1 set (a server-detected protocol violation)
#   drops:      close (socket EOF)   rst (h2 only: RST_STREAM CANCEL)
#
# The refusal cases still send websocket.http.response.* / sse.http.response.*;
# Task B4 switches those names to http.response.*.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;
use Net::HTTP2::nghttp2::Session;

my $loop = IO::Async::Loop->new;
my $h1proto = PAGI::Server::Protocol::HTTP1->new;

# The cases that take the WebSocket handshake to 'accepted' instead of
# refusing it.
my %ACCEPTS = map { $_ => 1 } qw(acc peer rsv1);

# A masked Close frame carrying the peer's own code and reason text. The
# server must deliver both verbatim rather than substituting a PAGI token
# (Www.pod "Disconnect - receive event", normative pairings).
sub peer_close_frame {
    return Protocol::WebSocket::Frame->new(
        type   => 'close',
        buffer => pack('n', 1000) . 'bye',
        masked => 1,
    )->to_bytes;
}

# A masked text frame with RSV1 set. No extension was negotiated, so this
# is a protocol violation regardless of payload (RFC 6455 section 5.2).
sub rsv1_frame {
    return Protocol::WebSocket::Frame->new(
        type   => 'text',
        buffer => 'hello',
        masked => 1,
        rsv    => [1, 0, 0],
    )->to_bytes;
}

sub make_app {
    my ($type, $case, $r) = @_;
    my $prefix = $type eq 'ws' ? 'websocket.http.response' : 'sse.http.response';
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq ($type eq 'ws' ? 'websocket' : 'sse');
        my $conn = $scope->{'pagi.connection'};
        $r->{has_conn} = $conn ? 1 : 0;
        if ($conn) {
            $conn->on_disconnect(sub { $r->{on_disconnect} = [@_] });
            $conn->on_complete(sub { $r->{on_complete}++ });
        }
        $r->{http_version} = $scope->{http_version};
        my $first = await $receive->();
        $r->{first} = $first->{type};

        if ($ACCEPTS{$case}) { await $send->({ type => q{websocket.accept} }); $r->{accepted} = 1 }
        if (!$ACCEPTS{$case} && $case ne q{pre}) {
            await $send->({ type => "$prefix.start", status => 403,
                            headers => [['content-type', 'text/plain']] });
            await $send->({ type => "$prefix.body", body => 'chunk-1',
                            more => $case eq 'mid' ? 1 : 0 });
            $r->{sent_body1} = 1;
        }

        $r->{parked} = 1;
        my $rf = $receive->();
        await Future->wait_any($rf->without_cancel, $loop->delay_future(after => $case eq 'post' ? 0.8 : 4));
        $r->{recv_ready} = $rf->is_ready ? 1 : 0;
        $r->{recv} = !$rf->is_ready ? 'PENDING'
                   : $rf->is_failed ? 'FAILED:' . ($rf->failure)[0]
                   : join(',', map { "$_=" . ($rf->get->{$_} // '') } sort keys %{$rf->get});
        $r->{obj_reason} = $conn ? $conn->disconnect_reason : undef;
        # A receive() left parked by a completed refusal outlives this sub.
        # Hand it to the caller so the suspended async sub behind it is not
        # reaped mid-run, which Future::AsyncAwait reports as a lost
        # returning future -- test-harness noise, not server behaviour.
        $r->{parked_future} = $rf unless $rf->is_ready;

        if ($case eq 'mid') {
            my $sf = $send->({ type => "$prefix.body", body => 'tail', more => 0 });
            await Future->wait_any($sf->without_cancel, $loop->delay_future(after => 1));
            $r->{term_send} = !$sf->is_ready ? 'PENDING'
                            : $sf->is_failed ? 'FAILED:' . ($sf->failure)[0] : 'resolved';
        }
        $r->{app_done} = 1;
        return;
    };
}

sub pump_until { my ($cond, $pump, $max) = @_; for (1 .. $max) { return 1 if $cond->(); $pump->() } return $cond->() ? 1 : 0 }

# ---------------- HTTP/1.1 ----------------
sub run_h1 {
    my ($type, $case) = @_;
    my %r;
    my $server = PAGI::Server->new(app => make_app($type, $case, \%r), host => '127.0.0.1',
                                   port => 0, quiet => 1, shutdown_timeout => 1,
                                   access_log => undef);
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5)
        or die "connect: $!";
    my $req = $type eq 'ws'
        ? "GET /socket HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
          . "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " . MIME::Base64::encode_base64('0123456789abcdef', '') . "\r\n\r\n"
        : "GET /events HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nAccept: text/event-stream\r\n\r\n";
    print $sock $req;
    $sock->blocking(0);
    my $wire = '';
    my $pump = sub { my $b; my $n = sysread($sock, $b, 65536); $wire .= $b if $n; $loop->loop_once(0.05) };
    pump_until(sub { $r{parked} }, $pump, 60);
    $pump->() for 1 .. 4;                       # let any flushed bytes arrive
    $r{wire_before_drop} = length $wire;
    if    ($case eq 'peer') { print $sock peer_close_frame() }  # closing handshake
    elsif ($case ne 'post') { close $sock }                     # abnormal drop
    pump_until(sub { $r{app_done} }, sub { $loop->loop_once(0.05) }, 120);
    $server->shutdown->get;
    $loop->remove($server);
    return \%r;
}

# ---------------- HTTP/2 ----------------
sub run_h2 {
    my ($type, $case, $drop) = @_;
    my %r;
    my $app = make_app($type, $case, \%r);
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1,
                                   http2 => 1, access_log => undef);
    $loop->add($server);
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $_->blocking(0) for $sock_a, $sock_b;
    my $stream = IO::Async::Stream->new(read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(stream => $stream, app => $app, protocol => $h1proto,
        server => $server, h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2');
    $server->add_child($stream);
    $conn->start;

    my (%headers, $data); $data = '';
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers => sub { 0 }, on_frame_recv => sub { 0 }, on_stream_close => sub { 0 },
        on_header => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; 0 },
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $data .= $d; 0 },
    });
    my $xfer = sub {
        $loop->loop_once(0.05);
        my $buf = ''; $sock_b->sysread($buf, 65536);
        $client->mem_recv($buf) if length $buf;
        my $out = $client->mem_send;
        $sock_b->syswrite($out) if length $out;
    };
    # handshake -- verbatim sequence from t/http2/22-denial-response.t complete_h2_handshake
    $loop->loop_once(0.1);
    my $server_settings = q{}; $sock_b->sysread($server_settings, 4096);
    $client->send_connection_preface;
    $sock_b->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($server_settings);
    $loop->loop_once(0.1);
    my $ack = q{}; $sock_b->sysread($ack, 4096); $client->mem_recv($ack) if length $ack;
    my $client_ack = $client->mem_send; $sock_b->syswrite($client_ack) if length $client_ack;
    $loop->loop_once(0.1);
    my $extra = q{}; $sock_b->sysread($extra, 4096); $client->mem_recv($extra) if length $extra;

    my $sid = $type eq 'ws'
        ? $client->submit_request(method => 'CONNECT', path => '/socket', scheme => 'https', authority => 'localhost',
            headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']], body => sub { undef })
        : $client->submit_request(method => 'GET', path => '/events', scheme => 'http', authority => 'localhost',
            headers => [['accept', 'text/event-stream']]);
    $sock_b->syswrite($client->mem_send);
    pump_until(sub { $r{parked} }, $xfer, 60);
    $xfer->() for 1 .. 4;
    $r{wire_before_drop} = (join(';', map { "$_=$headers{$_}" } sort keys %headers) || 'none') . " data='$data'";
    if    ($drop eq 'rst')  { $client->submit_rst_stream($sid, 8); $sock_b->syswrite($client->mem_send) }
    elsif ($drop eq 'close'){ close $sock_b }
    elsif ($drop eq 'peer') { $client->submit_data($sid, peer_close_frame(), 0); $sock_b->syswrite($client->mem_send) }
    elsif ($drop eq 'rsv1') { $client->submit_data($sid, rsv1_frame(), 0); $sock_b->syswrite($client->mem_send) }
    # 'none': the client stays put; the app is the only thing that ends the scope.
    pump_until(sub { $r{app_done} }, sub { $loop->loop_once(0.05) }, 120);
    $stream->close_now;
    eval { $loop->remove($server) };
    return \%r;
}

# ============================================================
# The matrix
# ============================================================
# Each row: [ label, runner ]. Labels are the audit's own cell names.
my @rows = (
    ['h1 ws pre close',       sub { run_h1('ws',  'pre')  }],
    ['h1 ws mid close',       sub { run_h1('ws',  'mid')  }],
    ['h1 ws post',            sub { run_h1('ws',  'post') }],
    ['h1 ws acc close',       sub { run_h1('ws',  'acc')  }],
    ['h1 ws acc peer-close',  sub { run_h1('ws',  'peer') }],
    ['h1 sse pre close',      sub { run_h1('sse', 'pre')  }],
    ['h1 sse mid close',      sub { run_h1('sse', 'mid')  }],
    ['h1 sse post',           sub { run_h1('sse', 'post') }],
    ['h2 ws pre rst',         sub { run_h2('ws',  'pre',  'rst')   }],
    ['h2 ws pre close',       sub { run_h2('ws',  'pre',  'close') }],
    ['h2 ws mid rst',         sub { run_h2('ws',  'mid',  'rst')   }],
    ['h2 ws mid close',       sub { run_h2('ws',  'mid',  'close') }],
    ['h2 ws post',            sub { run_h2('ws',  'post', 'none')  }],
    ['h2 ws acc rst',         sub { run_h2('ws',  'acc',  'rst')   }],
    ['h2 ws acc close',       sub { run_h2('ws',  'acc',  'close') }],
    ['h2 ws acc peer-close',  sub { run_h2('ws',  'peer', 'peer')  }],
    ['h2 ws acc rsv1',        sub { run_h2('ws',  'rsv1', 'rsv1')  }],
    ['h2 sse pre rst',        sub { run_h2('sse', 'pre',  'rst')   }],
    ['h2 sse pre close',      sub { run_h2('sse', 'pre',  'close') }],
    ['h2 sse mid rst',        sub { run_h2('sse', 'mid',  'rst')   }],
    ['h2 sse mid close',      sub { run_h2('sse', 'mid',  'close') }],
    ['h2 sse post',           sub { run_h2('sse', 'post', 'none')  }],
);

# type/reason are the disconnect event's; obj_reason defaults to reason
# (Www.pod "Agreement with disconnect events") and is spelled out only
# where the two legitimately differ. term is the terminal send's
# settlement. complete/disconnect are asserted only where the cell's whole
# point is which callback family fires.
my %expect = (
    # label                   => [ recv type,              recv reason,     term_send ]
    'h1 ws pre close'    => { type => 'websocket.disconnect', reason => 'client_closed', term => '-' },
    'h1 ws mid close'    => { type => 'websocket.disconnect', reason => 'client_closed', term => 'resolved' },
    'h1 ws post'         => { pending => 1,                                              term => '-' },
    'h1 ws acc close'    => { type => 'websocket.disconnect', reason => 'client_closed', term => '-' },
    'h1 sse pre close'   => { type => 'sse.disconnect',       reason => 'client_closed', term => '-' },
    'h1 sse mid close'   => { type => 'sse.disconnect',       reason => 'client_closed', term => 'resolved' },
    'h1 sse post'        => { pending => 1,                                              term => '-' },
    'h2 ws pre rst'      => { type => 'websocket.disconnect', reason => 'client_closed', term => '-' },
    'h2 ws pre close'    => { type => 'websocket.disconnect', reason => 'client_closed', term => '-' },
    'h2 ws mid rst'      => { type => 'websocket.disconnect', reason => 'client_closed', term => 'resolved' },
    'h2 ws mid close'    => { type => 'websocket.disconnect', reason => 'client_closed', term => 'resolved' },
    'h2 ws post'         => { pending => 1,                                              term => '-' },
    'h2 ws acc rst'      => { type => 'websocket.disconnect', reason => 'client_closed', term => '-' },
    'h2 ws acc close'    => { type => 'websocket.disconnect', reason => 'client_closed', term => '-' },
    'h2 sse pre rst'     => { type => 'sse.disconnect',       reason => 'client_closed', term => '-' },
    'h2 sse pre close'   => { type => 'sse.disconnect',       reason => 'client_closed', term => '-' },
    'h2 sse mid rst'     => { type => 'sse.disconnect',       reason => 'client_closed', term => 'resolved' },
    'h2 sse mid close'   => { type => 'sse.disconnect',       reason => 'client_closed', term => 'resolved' },
    'h2 sse post'        => { pending => 1,                                              term => '-' },

    # A completed closing handshake is a clean end regardless of the peer's
    # close code, and the peer's code and reason text are delivered as
    # protocol data, never replaced by a token (Www.pod "Disconnect -
    # receive event", "Meaning per scope").
    'h1 ws acc peer-close' => { type => 'websocket.disconnect', code => 1000, reason => 'bye',
                                obj_reason => undef, complete => 1, term => '-' },
    'h2 ws acc peer-close' => { type => 'websocket.disconnect', code => 1000, reason => 'bye',
                                obj_reason => undef, complete => 1, term => '-' },

    # A server-detected protocol violation before a clean end is abnormal
    # with the RFC code the server sent and the standard token
    # (Www.pod "Meaning per scope").
    'h2 ws acc rsv1'       => { type => 'websocket.disconnect', code => 1002, reason => 'protocol_error',
                                disconnect => 1, complete => 0, term => '-' },
);

sub check_cell {
    my ($label, $r, $warns) = @_;
    my $e = $expect{$label} or die "no expectation for $label";
    my %recv = $r->{recv} && $r->{recv} ne 'PENDING' ? map { split /=/, $_, 2 } split /,/, $r->{recv} : ();
    subtest $label => sub {
        if ($e->{pending}) {
            is($r->{recv}, 'PENDING', 'no event after a completed refusal');
        }
        else {
            is($recv{type},   $e->{type},   'event type');
            is($recv{reason}, $e->{reason}, 'event reason token');
            is($recv{code},   $e->{code},   'event close code') if exists $e->{code};
            my $obj = exists $e->{obj_reason} ? $e->{obj_reason} : $e->{reason};
            is($r->{obj_reason}, $obj, 'object reason agrees with the event');
        }
        is($r->{term_send} // '-', $e->{term}, 'terminal send settlement');
        is($r->{has_conn}, 1, 'pagi.connection present');
        is($r->{on_complete} ? 1 : 0, $e->{complete} ? 1 : 0, 'on_complete fired')
            if exists $e->{complete};
        is($r->{on_disconnect} ? $r->{on_disconnect}[0] : undef, $e->{reason}, 'on_disconnect fired with the token')
            if $e->{disconnect};
        is($warns, [], 'no spurious server log after the client went away (S5)');
    };
}

$| = 1;
for my $row (@rows) {
    my ($label, $run) = @$row;
    my $r;
    my $warns = warnings { $r = $run->() };
    check_cell($label, $r, $warns);
}

done_testing;
