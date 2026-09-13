use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use Scalar::Util qw(refaddr);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

# ============================================================
# Test: nghttp2's own diagnostics reach the scope, and a shutdown
#       announces GOAWAY
# ============================================================
# nghttp2 parses the peer's frames, so when the peer breaks HTTP/2 it is
# nghttp2 -- not PAGI -- that knows which rule was broken. Net::HTTP2::nghttp2
# hands that over through on_invalid_frame_recv (a frame it rejected, with the
# NGHTTP2_ERR_* code) and on_error (a human-readable diagnostic). Without
# them the server closes such a connection as an ordinary peer disconnect with
# no detail at all, which is what these cases pin against.
#
# Www.pod "Connection Object Interface" / Compliance.pod "disconnect_detail
# contents": a server-detected abnormal end names the violated rule. For an
# HTTP/2 connection nghttp2 tore down, that name is nghttp2's.
#
# The third case is the other half of the same honesty: RFC 9113 section 6.8
# says a server ending a connection SHOULD send GOAWAY naming the last stream
# it took up, so the peer knows which requests it may retry elsewhere. A
# shutdown used to terminate the session locally, which puts no frame on the
# wire at all.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# Every server built here logs into this collector rather than STDERR, so the
# cases can assert over what was logged as well as over what was sent.
my @LOG;

# An application resumed from an already-resolved Future never returns control
# to the event loop, so an alarm is the only bound a pump can impose.
my $ALARM_FIRED = 0;

sub bounded {
    my ($body, $seconds) = @_;
    local $SIG{ALRM} = sub { $ALARM_FIRED = 1; die "pump alarm\n" };
    alarm($seconds // 10);
    my $ok  = eval { $body->(); 1 };
    my $err = $@;
    alarm(0);
    die $err if !$ok && $err ne "pump alarm\n";
    return $ALARM_FIRED;
}

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
    );
    $loop->add($server);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;
    # The listener is what registers a connection and marks the server running;
    # this harness builds the connection directly, so it does both itself --
    # PAGI::Server::shutdown returns at once on a server that never ran, and
    # _drain_connections only sees connections the server knows about.
    $server->{connections}{refaddr($conn)} = $conn;
    $server->{running} = 1;
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

# Everything the server put on the wire, in order, so a case can assert over
# frames the client session does not surface (GOAWAY's last_stream_id lives in
# the payload, and the binding's frame hashref does not carry it).
my $RAW = '';

sub pump {
    my ($client, $sock, $rounds, $cond) = @_;
    return bounded(sub {
        for (1 .. ($rounds // 20)) {
            $loop->loop_once(0.02);
            last unless defined fileno($sock);
            my $buf = '';
            $sock->sysread($buf, 65536);
            if (length $buf) {
                $RAW .= $buf;
                eval { $client->mem_recv($buf) };
            }
            my $out = eval { $client->mem_send };
            $sock->syswrite($out) if defined $out && length $out;
            last if $cond && $cond->();
        }
    });
}

sub handshake {
    my ($client, $sock) = @_;
    $RAW = '';
    $loop->loop_once(0.1);
    my $settings = '';
    $sock->sysread($settings, 65536);
    $RAW .= $settings;
    $client->send_connection_preface;
    $sock->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($settings) if length $settings;
    pump($client, $sock, 5);
    return;
}

# RFC 9113 section 4.1 frame header: 24-bit length, type, flags, 31-bit id.
sub parse_frames {
    my ($bytes) = @_;
    my @frames;
    my $i = 0;
    while ($i + 9 <= length $bytes) {
        my $len = unpack('N', "\0" . substr($bytes, $i, 3));
        last if $i + 9 + $len > length $bytes;
        push @frames, {
            type      => ord(substr($bytes, $i + 3, 1)),
            flags     => ord(substr($bytes, $i + 4, 1)),
            stream_id => unpack('N', substr($bytes, $i + 5, 4)) & 0x7fffffff,
            payload   => substr($bytes, $i + 9, $len),
        };
        $i += 9 + $len;
    }
    return @frames;
}

sub raw_frame {
    my ($type, $flags, $stream_id, $payload) = @_;
    return substr(pack('N', length $payload), 1, 3)
         . chr($type) . chr($flags)
         . pack('N', $stream_id & 0x7fffffff) . $payload;
}

sub errors_since {
    my ($mark) = @_;
    return grep { ($_->{level} // '') =~ /^(warn|error)$/ } @LOG[$mark .. $#LOG];
}

sub shutdown_server {
    my ($server) = @_;
    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
}

# A POST whose request half the client never finishes, so the scope stays open
# and its application stays parked on receive() until the connection ends.
sub submit_open_request {
    my ($client, $sock, $path, %o) = @_;
    my $sid = $client->submit_request(
        method => 'POST', path => $path, scheme => 'http', authority => 'localhost',
        headers => $o{headers} // [], body => sub { return undef },
    );
    my $out = $client->mem_send;
    $sock->syswrite($out) if length $out;
    return $sid;
}

# The application every case uses: it records its connection object, parks,
# and records how the scope ended once it is woken.
my %ENDED;

my $parked_app = async sub {
    my ($scope, $receive, $send) = @_;
    my $conn = $scope->{'pagi.connection'};
    my $path = $scope->{path};
    await $receive->();
    $ENDED{$path} = {
        connected => $conn->is_connected,
        reason    => $conn->disconnect_reason,
        detail    => $conn->disconnect_detail,
    };
    return;
};

# ============================================================
# (1) a frame nghttp2 rejects names itself
# ============================================================
subtest 'a frame nghttp2 rejects reaches the scope as a protocol error' => sub {
    %ENDED = (); $ALARM_FIRED = 0;
    my $mark = scalar @LOG;

    my ($conn, $stream_io, $sock, $server) = create_h2_connection(app => $parked_app);
    my $client = create_client;
    handshake($client, $sock);
    my $sid = submit_open_request($client, $sock, '/rejected');
    pump($client, $sock, 10);

    # RFC 9113 section 6.9: a WINDOW_UPDATE increment of 0 is a protocol
    # error. nghttp2 rejects the frame and ends the session itself; no
    # well-behaved client session would produce it, so it goes on the wire raw.
    $sock->syswrite(raw_frame(8, 0, $sid, pack('N', 0)));
    pump($client, $sock, 20, sub { $ENDED{'/rejected'} });

    my $end = $ENDED{'/rejected'} // {};
    is($end->{connected}, 0, 'the scope ended');
    is($end->{reason}, 'protocol_error', 'the peer broke the protocol, and is named for it');
    like($end->{detail}, qr/WINDOW_UPDATE/,
        'the detail names the frame type nghttp2 rejected');
    like($end->{detail}, qr/NGHTTP2_ERR_PROTO/,
        'and names nghttp2\'s error by the constant the binding exports');

    is(scalar(errors_since($mark)), 0, 'a misbehaving client logged nothing')
        or diag(join qq{\n}, map { "$_->{level}: " . ($_->{message} // q{}) } errors_since($mark));
    is($ALARM_FIRED, 0, 'no pump alarm');

    $stream_io->close_now;
    shutdown_server($server);
};

# ============================================================
# (2) nghttp2's human-readable diagnostic reaches the scope
# ============================================================
subtest 'nghttp2\'s own message reaches the scope that was open' => sub {
    %ENDED = (); $ALARM_FIRED = 0;
    my $mark = scalar @LOG;

    my ($conn, $stream_io, $sock, $server) = create_h2_connection(app => $parked_app);
    my $client = create_client;
    handshake($client, $sock);
    my $sid = submit_open_request($client, $sock, '/open');
    pump($client, $sock, 10);

    # RFC 9113 section 8.2.2 forbids connection-specific header fields. nghttp2
    # rejects this request without ever delivering it, so the only scope left
    # to report the reason is the one already open on the first stream.
    submit_open_request($client, $sock, '/forbidden',
        headers => [['connection', 'keep-alive']]);
    pump($client, $sock, 20, sub { $ENDED{'/open'} });

    my $end = $ENDED{'/open'} // {};
    is($end->{connected}, 0, 'the open scope ended');
    is($end->{reason}, 'protocol_error', 'as a protocol error');
    like($end->{detail}, qr/\bconnection\b/,
        'the detail carries nghttp2\'s own account of the offending field')
        or diag('detail: ' . ($end->{detail} // '(undef)'));

    is(scalar(errors_since($mark)), 0, 'a misbehaving client logged nothing')
        or diag(join qq{\n}, map { "$_->{level}: " . ($_->{message} // q{}) } errors_since($mark));
    is($ALARM_FIRED, 0, 'no pump alarm');

    $stream_io->close_now;
    shutdown_server($server);
};

# ============================================================
# (3) a graceful shutdown announces GOAWAY
# ============================================================
subtest 'a graceful shutdown announces GOAWAY naming the last stream taken up' => sub {
    %ENDED = (); $ALARM_FIRED = 0;
    my $mark = scalar @LOG;

    my ($conn, $stream_io, $sock, $server) = create_h2_connection(app => $parked_app);
    my $client = create_client;
    handshake($client, $sock);
    my $first  = submit_open_request($client, $sock, '/one');
    my $second = submit_open_request($client, $sock, '/two');
    pump($client, $sock, 10);

    my $shutdown = $server->shutdown;
    pump($client, $sock, 20, sub { $ENDED{'/two'} });
    eval { $shutdown->get };

    my ($goaway) = grep { $_->{type} == 7 } parse_frames($RAW);
    ok($goaway, 'the client was told the connection is going away')
        or diag('frames: ' . join(',', map { "t$_->{type}" } parse_frames($RAW)));
    SKIP: {
        skip 'no GOAWAY to read', 2 unless $goaway;
        is(unpack('N', substr($goaway->{payload}, 0, 4)) & 0x7fffffff, $second,
            'GOAWAY names the highest stream this server took up');
        is(unpack('N', substr($goaway->{payload}, 4, 4)), 0,
            'and ends it with NO_ERROR -- a shutdown is nobody\'s fault');
    }

    is($ENDED{'/two'}{reason}, 'server_shutdown', 'the scope ends as a shutdown');
    is(scalar(errors_since($mark)), 0, 'the shutdown logged nothing at warn or error')
        or diag(join qq{\n}, map { "$_->{level}: " . ($_->{message} // q{}) } errors_since($mark));
    is($ALARM_FIRED, 0, 'no pump alarm');

    $stream_io->close_now;
    eval { $loop->remove($server) };
};

done_testing;
