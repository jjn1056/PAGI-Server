use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM SOL_SOCKET SO_SNDBUF SO_RCVBUF);
use Time::HiRes qw(time);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.014+ required)');
}

# What the server holds for an HTTP/2 connection is bounded by its own
# watermarks, not by the window the client advertises (CVE-2019-9517,
# "Internal Data Buffering"). A client may open a large flow-control window
# and then read nothing; the server must not serialize everything that window
# allows into its own memory. Once the connection's write buffer reaches
# write_high_watermark, the server stops taking output from nghttp2; each
# stream's queue then fills to write_high_watermark and its sends wait, and
# everything resumes when the buffer drains below write_low_watermark.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Net::HTTP2::nghttp2::Session;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;
my @log;
my @warnings;
$SIG{__WARN__} = sub { push @warnings, $_[0] };

my $HIGH = 8192;
my $LINES = 20_000;

# Line $n of a body: 100 bytes, numbered, so order can be checked.
sub line { sprintf("%07d", $_[0]) . ('x' x 92) . "\n" }

sub create_h2_connection {
    my ($app) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0)
        or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);

    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
        http2 => 1, shutdown_timeout => 1, logger => sub { push @log, $_[0] },
        write_high_watermark => $HIGH, write_low_watermark => 2048,
    );
    $loop->add($server);

    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 },
    );
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
        write_high_watermark => $HIGH, write_low_watermark => 2048,
    );
    $server->add_child($stream);
    $conn->start;
    # The most a full socket pair can hold between the two ends.
    my $kernel = unpack('i', getsockopt($sock_a, SOL_SOCKET, SO_SNDBUF))
               + unpack('i', getsockopt($sock_b, SOL_SOCKET, SO_RCVBUF));
    return ($sock_b, $server, $conn, $kernel);   # the caller keeps the connection alive
}

sub stop_server {
    my ($server) = @_;
    $server->shutdown->get;
    $loop->remove($server);
}

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 10);
    $loop->loop_once(0.01) until $cond->() || time > $deadline;
    return $cond->();
}

# A client that advertises a 16 MiB stream window and a 1 GiB connection
# window, as curl does with 10 MiB and 1000 MiB, records what it receives,
# and reads only when told to.
sub big_window_client {
    my ($client_sock) = @_;
    my %seen = (body => {}, closed => {}, ping_acks => 0);
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { 0 },
        on_frame_recv      => sub {
            my ($frame) = @_;
            $seen{ping_acks}++ if $frame->{type} == 6 && ($frame->{flags} & 0x1);
            0;
        },
        on_data_chunk_recv => sub { $seen{body}{ $_[0] } .= $_[1]; 0 },
        on_stream_close    => sub { $seen{closed}{ $_[0] } = $_[1]; 0 },
    });
    # The server's SETTINGS are fed to the client only after its own preface,
    # so the client's first frame is its SETTINGS, not an ACK.
    $loop->loop_once(0.1);
    $client_sock->sysread(my $server_settings, 65536);
    $client->send_connection_preface(initial_window_size => 2**24 - 1);
    $client->submit_window_update(0, 2**30);
    $client_sock->syswrite($client->mem_send);
    $client->mem_recv($server_settings) if length($server_settings // '');
    for (1 .. 3) {
        $loop->loop_once(0.1);
        read_into($client, $client_sock);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length $out;
    }
    return ($client, \%seen);
}

# Everything waiting on the client's socket, fed to the client.
sub read_into {
    my ($client, $client_sock) = @_;
    while (defined(my $n = $client_sock->sysread(my $buf, 65536))) {
        last unless $n;
        $client->mem_recv($buf);
    }
}

sub submit_get {
    my ($client, $client_sock, @headers) = @_;
    my $sid = $client->submit_request(
        method => 'GET', path => '/', scheme => 'https', authority => 'localhost',
        headers => [@headers],
    );
    $client_sock->syswrite($client->mem_send);
    return $sid;
}

# The client reads and answers (WINDOW_UPDATEs) until $done says so.
sub read_until {
    my ($client, $client_sock, $done, $secs) = @_;
    return pump_until(sub {
        read_into($client, $client_sock);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length $out;
        return $done->();
    }, $secs // 30);
}

# Until the app's count of resolved sends stops moving for half a second.
sub until_parked {
    my ($count) = @_;
    my ($last, $since) = (-1, time);
    return pump_until(sub {
        ($last, $since) = ($count->(), time) if $count->() != $last;
        return $count->() > 0 && time - $since > 0.5;
    }, 30);
}

# An HTTP response of $lines numbered lines, one send each, counting the
# sends that have resolved.
sub body_app {
    my ($resolved, $lines, $ended) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'http';
        $scope->{'pagi.connection'}->end_future->on_done(sub { $ended->{ $scope } = 1 }) if $ended;
        await $send->({ type => 'http.response.start', status => 200,
            headers => [['content-type', 'text/plain']] });
        for my $n (1 .. $lines) {
            await $send->({ type => 'http.response.body', body => line($n), more => 1 });
            $$resolved++;
        }
        await $send->({ type => 'http.response.body', body => '', more => 0 });
    };
}

subtest 'one stream: sends stop at the server\'s bound, not the client\'s window' => sub {
    my $resolved = 0;
    my ($client_sock, $server, $h2conn, $kernel) = create_h2_connection(body_app(\$resolved, $LINES));
    my ($client, $seen) = big_window_client($client_sock);
    my $sid = submit_get($client, $client_sock);
    my $bound = 2 * $HIGH + $kernel + 16384;

    ok(until_parked(sub { $resolved }), 'the body parks');
    cmp_ok($resolved, '<', $LINES, 'it did not run to its end while the client read nothing');
    cmp_ok($resolved * 100, '<=', $bound, "at most $bound bytes left the app")
        or diag "resolved=$resolved";

    ok(read_until($client, $client_sock, sub { exists $seen->{closed}{$sid} }), 'once the client reads, the stream completes');
    is($seen->{closed}{$sid}, 0, 'cleanly');
    ok(($seen->{body}{$sid} // '') eq join('', map { line($_) } 1 .. $LINES), 'every line arrived, in order')
        or diag 'got ' . length($seen->{body}{$sid} // '') . ' bytes';
    stop_server($server);
};

subtest 'twenty streams: each adds one stream queue' => sub {
    my $resolved = 0;
    my $lines = 5_000;
    my ($client_sock, $server, $h2conn, $kernel) = create_h2_connection(body_app(\$resolved, $lines));
    my ($client, $seen) = big_window_client($client_sock);
    my @sids = map { submit_get($client, $client_sock) } 1 .. 20;
    my $bound = 2 * $HIGH + $kernel + 16384 + 20 * ($HIGH + 100);

    ok(until_parked(sub { $resolved }), 'the bodies park');
    cmp_ok($resolved, '<', 20 * $lines, 'they did not run to their end');
    cmp_ok($resolved * 100, '<=', $bound, "at most $bound bytes left the apps")
        or diag "resolved=$resolved";

    ok(read_until($client, $client_sock, sub { 20 == grep { exists $seen->{closed}{$_} } @sids }),
        'once the client reads, every stream completes');
    my $want = join '', map { line($_) } 1 .. $lines;
    is([grep { ($seen->{body}{$_} // '') ne $want } @sids], [], 'every stream got every line, in order');
    stop_server($server);
};

subtest 'SSE: the same bound' => sub {
    my $resolved = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'sse';
        await $send->({ type => 'sse.start', status => 200 });
        for my $n (1 .. $LINES) {
            await $send->({ type => 'sse.send', data => ('x' x 100) });
            $resolved++;
        }
        await $send->({ type => 'sse.close' });
    };
    my ($client_sock, $server, $h2conn, $kernel) = create_h2_connection($app);
    my ($client, $seen) = big_window_client($client_sock);
    my $sid = submit_get($client, $client_sock, ['accept', 'text/event-stream']);
    my $bound = 2 * $HIGH + $kernel + 16384;

    ok(until_parked(sub { $resolved }), 'the stream parks');
    cmp_ok($resolved, '<', $LINES, 'it did not run to its end');
    cmp_ok($resolved * 100, '<=', $bound, "at most $bound bytes of events left the app")
        or diag "resolved=$resolved";

    ok(read_until($client, $client_sock, sub { exists $seen->{closed}{$sid} }), 'once the client reads, the stream completes');
    stop_server($server);
};

subtest 'a PING while output is held is answered once the client reads' => sub {
    my $resolved = 0;
    my ($client_sock, $server, $h2conn) = create_h2_connection(body_app(\$resolved, $LINES));
    my ($client, $seen) = big_window_client($client_sock);
    submit_get($client, $client_sock);
    ok(until_parked(sub { $resolved }), 'the body parks');

    $client->submit_ping(0, '12345678');
    $client_sock->syswrite($client->mem_send);
    ok(read_until($client, $client_sock, sub { $seen->{ping_acks} }), 'the PING is answered');
    stop_server($server);
};

subtest 'the client closes while output is held' => sub {
    my $resolved = 0;
    my %ended;
    my ($client_sock, $server, $h2conn) = create_h2_connection(body_app(\$resolved, $LINES, \%ended));
    my ($client, $seen) = big_window_client($client_sock);
    submit_get($client, $client_sock);
    ok(until_parked(sub { $resolved }), 'the body parks');

    @log = ();
    @warnings = ();
    close $client_sock;
    ok(pump_until(sub { keys %ended }, 10), "the stream's scope ends");
    pump_until(sub { 0 }, 0.5);   # let anything the drain scheduled run
    is([grep { $_->{level} =~ /^(?:warn|error|fatal)$/ } @log], [], 'nothing logged at warn or above');
    is(\@warnings, [], 'no Perl warnings');
    stop_server($server);
};

is(\@warnings, [], 'no Perl warnings anywhere');

done_testing;
