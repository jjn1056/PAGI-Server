use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use Time::HiRes ();
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
# Test: a graceful shutdown lets an in-flight HTTP/2 stream finish
# ============================================================
# RFC 9113 section 6.8: a server ending a connection announces GOAWAY naming
# the last stream it took up, and the peer may still finish the streams at or
# below that id. The announcement is only half a promise -- the server has to
# stay long enough for those streams to end.
#
# The drain sweep asks each connection whether it has work in flight. An
# HTTP/2 connection answers for its stream table, so a connection still
# producing a response is left alone until its last stream ends (or
# shutdown_timeout expires), while an idle one is closed at once, exactly as a
# keep-alive HTTP/1.1 connection is.
#
# The HTTP/1.1 twin -- an active request completes during shutdown -- is pinned
# by t/18-graceful-shutdown.t and is not repeated here.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# Every server built here logs into this collector rather than STDERR, so a
# case can assert over what was logged as well as over what was sent.
my @LOG;

sub create_h2_connection {
    my (%o) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app    = $o{app};
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, http2 => 1,
        shutdown_timeout => $o{shutdown_timeout} // 10,
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

# What the client saw, in the order it saw it: the GOAWAY announcement, the
# close of its own request stream (the response completing), and the server's
# EOF. The client never closes its end, so an 'eof' in here is the server's
# close and nothing else.
my @EVENT;
my $BODY = '';

sub create_client {
    require Net::HTTP2::nghttp2::Session;
    @EVENT = ();
    $BODY  = '';
    return Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { 0 },
        on_frame_recv      => sub { push @EVENT, 'goaway' if $_[0]{type} == 7; 0 },
        on_data_chunk_recv => sub { $BODY .= $_[1]; 0 },
        on_stream_close    => sub { push @EVENT, 'response'; 0 },
    });
}

sub first_at {
    my ($what) = @_;
    for my $i (0 .. $#EVENT) { return $i if $EVENT[$i] eq $what }
    return -1;
}

# Run the loop and the client together until $cond holds or the server closes,
# bounded by wall clock rather than by a round count.
sub pump {
    my ($client, $sock, $seconds, $cond) = @_;
    my $deadline = Time::HiRes::time() + $seconds;
    while (Time::HiRes::time() < $deadline) {
        $loop->loop_once(0.02);
        my $buf = '';
        my $n = $sock->sysread($buf, 65536);
        if (defined $n && $n == 0) { push @EVENT, 'eof'; last }
        if (length $buf) {
            eval { $client->mem_recv($buf) };
        }
        my $out = eval { $client->mem_send };
        $sock->syswrite($out) if defined $out && length $out;
        last if $cond && $cond->();
    }
    return;
}

sub handshake {
    my ($client, $sock) = @_;
    $loop->loop_once(0.1);
    my $settings = '';
    $sock->sysread($settings, 65536);
    $client->send_connection_preface;
    $sock->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($settings) if length $settings;
    pump($client, $sock, 0.5);
    return;
}

sub submit_get {
    my ($client, $sock, $path) = @_;
    my $sid = $client->submit_request(
        method => 'GET', path => $path, scheme => 'http', authority => 'localhost');
    my $out = $client->mem_send;
    $sock->syswrite($out) if length $out;
    return $sid;
}

sub errors_since {
    my ($mark) = @_;
    return grep { ($_->{level} // '') =~ /^(warn|error)$/ } @LOG[$mark .. $#LOG];
}

# The application of the in-flight case: it takes 0.3 s to answer, so the
# shutdown lands while the response is still being produced.
my $REACHED_DELAY = 0;

my $slow_app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'http';
    await $receive->();
    $REACHED_DELAY = 1;
    await $loop->delay_future(after => 0.3);
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'in flight' });
    return;
};

my $prompt_app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'http';
    await $receive->();
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'done' });
    return;
};

# ============================================================
# (1) a stream still in flight finishes, and the server closes after it
# ============================================================
subtest 'a shutdown lets an in-flight HTTP/2 stream finish before closing' => sub {
    my $mark = scalar @LOG;
    $REACHED_DELAY = 0;

    my ($conn, $stream_io, $sock, $server)
        = create_h2_connection(app => $slow_app, shutdown_timeout => 10);
    my $client = create_client;
    handshake($client, $sock);
    submit_get($client, $sock, '/slow');
    pump($client, $sock, 2, sub { $REACHED_DELAY });
    ok($REACHED_DELAY, 'the application is inside its delay when the shutdown starts');

    my $t0 = Time::HiRes::time();
    my $shutdown = $server->shutdown;
    pump($client, $sock, 5);
    my $elapsed = Time::HiRes::time() - $t0;
    eval { $shutdown->get };

    is($EVENT[0], 'goaway', 'the client is told the connection is going away first');
    ok(first_at('response') > first_at('goaway'),
        'and the response it already had in flight arrives after that announcement')
        or diag('events: ' . join(',', @EVENT));
    is($BODY, 'in flight', 'the whole body was delivered');
    is($EVENT[-1], 'eof', 'the server closed the connection, after the stream ended');
    cmp_ok($elapsed, '<', 2, 'and it closed on the stream, not on shutdown_timeout');

    is(scalar(errors_since($mark)), 0, 'the shutdown logged nothing at warn or error')
        or diag(join qq{\n}, map { "$_->{level}: " . ($_->{message} // q{}) } errors_since($mark));

    $stream_io->close_now;
    eval { $loop->remove($server) };
};

# ============================================================
# (2) control: an idle HTTP/2 connection is closed at once
# ============================================================
subtest 'an idle HTTP/2 connection is closed at once' => sub {
    my $mark = scalar @LOG;

    my ($conn, $stream_io, $sock, $server)
        = create_h2_connection(app => $prompt_app, shutdown_timeout => 10);
    my $client = create_client;
    handshake($client, $sock);
    submit_get($client, $sock, '/quick');
    pump($client, $sock, 2, sub { first_at('response') >= 0 });
    is($BODY, 'done', 'the request was answered before the shutdown');
    # The stream's table entry is released a turn after the stream closes.
    $loop->loop_once(0.02) for 1 .. 3;

    my $t0 = Time::HiRes::time();
    my $shutdown = $server->shutdown;
    pump($client, $sock, 5);
    my $elapsed = Time::HiRes::time() - $t0;
    eval { $shutdown->get };

    is($EVENT[-1], 'eof', 'the connection with nothing in flight is closed');
    cmp_ok($elapsed, '<', 1, 'at once, without waiting for anything');
    ok(first_at('goaway') >= 0, 'and it is still told why')
        or diag('events: ' . join(',', @EVENT));

    is(scalar(errors_since($mark)), 0, 'the shutdown logged nothing at warn or error')
        or diag(join qq{\n}, map { "$_->{level}: " . ($_->{message} // q{}) } errors_since($mark));

    $stream_io->close_now;
    eval { $loop->remove($server) };
};

done_testing;
