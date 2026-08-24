use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use POSIX qw(WNOHANG);
use File::Temp qw(tempfile);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# ============================================================
# Test: Declining an SSE request over HTTP/2 (sse.http.response.*)
# ============================================================
# Before sse.start, an application may DECLINE the stream and return a
# normal HTTP response (404/401/204/...) via sse.http.response.start /
# sse.http.response.body. First-send-wins: a stream event after a decline,
# and a decline after sse.start, MUST raise.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app   => $args{app} // sub { },
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
        http2 => 1,
        %args,
    );
    $loop->add($server);
    return $server;
}

sub create_h2_connection {
    my (%overrides) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0)
        or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app    = $overrides{app}    // sub { };
    my $server = $overrides{server} // create_test_server(app => $app);
    my $stream = IO::Async::Stream->new(
        read_handle  => $sock_a,
        write_handle => $sock_a,
        on_read      => sub { 0 },
    );
    my $conn = PAGI::Server::Connection->new(
        stream        => $stream,
        app           => $app,
        protocol      => $protocol,
        server        => $server,
        h2_protocol   => $server->{http2_protocol},
        alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;
    return ($conn, $stream, $sock_b, $server);
}

sub create_client {
    my (%overrides) = @_;
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(
        callbacks => {
            on_begin_headers   => sub { 0 },
            on_header          => $overrides{on_header}          // sub { 0 },
            on_frame_recv      => sub { 0 },
            on_data_chunk_recv => $overrides{on_data_chunk_recv} // sub { 0 },
            on_stream_close    => sub { 0 },
        },
    );
}

sub complete_h2_handshake {
    my ($client, $client_sock) = @_;
    $loop->loop_once(0.1);
    my $server_settings = '';
    $client_sock->sysread($server_settings, 4096);
    $client->send_connection_preface;
    my $data = $client->mem_send;
    $client_sock->syswrite($data);
    $loop->loop_once(0.1);
    $client->mem_recv($server_settings);
    $loop->loop_once(0.1);
    my $ack = '';
    $client_sock->sysread($ack, 4096);
    $client->mem_recv($ack) if length($ack);
    my $client_ack = $client->mem_send;
    $client_sock->syswrite($client_ack) if length($client_ack);
    $loop->loop_once(0.1);
    my $extra = '';
    $client_sock->sysread($extra, 4096);
    $client->mem_recv($extra) if length($extra);
}

sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    $rounds //= 10;
    for (1 .. $rounds) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

sub decline_request {
    my (%args) = @_;
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $args{app});

    my %headers;
    my $body = '';
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $d)    = @_; $body .= $d;         return 0 },
    );
    complete_h2_handshake($client, $client_sock);
    $client->submit_request(
        method    => 'GET',
        path      => $args{path} // '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    $stream_io->close_now;
    $loop->remove($server);
    return (\%headers, $body);
}

subtest 'sse.http.response.* returns a plain HTTP 404 (declines the stream)' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.http.response.start', status => 404,
                        headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'sse.http.response.body', body => 'No such stream', more => 0 });
        return;
    };
    my ($headers, $body) = decline_request(app => $app);
    is($headers->{':status'}, '404', '404 status, not 200');
    is($headers->{'content-type'}, 'text/plain', 'plain content-type, NOT event-stream');
    like($body, qr/No such stream/, 'decline body delivered');
};

subtest 'multi-chunk decline body buffers until more=>0' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.http.response.start', status => 401,
                        headers => [['x-deny', 'auth']] });
        await $send->({ type => 'sse.http.response.body', body => 'go ',   more => 1 });
        await $send->({ type => 'sse.http.response.body', body => 'away', more => 0 });
        return;
    };
    my ($headers, $body) = decline_request(app => $app);
    is($headers->{':status'}, '401', '401 status');
    is($headers->{'x-deny'},  'auth', 'custom header present');
    is($body, 'go away', 'body chunks concatenated before submission');
};

subtest 'first-send-wins: stream after decline, and decline after stream, raise' => sub {
    my ($after_decline_raised, $after_start_raised) = (0, 0);

    my $app1 = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.http.response.start', status => 404, headers => [] });
        eval { await $send->({ type => 'sse.send', data => 'x' }); 1 } or $after_decline_raised = 1;
        await $send->({ type => 'sse.http.response.body', body => '', more => 0 });
        return;
    };
    decline_request(app => $app1);
    ok($after_decline_raised, 'sse.send after sse.http.response.start raised');

    my $app2 = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.start', status => 200 });
        eval { await $send->({ type => 'sse.http.response.start', status => 404, headers => [] }); 1 }
            or $after_start_raised = 1;
        return;
    };
    decline_request(app => $app2);
    ok($after_start_raised, 'sse.http.response.start after sse.start raised');
};

# ============================================================
# Task 5 (Phase 6 protocol-alignment): dead-stream fallback safety
# ============================================================
# Phase 5 Task 3 disclosed a SIGABRT class: an h2 SSE app that returns
# without ever sending sse.start, on a stream the client has ALREADY
# RST_STREAM'd, races the dispatch wrapper's fallback against
# _h2_on_close's deferred (loop->later) deletion of the h2_streams entry.
# SSE scopes carry no pagi.connection, so the wrapper's only liveness
# signal is `exists $h2_streams{$stream_id}` -- which is still true in
# that window, even though nghttp2 itself already forgot the stream. The
# wrapper then calls submit_response on a stream nghttp2 no longer owns,
# which aborts the process at the C level (uncatchable by the wrapper's
# own eval).
#
# This reproduction runs the whole client/server exchange in a forked
# child so a real SIGABRT doesn't take the test suite down with it: the
# parent inspects the child's wait status (signal 6 == SIGABRT) and its
# captured STDERR.
subtest 'client RST landing before the deferred stream-close cleanup runs does not synthesize a response on the dead stream' => sub {
    my ($result_fh, $result_file) = tempfile(UNLINK => 1);
    close $result_fh;
    my ($stderr_fh, $stderr_file) = tempfile(UNLINK => 1);
    close $stderr_fh;

    my $pid = fork();
    die "fork failed: $!" unless defined $pid;

    if ($pid == 0) {
        # Child. Route STDERR to a file the parent can inspect after
        # waitpid (the process may not survive to print anything else),
        # and use POSIX::_exit throughout so no Test2 global-destruction
        # / END-block bookkeeping runs in this fork.
        require POSIX;
        open(STDERR, '>', $stderr_file) or POSIX::_exit(97);
        STDERR->autoflush(1);

        open(my $rf, '>', $result_file) or POSIX::_exit(98);
        $rf->autoflush(1);

        my $reached_return = 0;
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            # First receive(): sse.request. A GET carries no body, so
            # body_complete is already true at dispatch and this
            # resolves immediately.
            await $receive->();
            # Second receive(): blocks until the client's RST wakes it
            # with sse.disconnect.
            await $receive->();
            $reached_return = 1;
            return;   # never sent sse.start
        };

        my ($conn, $stream_io, $client_sock, $server) =
            create_h2_connection(app => $app);

        my $client = create_client();
        complete_h2_handshake($client, $client_sock);

        my $stream_id = $client->submit_request(
            method    => 'GET',
            path      => '/events',
            scheme    => 'http',
            authority => 'localhost',
            headers   => [['accept', 'text/event-stream']],
        );
        $client_sock->syswrite($client->mem_send);

        # Drive the connection until the stream is dispatched and the app
        # is blocked on its second receive() await.
        exchange_frames($client, $client_sock, 5);

        print $rf "PRE_RST_STREAMS=" . scalar(keys %{$conn->{h2_streams}}) . "\n";
        print $rf "PRE_RST_REACHED_RETURN=$reached_return\n";

        # RST the stream and process ONLY that single read -- deliberately
        # not giving the loop a further turn -- so the interleaving lands
        # exactly where P5T3 hit it: _h2_on_close runs synchronously
        # (nghttp2 has already forgotten this stream) but the deferred
        # `h2_streams` delete (loop->later) has not run yet.
        use constant H2_CANCEL_CODE => 8;   # RST_STREAM CANCEL (RFC 9113)
        $client->submit_rst_stream($stream_id, H2_CANCEL_CODE);
        $client_sock->syswrite($client->mem_send);
        $loop->loop_once(0.2);

        print $rf "POST_RST_STREAMS=" . scalar(keys %{$conn->{h2_streams}}) . "\n";
        print $rf "POST_RST_REACHED_RETURN=$reached_return\n";

        # Let the rest of the loop (including the deferred delete) settle
        # normally.
        for (1 .. 10) {
            $loop->loop_once(0.05);
        }
        print $rf "SETTLED_STREAMS=" . scalar(keys %{$conn->{h2_streams}}) . "\n";
        print $rf "DONE\n";
        close $rf;

        $stream_io->close_now;
        $loop->remove($server);
        POSIX::_exit(0);
    }

    # Parent: bound the wait so a hang in the child (rather than a crash)
    # can't stall the suite, then classify the exit.
    my $waited = 0;
    my $reaped = 0;
    while ($waited < 10) {
        $reaped = waitpid($pid, WNOHANG);
        last if $reaped == $pid;
        select(undef, undef, undef, 0.1);
        $waited += 0.1;
    }
    if ($reaped != $pid) {
        kill 'KILL', $pid;
        waitpid($pid, 0);
        fail('child process did not finish within 10s (hung rather than crashed or completed)');
        return;
    }
    my $status    = $?;
    my $signaled  = ($status & 127) != 0;
    my $signal    = $status & 127;
    my $exit_code = $status >> 8;

    open(my $rf, '<', $result_file) or die "can't read result file: $!";
    my %result;
    while (my $line = <$rf>) {
        chomp $line;
        my ($k, $v) = split /=/, $line, 2;
        $result{$k} = defined($v) ? $v : 1;
    }
    close $rf;

    open(my $sf, '<', $stderr_file) or die "can't read stderr file: $!";
    my $child_stderr = do { local $/; <$sf> } // '';
    close $sf;

    ok(!$signaled,
        'server process was not killed by a fatal signal (no SIGABRT synthesizing a response on a dead stream)')
        or diag("child terminated by signal $signal; child stderr:\n$child_stderr");

    is($result{PRE_RST_REACHED_RETURN}, 0,
        'sanity: app had not yet returned before the RST landed (right interleaving provoked)');

    ok(exists $result{DONE}, 'child ran to completion without dying mid-way')
        or diag("captured so far: ", join(', ', map { "$_=$result{$_}" } sort keys %result),
                "; child stderr:\n$child_stderr");

    is($exit_code, 0, 'server process exited cleanly') if !$signaled && exists $result{DONE};

    unlike($child_stderr, qr/PAGI application returned without starting a response/,
        'no spurious "returned without starting a response" warning for an already-reset stream');
    unlike($child_stderr, qr/PAGI application error/,
        'no spurious application-error warning either');

    is($result{SETTLED_STREAMS}, 0, "reset stream's h2_streams entry is reclaimed, no leak")
        if exists $result{SETTLED_STREAMS};
};

done_testing;
