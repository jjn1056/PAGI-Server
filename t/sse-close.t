use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

eval { require Future::IO::Impl::IOAsync; 1 }
    or plan skip_all => 'Future::IO::Impl::IOAsync required for SSE tests';

use PAGI::Server;
use IO::Socket::INET;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Regression for the sse.close send-event (HTTP/1.1):
#   - sse.close is accepted (does not raise) and ends the STREAM immediately,
#     decoupled from the application returning;
#   - sending after sse.close raises (failed Future);
#   - the post-close event never reaches the wire;
#   - the stream is framed off with the chunked terminator, and the CONNECTION
#     stays alive: a clean end honors the "Connection: keep-alive" that
#     sse.start advertised (design 11.6, ratified by John 2026-08-22), so the
#     same socket serves the next request.
# Both an explicit sse.close and a plain return must end the stream identically
# on the wire (D3: keep return-to-end valid).

my $loop = IO::Async::Loop->new;

sub create_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, shutdown_timeout => 1,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

# Serves the follow-up request that proves the connection survived the stream.
async sub serve_plain {
    my ($scope, $receive, $send) = @_;
    await $receive->();
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [ ['content-type', 'text/plain'] ] });
    await $send->({ type => 'http.response.body', body => 'REUSED', more => 0 });
    return;
}

# Reads raw bytes until $stop matches, the server closes (EOF), or a deadline.
sub read_until {
    my ($sock, $stop, $timeout) = @_;
    my $buf = '';
    my $eof = 0;
    my $deadline = time + ($timeout // 5);
    while (time < $deadline) {
        my $chunk;
        my $n = sysread($sock, $chunk, 4096);
        if (defined $n && $n > 0) { $buf .= $chunk }
        elsif (defined $n && $n == 0) { $eof = 1; last }   # server closed the connection
        last if $buf =~ $stop;
        $loop->loop_once(0.05);
    }
    return ($buf, $eof);
}

# Open an SSE GET and read the stream up to its chunked terminator, then drain
# the loop so the app coroutine finishes. Returns (socket, wire, saw_eof); the
# socket is left open so callers can assert connection reuse on it.
sub sse_get {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    ) or return (undef, '', 0);
    print $sock "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nAccept: text/event-stream\r\n\r\n";
    $sock->blocking(0);

    my ($wire, $eof) = read_until($sock, qr/\r\n0\r\n\r\n/);
    $loop->loop_once(0.05) for 1 .. 20;   # let the app coroutine run to completion
    return ($sock, $wire, $eof);
}

# Issue an ordinary request on an already-used socket.
sub plain_request_on {
    my ($sock, $port) = @_;
    print $sock "GET /after HTTP/1.1\r\nHost: 127.0.0.1:$port\r\n\r\n";
    my ($wire) = read_until($sock, qr/REUSED/);
    return $wire;
}

subtest 'sse.close ends the stream; send-after-close raises' => sub {
    my ($close_ok, $post_close_raised) = (0, 0);

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return await serve_plain(@_) if ($scope->{type} // '') eq 'http';
        die "expected sse scope" unless ($scope->{type} // '') eq 'sse';

        await $send->({ type => 'sse.start', status => 200,
                        headers => [ ['content-type', 'text/event-stream'] ] });
        await $send->({ type => 'sse.send', event => 'tick', data => '1' });

        eval { await $send->({ type => 'sse.close', reason => 'done_testing' }); $close_ok = 1; 1 };

        # After sse.close, any further send MUST raise. The transport is still
        # open at this point (design 11.6), so this is the sequence machine
        # rejecting the send, not a closed-transport no-op.
        eval { await $send->({ type => 'sse.send', event => 'late', data => 'LATE' }); 1 }
            or $post_close_raised = 1;
    };

    my $server = create_server($app);
    my $port = $server->port;
    my ($sock, $wire, $eof) = sse_get($port);

    like($wire, qr/HTTP\/1\.1 200/,                 '200 OK');
    like($wire, qr/content-type:\s*text\/event-stream/i, 'event-stream content type');
    like($wire, qr/data: 1/,                        'tick event delivered before close');
    ok($close_ok,          'sse.close was accepted (did not raise)');
    ok($post_close_raised, 'sse.send after sse.close raised');
    unlike($wire, qr/LATE/, 'post-close event did not reach the wire');
    like($wire, qr/\r\n0\r\n\r\n/, 'sse.close framed the stream off with the chunked terminator');
    ok(!$eof,              'connection stays alive after sse.close (keep-alive honored)');
    like(plain_request_on($sock, $port), qr/REUSED/,
        'same socket serves an ordinary request after sse.close');
    close $sock;

    $server->shutdown->get;
};

subtest 'return-to-end still terminates the stream (D3)' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return await serve_plain(@_) if ($scope->{type} // '') eq 'http';
        await $send->({ type => 'sse.start', status => 200,
                        headers => [ ['content-type', 'text/event-stream'] ] });
        await $send->({ type => 'sse.send', event => 'tick', data => 'R' });
        return;   # no sse.close -- end by returning
    };

    my $server = create_server($app);
    my $port = $server->port;
    my ($sock, $wire, $eof) = sse_get($port);

    like($wire, qr/data: R/, 'event delivered');
    like($wire, qr/\r\n0\r\n\r\n/, 'returning framed the stream off with the chunked terminator');
    ok(!$eof,                'connection stays alive on return (keep-alive honored)');
    like(plain_request_on($sock, $port), qr/REUSED/,
        'same socket serves an ordinary request after the stream ended by returning');
    close $sock;

    $server->shutdown->get;
};

done_testing;
