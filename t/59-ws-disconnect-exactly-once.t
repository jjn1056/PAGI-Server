#!/usr/bin/env perl

# =============================================================================
# Test: h1 WebSocket delivers exactly one websocket.disconnect after a clean
# peer close (audit-found bug, live-probed).
#
# Reproduction: client sends a WS Close frame (code 1000, reason 'bye'), then
# closes the TCP connection ~50ms later (two separate events on the wire, not
# coalesced). Per Www.pod, a peer-initiated close must pass the peer's code
# and reason through to the app unchanged (no PAGI token substitution), and
# the app must receive exactly one websocket.disconnect for the scope.
#
# Root cause: _process_websocket_frames' opcode-8 branch queues the peer's
# websocket.disconnect event directly without marking the connection's
# disconnect-handled guard. The later on_closed callback (fired when the TCP
# connection actually closes) then runs _handle_disconnect_and_close(
# 'client_closed'), which -- finding the guard unset -- queues a SECOND,
# ghost websocket.disconnect (1006/'client_closed') that nobody asked for.
#
# Because a well-behaved app stops calling receive() the moment it sees the
# first websocket.disconnect, that ghost event is never drained -- it simply
# sits in the connection's receive_queue forever. That is the discriminator
# this test checks via white-box access to the Connection object: after the
# peer's close frame is processed, the guard must already be set (so the
# later TCP-close path delivers nothing further), and once the connection
# has fully torn down, receive_queue must be empty -- not holding a stray
# second entry.
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use Scalar::Util qw(refaddr);
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

# Helper to build a masked WebSocket frame (client -> server frames MUST be
# masked per RFC 6455). Mirrors t/14-websocket-invalid-utf8.t's helper.
sub make_websocket_frame {
    my ($opcode, $payload) = @_;

    my $frame = chr(0x80 | $opcode);

    my $len = length($payload);
    if ($len < 126) {
        $frame .= chr(0x80 | $len);
    }
    else {
        $frame .= chr(0x80 | 126) . pack('n', $len);
    }

    my $mask = pack('N', int(rand(0xFFFFFFFF)));
    $frame .= $mask;

    my $masked_payload = '';
    for my $i (0 .. length($payload) - 1) {
        $masked_payload .= chr(ord(substr($payload, $i, 1)) ^ ord(substr($mask, $i % 4, 1)));
    }
    $frame .= $masked_payload;

    return $frame;
}

subtest 'clean peer close delivers exactly one websocket.disconnect' => sub {
    my @seen;

    my $test_app = async sub {
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

        my $event = await $receive->();  # websocket.connect
        await $send->({ type => 'websocket.accept' });

        # Normal, well-behaved app: stop calling receive() the moment the
        # first websocket.disconnect arrives -- exactly one call, ever.
        my $ev = await $receive->();
        push @seen, $ev;

        # Stay alive well past the TCP close that follows the peer's Close
        # frame (~50ms later, per the reproduction) without calling receive()
        # again. This isolates the on_closed path as the only thing that can
        # still act after the Close frame was handled -- the app's own
        # session-complete teardown (which fires the moment this coroutine
        # returns) must not be what triggers a second delivery.
        await $loop->delay_future(after => 0.5);
    };

    my $server = PAGI::Server->new(
        app   => $test_app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    );

    SKIP: {
        skip "Cannot connect to server", 5 unless $sock;

        my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
        print $sock "GET / HTTP/1.1\r\n";
        print $sock "Host: 127.0.0.1:$port\r\n";
        print $sock "Upgrade: websocket\r\n";
        print $sock "Connection: Upgrade\r\n";
        print $sock "Sec-WebSocket-Key: $key\r\n";
        print $sock "Sec-WebSocket-Version: 13\r\n";
        print $sock "\r\n";

        $sock->blocking(0);
        my $response = '';
        my $deadline = time + 3;
        while (time < $deadline) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            if (defined $n && $n > 0) {
                $response .= $buf;
                last if $response =~ /\r\n\r\n/;
            }
            $loop->loop_once(0.1);
        }
        like($response, qr/HTTP\/1\.1 101/, 'WebSocket upgrade successful');

        # White-box handle on the live Connection object -- captured now,
        # before any close activity, so it survives the connection's later
        # removal from $server->{connections}.
        my ($conn) = values %{$server->{connections}};
        ok($conn, 'captured Connection object for white-box inspection');

        # Step 1: peer sends a clean Close frame (code 1000, reason 'bye').
        my $close_frame = make_websocket_frame(8, pack('n', 1000) . 'bye');
        $sock->blocking(1);
        print $sock $close_frame;
        $sock->flush;

        # Wait for the app to observe its first (and, if correct, only)
        # event -- this is the peer's own close, processed synchronously
        # from the frame above, well before the TCP FIN below.
        $deadline = time + 3;
        while (!@seen && time < $deadline) {
            $loop->loop_once(0.1);
        }

        is(scalar(@seen), 1, 'app received exactly one event so far');
        if (@seen) {
            is($seen[0]{type}, 'websocket.disconnect', 'event is websocket.disconnect');
            is($seen[0]{code}, 1000, 'peer code 1000 passed through unchanged');
            is($seen[0]{reason}, 'bye', "peer reason 'bye' passed through unchanged");
        }

        # Direct check on the root cause: processing the peer's Close frame
        # must have already marked the connection's disconnect-handled guard,
        # so the later TCP-close path below delivers nothing further.
        ok($conn->{_disconnect_handled}, 'disconnect-handled guard set by the Close frame path');

        # Step 2: TCP closes ~50ms later (a separate event on the wire, not
        # coalesced with the Close frame above).
        $loop->loop_once(0.05);
        close $sock;

        # Wait for the connection to fully tear down (removed from the
        # server's connection table by _close).
        $deadline = time + 3;
        while (exists $server->{connections}{refaddr($conn)} && time < $deadline) {
            $loop->loop_once(0.1);
        }
        ok(!exists $server->{connections}{refaddr($conn)}, 'connection fully torn down');

        # The discriminator: a ghost second websocket.disconnect queued by
        # the TCP-close path would never be drained (the app already
        # stopped calling receive() after its first event above), so it
        # would sit in receive_queue forever. It must not be there.
        is(scalar(@{$conn->{receive_queue}}), 0, 'no leftover ghost event in receive_queue');

        # And the app itself must still have observed only the one event.
        is(scalar(@seen), 1, 'app never received a second websocket.disconnect');
    }

    $server->shutdown->get;
};

done_testing;
