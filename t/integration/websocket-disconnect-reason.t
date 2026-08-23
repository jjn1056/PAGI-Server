use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Net::Async::WebSocket::Client;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";

use PAGI::Server;
use PAGI::Server::Connection;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# B5 (SYNC A2): a server-detected abnormal WebSocket close must deliver a
# populated reason (and the matching close code) in the websocket.disconnect
# event the app receives -- not the old hardcoded { code => 1006, reason => '' }.
#
# Queue overflow is the deterministic trigger: it runs the same
# _handle_disconnect -> disconnect-event path that every other server-detected
# close uses, so it validates the centralized reason/code plumbing.

my $loop = IO::Async::Loop->new;

subtest 'queue overflow delivers reason=queue_overflow, code=1008' => sub {
    my $disconnect_reason;
    my $disconnect_code;
    my $app_started = 0;

    my $app = async sub {
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

        await $send->({ type => 'websocket.accept' });
        $app_started = 1;

        # Let the client flood the receive queue past the limit first.
        await $loop->delay_future(after => 0.3);

        # Drain until the server reports the disconnect.
        while (1) {
            my $event = await $receive->();
            if ($event->{type} eq 'websocket.disconnect') {
                $disconnect_reason = $event->{reason};
                $disconnect_code   = $event->{code};
                last;
            }
        }
    };

    my $server = PAGI::Server->new(
        app               => $app,
        host              => '127.0.0.1',
        port              => 0,
        quiet             => 1,
        max_receive_queue => 5,
    );
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $client = Net::Async::WebSocket::Client->new;
    $loop->add($client);

    eval {
        $client->connect(url => "ws://127.0.0.1:$port/")->get;

        my $deadline = time + 2;
        while (!$app_started && time < $deadline) {
            $loop->loop_once(0.01);
        }

        # Flood past max_receive_queue while the app is still delaying.
        for my $i (1 .. 20) {
            eval { $client->send_text_frame("msg$i") };
            last if $@;
            $loop->loop_once(0.01);
        }

        # Let the app drain and observe the disconnect.
        $deadline = time + 3;
        while (!defined($disconnect_reason) && time < $deadline) {
            $loop->loop_once(0.05);
        }
    };

    is($disconnect_reason, 'queue_overflow',
        'app receives standard reason token for queue overflow');
    is($disconnect_code, 1008,
        'app receives the 1008 close code, not the 1006 default');

    eval { $loop->remove($client) };
    $server->shutdown->get;
    eval { $loop->remove($server) };
};

# ============================================================
# receive() fallback prefers the recorded ws_disconnect_reason over ''
# ============================================================
# White-box: exercises _create_websocket_receive's OWN fallback branch
# directly -- the code path a receive() call takes when it resolves after
# {closed} is already true (a server-initiated close, e.g. idle timeout,
# racing an already-pending receive()), rather than through the primary
# _handle_disconnect push+resolve path the queue_overflow subtest above
# already covers. The fallback must report whatever ws_disconnect_reason a
# server-initiated close recorded (_handle_disconnect sets it before
# closing), via _ws_disconnect_event, defaulting to '' only when nothing
# was recorded.
subtest 'receive() fallback after close reports the recorded ws_disconnect_reason, not empty' => sub {
    my $conn = PAGI::Server::Connection->new(app => sub { });
    $conn->{closed} = 1;
    $conn->{ws_disconnect_reason} = 'idle_timeout';

    my $receive = $conn->_create_websocket_receive;
    my $event = $receive->()->get;

    is($event->{type}, 'websocket.disconnect', 'fallback event type is websocket.disconnect');
    is($event->{code}, 1006, 'fallback code is still 1006 (abnormal closure)');
    is($event->{reason}, 'idle_timeout', "fallback reason is the recorded token, not ''");
};

subtest 'receive() fallback with no recorded reason still falls back to empty' => sub {
    my $conn = PAGI::Server::Connection->new(app => sub { });
    $conn->{closed} = 1;

    my $receive = $conn->_create_websocket_receive;
    my $event = $receive->()->get;

    is($event->{code}, 1006, 'fallback code is still 1006');
    is($event->{reason}, '', "fallback reason is '' when nothing was recorded");
};

done_testing;
