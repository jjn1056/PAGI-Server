use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use Scalar::Util qw(weaken);
use Time::HiRes qw(time);
use PAGI::Server;

plan skip_all => 'Server integration tests not supported on Windows' if $^O eq 'MSWin32';

# A strong connection capture in a stream callback creates a cycle even if
# that callback branch never executes. Exercise actual EOF and prove the
# connection state can be collected after the handler returns and delivery finishes.
my $loop = IO::Async::Loop->new;
sub pump {
    my ($condition) = @_;
    my $until = time + 5;
    while (!$condition->() && time < $until) { $loop->loop_once(.01) }
    return $condition->();
}
for my $observed (0, 1) {
    my $name = $observed ? 'observed interrupted upload' : 'unobserved interrupted upload';
    subtest $name => sub {
        my ($started, $ended, $weak_state, $token, $inside, $callbacks, $reentrant);
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            return unless $scope->{type} eq 'http';
            my $state = $scope->{'pagi.connection'};
            $weak_state = $state;
            weaken($weak_state);
            $state->on_disconnect(sub { ++$callbacks; $reentrant = 1 if $inside }) if $observed;
            $started = 1;
            while (1) {
                $inside = 1;
                my $future = $receive->();
                $inside = 0;
                my $event = await $future;
                last if $event->{type} eq 'http.disconnect';
            }
            $token = $state->disconnect_reason;
            $ended = 1;
        };
        my $server = PAGI::Server->new(
            app => $app,
            host => '127.0.0.1',
            port => 0,
            quiet => 1,
            access_log => undef,
            shutdown_timeout => 1,
        );
        $loop->add($server);
        $server->listen->get;
        my $sock = IO::Socket::INET->new(
            PeerAddr => '127.0.0.1',
            PeerPort => $server->port,
            Proto => 'tcp',
            Timeout => 2,
        ) or die "Cannot connect: $!";
        my $request = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 16\r\n\r\npart";
        is(syswrite($sock, $request), length($request), 'partial upload sent');
        ok(pump(sub { $started }), 'handler started before peer closed');
        close $sock;
        ok(pump(sub { $ended && (!$observed || $callbacks) }),
            'handler and expected notification finished');
        is($token, 'client_closed', 'abnormal outcome from real EOF');
        ok(!$reentrant, 'callback did not run inside receive call');
        my $released = pump(sub { !defined $weak_state });
        is(scalar(keys %{$server->{connections}}), 0, 'closed connection removed from registry');
        ok($released, 'connection state released after delivery');
        is($callbacks // 0, $observed ? 1 : 0, 'expected notification count');
        $server->shutdown->get;
        $loop->remove($server);
    };
}
done_testing;
