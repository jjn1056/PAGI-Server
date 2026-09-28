#!/usr/bin/env perl

# =============================================================================
# Test: terminal callbacks are delivered on the event loop, never synchronously
# inside the application's terminal $send (S5).
#
# L<PAGI::Spec::Www>, "Callback invocation context" / Server Requirement 3a:
# on_disconnect, on_complete and on_end are delivered on the event loop, never
# synchronously within an application's call into $send or $receive. The
# synchronous FACTS stay immediate at the transition.
#
# On HTTP/1.1 the clean terminal transition (_mark_complete) runs in the
# terminal send's successful tail, before the application's
# await continuation resumes. So right after `await $send->(terminal body)`:
#
#   * response_complete() is already true                -- the fact is immediate
#   * the on_complete / on_end callbacks have NOT run yet -- delivery is deferred
#
# and the callbacks run on a later loop turn. This test observes both moments
# from inside the application (the test process is the server, one event loop)
# and pins the ordering with a bounded pump, no sleeps.
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

sub start_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app              => $app,
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        access_log       => undef,
        shutdown_timeout => 1,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub connect_client {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 2,
    ) or die "Cannot connect: $!";
    $sock->blocking(0);
    return $sock;
}

# Drive the loop until $cond is true or $timeout expires; returns its final truth.
sub pump_until {
    my ($cond, $timeout) = @_;
    $timeout //= 10;
    my $deadline = time + $timeout;
    while (time < $deadline) {
        return 1 if $cond->();
        $loop->loop_once(0.05);
    }
    return $cond->() ? 1 : 0;
}

sub shutdown_server {
    my ($server) = @_;
    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
}

subtest 'on_complete/on_end fire on a later loop turn, not inside the terminal send' => sub {
    my %obs;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'http';
        my $conn = $scope->{'pagi.connection'};

        my $fired_complete = 0;
        my $fired_end      = 0;
        $conn->on_complete(sub { $fired_complete++; $obs{complete_ran} = 1 });
        $conn->on_end(sub { $fired_end++; $obs{end_ran} = 1 });

        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [['content-type', 'text/plain']],
        });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });

        # Observed synchronously, right after the terminal send resolves: the
        # fact is set, the callbacks are not yet delivered.
        $obs{rc_after_send}       = $conn->response_complete;
        $obs{complete_after_send} = $fired_complete;
        $obs{end_after_send}      = $fired_end;
    };

    my $server = start_server($app);
    my $port   = $server->port;

    my $sock = connect_client($port);
    syswrite($sock,
          "GET / HTTP/1.1\r\n"
        . "Host: localhost\r\n"
        . "Connection: close\r\n"
        . "\r\n");

    # Pump until the application has recorded its synchronous observation AND
    # the deferred callbacks have run on a later turn.
    my $ran = pump_until(
        sub { $obs{complete_ran} && $obs{end_ran} && defined $obs{rc_after_send} },
        10,
    );
    ok($ran, 'application observed the send and both callbacks ran');

    # The fact is immediate at the terminal transition.
    is($obs{rc_after_send}, 1,
        'response_complete() is true synchronously, right after the terminal send');

    # Delivery is deferred: the callbacks had not run at that synchronous instant.
    is($obs{complete_after_send}, 0,
        'on_complete had NOT run synchronously inside the terminal send');
    is($obs{end_after_send}, 0,
        'on_end had NOT run synchronously inside the terminal send');

    # They run on a later loop turn.
    is($obs{complete_ran}, 1, 'on_complete ran on a later loop turn');
    is($obs{end_ran},      1, 'on_end ran on a later loop turn');

    close($sock);
    shutdown_server($server);
};

done_testing;
