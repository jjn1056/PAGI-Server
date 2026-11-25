#!/usr/bin/env perl

# PAGI::Spec::Www, "Application Left a Response Incomplete": an application
# that stopped because its client left MUST NOT send the terminal event,
# since every intermediary in the $send chain still sees it. With
# run_until_end the application sends it only when the work finished.
#
# A wire-only test cannot see a terminal event sent after the client left:
# the server discards it. A middleware in front of the application records
# every event the application sends, as any intermediary would.

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future;
use Future::AsyncAwait;
use Time::HiRes qw(time);

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;
my (@log, %seen_by_middleware, %done);

# The shape of examples/02-streaming-response in PAGI: chunks, then the end
# of the body and a trailer, both only when the work finished.
my $streaming_app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'http';
    my $conn = $scope->{'pagi.connection'};
    await $send->({ type => 'http.response.start', status => 200,
        headers => [['content-type', 'text/plain']], trailers => 1 });
    await $conn->run_until_end(async sub {
        for my $n (1 .. 3) {
            await $loop->delay_future(after => 0.3) if $n > 1;
            await $send->({ type => 'http.response.body', body => "Chunk $n\n", more => 1 });
        }
    });
    return unless $conn->is_connected;
    await $send->({ type => 'http.response.body', body => '', more => 0 });
    await $send->({ type => 'http.response.trailers', headers => [['x-stream-complete', '1']] });
};

# Records what the application sends, then passes it on.
my $recording = async sub {
    my ($scope, $receive, $send) = @_;
    return await $streaming_app->($scope, $receive, $send) unless $scope->{type} eq 'http';
    my $path = $scope->{path};
    my $seen = $seen_by_middleware{$path} = [];
    my $recording_send = sub {
        my ($event) = @_;
        push @$seen, $event->{type} eq 'http.response.body'
            ? "body more=" . ($event->{more} // 0) : $event->{type};
        return $send->($event);
    };
    await $streaming_app->($scope, $receive, $recording_send);
    $done{$path} = 1;
};

my $server = PAGI::Server->new(
    app => $recording, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
    shutdown_timeout => 1, logger => sub { push @log, $_[0] },
);
$loop->add($server);
$server->listen->get;

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 10);
    $loop->loop_once(0.01) until $cond->() || time > $deadline;
    return $cond->();
}

sub request {
    my ($path) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2)
        or die "connect: $!";
    $sock->blocking(0);
    syswrite $sock, "GET $path HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    return $sock;
}

subtest 'the work finished: the middleware sees the end of the body and the trailer' => sub {
    my $sock = request('/full');
    ok(pump_until(sub { $done{'/full'} }), 'the application returned');
    is($seen_by_middleware{'/full'},
        ['http.response.start', 'body more=1', 'body more=1', 'body more=1', 'body more=0', 'http.response.trailers'],
        'every chunk, then the terminal body and the trailer');
    close $sock;
};

subtest 'the client left halfway: the middleware sees no terminal event' => sub {
    my $sock = request('/cut');
    my $got = '';
    ok(pump_until(sub {
        my $n = sysread($sock, my $buf, 65536);
        $got .= $buf if $n;
        return $got =~ /Chunk 2/;
    }), 'two chunks arrived');
    close $sock;
    ok(pump_until(sub { $done{'/cut'} }), 'the application returned');
    is($seen_by_middleware{'/cut'}, ['http.response.start', 'body more=1', 'body more=1'],
        'the partial body only: no terminal body, no trailer');
};

$server->shutdown->get;
$loop->remove($server);
is([ grep { !/\A(?:listening|Using default max_connections)/ } map { $_->{message} // '' } @log ], [],
    'nothing logged');
done_testing;
