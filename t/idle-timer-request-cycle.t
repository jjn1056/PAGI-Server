#!/usr/bin/env perl

# The idle timeout covers a connection waiting for a request, not one
# handling it: a request head must arrive within `timeout` (bytes of an
# unfinished head do not extend it), a running handler is never cut off, and
# the timer starts over once a response is complete.

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use Time::HiRes qw(time);
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;
my @handled;

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
    push @handled, $scope->{path};
    await $loop->delay_future(after => 0.8) if $scope->{path} eq '/late-read';
    my $body = '';
    while (1) {
        my $event = await $receive->();
        last unless $event->{type} eq 'http.request';
        $body .= $event->{body} // '';
        last unless $event->{more};
    }
    await $loop->delay_future(after => 0.8) if $scope->{path} eq '/slow';
    if ($scope->{path} eq '/after-body') {
        # The body is in; waiting on receive now waits for a disconnect.
        await Future->wait_any($receive->(), $loop->delay_future(after => 0.8));
    }
    my $out = $scope->{path} . ':' . length($body);
    await $send->({ type => 'http.response.start', status => 200,
        headers => [['content-length', length $out]] });
    await $send->({ type => 'http.response.body', body => $out, more => 0 });
};

my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1, timeout => 0.4,
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;

sub client {
    my $s = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port,
        Proto => 'tcp', Timeout => 5) or die "connect failed: $!";
    $s->blocking(0);
    return $s;
}

# Runs the loop for up to $seconds, collecting what arrives on $s, until
# $done->($received) is true or the peer closes. Returns (bytes, closed).
sub pump {
    my ($s, $seconds, $done) = @_;
    my ($got, $closed) = ('', 0);
    my $deadline = time + $seconds;
    while (time < $deadline) {
        $loop->loop_once(0.02);
        my $n = sysread($s, my $buf, 65536);
        if (defined $n && $n == 0) { $closed = 1; last }
        $got .= $buf if $n;
        last if $done && $done->($got);
    }
    return ($got, $closed);
}

subtest 'a handler slower than the idle timeout still answers' => sub {
    my $s = client();
    print $s "GET /slow HTTP/1.1\r\nHost: x\r\n\r\n";
    my ($got) = pump($s, 3, sub { $_[0] =~ m{/slow:0\z} });
    like($got, qr{\AHTTP/1\.1 200 }, 'the response arrives');
    like($got, qr{/slow:0\z}, 'with its body');
    close $s;
};

subtest 'after a slow handler the keep-alive timer starts over' => sub {
    my $s = client();
    print $s "GET /slow HTTP/1.1\r\nHost: x\r\n\r\n";
    pump($s, 3, sub { $_[0] =~ m{/slow:0\z} });
    pump($s, 0.2);    # idle, but for less than the timeout
    print $s "GET /fast HTTP/1.1\r\nHost: x\r\n\r\n";
    my ($got) = pump($s, 2, sub { $_[0] =~ m{/fast:0\z} });
    like($got, qr{/fast:0\z}, 'a second request within the timeout is served');
    my $t0 = time;
    my (undef, $closed) = pump($s, 3);
    ok($closed, 'then the idle connection is closed');
    cmp_ok(time - $t0, '<', 1.5, 'about one timeout later');
    close $s;
};

subtest 'a request head trickled byte by byte is closed at the timeout' => sub {
    @handled = ();
    my $s = client();
    my @bytes = split //, "GET /trickle HTTP/1.1\r\nHost: x\r\nX-Slow: " . ('a' x 40);
    my $t0 = time;
    my $closed = 0;
    while (@bytes && time - $t0 < 3) {
        syswrite($s, shift @bytes) or do { $closed = 1; last };
        my (undef, $c) = pump($s, 0.1);
        if ($c) { $closed = 1; last }
    }
    my $elapsed = time - $t0;
    ok($closed, 'the server closed the connection');
    cmp_ok($elapsed, '<', 1.5, 'within about the timeout, not while bytes kept coming');
    is(\@handled, [], 'the application never saw the request');
    close $s;
};

subtest 'a request body trickled in is still read' => sub {
    my $body = 'b' x 12;
    my $s = client();
    print $s "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: " . length($body) . "\r\n\r\n";
    my $got = '';
    for my $byte (split //, $body) {
        syswrite($s, $byte);
        $got .= (pump($s, 0.1))[0];
    }
    $got .= (pump($s, 2, sub { $got . $_[0] =~ m{/upload:12\z} }))[0];
    like($got, qr{/upload:12\z}, 'the whole body arrives, past the idle timeout');
    close $s;
};

subtest 'a client silent partway through its body is closed at the timeout' => sub {
    my $s = client();
    print $s "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n" . ('b' x 10);
    my $t0 = time;
    my (undef, $closed) = pump($s, 3);
    ok($closed, 'the server closed the connection');
    cmp_ok(time - $t0, '<', 1.5, 'about one timeout after the client went quiet');
    close $s;
};

subtest 'a handler that reads its body late still answers' => sub {
    my $s = client();
    print $s "POST /late-read HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\nbody";
    my ($got) = pump($s, 3, sub { $_[0] =~ m{/late-read:4\z} });
    like($got, qr{/late-read:4\z}, 'the response arrives');
    close $s;
};

subtest 'an application waiting after the body is complete is not cut off' => sub {
    my $s = client();
    print $s "POST /after-body HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\nbody";
    my ($got) = pump($s, 3, sub { $_[0] =~ m{/after-body:4\z} });
    like($got, qr{/after-body:4\z}, 'the response arrives');
    close $s;
};

done_testing;
