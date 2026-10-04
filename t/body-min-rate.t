#!/usr/bin/env perl

# body_min_rate: while the application waits for a request body, the client
# must average body_min_rate bytes/s once body_rate_grace has passed.

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
    app => $app, host => '127.0.0.1', port => 0, quiet => 1, timeout => 5, body_min_rate => 100, body_rate_grace => 0.5,
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;

sub client_to {
    my ($to) = @_;
    my $s = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $to,
        Proto => 'tcp', Timeout => 5) or die "connect failed: $!";
    $s->blocking(0);
    return $s;
}

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

# Sends $body one $chunk-byte piece every $gap seconds; returns what came
# back and whether the server closed the connection.
sub trickle_body {
    my ($body, $chunk, $gap, $to_port) = @_;
    my $s = $to_port ? client_to($to_port) : client();
    print $s "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: " . length($body) . "\r\n\r\n";
    my ($got, $closed) = ('', 0);
    for my $piece (unpack("(a$chunk)*", $body)) {
        syswrite($s, $piece) or do { $closed = 1; last };
        my ($g, $c) = pump($s, $gap);
        $got .= $g;
        if ($c) { $closed = 1; last }
    }
    unless ($closed) {
        my ($g, $c) = pump($s, 2, sub { $got . $_[0] =~ m{/upload:\d+\z} });
        ($got, $closed) = ($got . $g, $c);
    }
    close $s;
    return ($got, $closed);
}

subtest 'a body below the minimum rate is ended' => sub {
    my $t0 = time;
    my ($got, $closed) = trickle_body('b' x 100, 1, 0.1);    # ~10 bytes/s
    ok($closed, 'the server closed the connection');
    like($got, qr{\AHTTP/1\.1 408 Request Timeout\r\n}, 'with 408 Request Timeout');
    is(scalar(() = $got =~ m{^HTTP/1\.1 }mg), 1, 'and nothing the application sends afterwards');
    cmp_ok(time - $t0, '<', 2.5, 'soon after the grace period');
};

subtest 'a body at or above the minimum rate completes' => sub {
    my ($got) = trickle_body('b' x 300, 30, 0.1);             # ~300 bytes/s
    like($got, qr{/upload:300\z}, 'the whole body arrived');
};

subtest 'a body sent at once is untouched' => sub {
    my ($got) = trickle_body('b' x 100_000, 100_000, 0.05);
    like($got, qr{/upload:100000\z}, 'the whole body arrived');
};

subtest 'body_min_rate => 0 turns it off' => sub {
    my $off = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1,
        timeout => 5, body_min_rate => 0, body_rate_grace => 0.5);
    $loop->add($off);
    $off->listen->get;
    my ($got) = trickle_body('b' x 20, 1, 0.1, $off->port);   # ~10 bytes/s
    like($got, qr{/upload:20\z}, 'a slow body completes');
    $loop->remove($off);
};

subtest 'invalid values die at construction' => sub {
    for my $bad (-1, 'fast', '') {
        like(dies { PAGI::Server->new(app => $app, body_min_rate => $bad) },
            qr/\QInvalid body_min_rate '$bad' - must be a non-negative number of bytes per second (0 turns it off)\E/,
            "body_min_rate '$bad'");
        like(dies { PAGI::Server->new(app => $app, body_rate_grace => $bad) },
            qr/\QInvalid body_rate_grace '$bad' - must be a non-negative number of seconds\E/,
            "body_rate_grace '$bad'");
    }
    ok(lives { PAGI::Server->new(app => $app, body_min_rate => 1024, body_rate_grace => 2.5) },
        'numbers are accepted');
};

done_testing;
