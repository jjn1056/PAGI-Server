use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;
use IO::Socket::INET;
use Time::HiRes qw(time);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# A response's bytes reach the client whole and in order however the server
# gathers them on their way to the socket: many small sends, a close after
# the response (its Connection may be gone before the loop writes), a second
# response on a kept-alive connection, one very large send.

my $loop = IO::Async::Loop->new;

# Line $n of a body: 100 bytes, numbered, so order can be checked.
sub line { sprintf("%07d", $_[0]) . ('x' x 92) . "\n" }

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope\n" unless $scope->{type} eq 'http';
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    if ($scope->{path} eq '/big') {
        await $send->({ type => 'http.response.body', body => 'x' x (1 << 20), more => 0 });
        return;
    }
    my ($n) = $scope->{path} =~ m{\A/lines/(\d+)\z} or die "unknown path\n";
    for my $i (1 .. $n) {
        await $send->({ type => 'http.response.body', body => line($i), more => 1 });
    }
    await $send->({ type => 'http.response.body', body => '', more => 0 });
    return;
};

sub start_server {
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0,
        quiet => 1, access_log => undef, shutdown_timeout => 1);
    $loop->add($server);
    $server->listen->get;
    return $server;
}

# Reads while running the loop until $done says so, EOF, or $secs pass.
sub read_until {
    my ($sock, $done, $secs) = @_;
    my ($buf, $eof, $t0) = ('', 0, time);
    while (time - $t0 < $secs) {
        my $n = sysread($sock, my $chunk, 1 << 16);
        if (defined $n && $n == 0) { $eof = 1; last }
        $buf .= $chunk if $n;
        last if $done && $done->($buf);
        $loop->loop_once(0.01);
    }
    return ($buf, $eof);
}

# The body of one chunked response at the start of $buf, and what follows it.
sub dechunk {
    my ($buf) = @_;
    my $at = index($buf, "\r\n\r\n");
    return unless $at >= 0;
    my ($body, $pos) = ('', $at + 4);
    while ((my $eol = index($buf, "\r\n", $pos)) >= 0) {
        my $len = hex substr($buf, $pos, $eol - $pos);
        return ($body, substr($buf, $eol + 4)) if $len == 0;
        return if length($buf) < $eol + 2 + $len + 2;
        $body .= substr($buf, $eol + 2, $len);
        $pos = $eol + 2 + $len + 2;
    }
    return;
}

my $response_done = sub { my ($body) = dechunk($_[0]); defined $body };

sub connect_to {
    my ($server) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp')
        or die "connect: $!";
    $sock->blocking(0);
    return $sock;
}

subtest 'a close-after-response body sent in many small sends arrives whole' => sub {
    my $server = start_server();
    my $sock = connect_to($server);
    syswrite $sock, "GET /lines/2000 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
    my ($buf, $eof) = read_until($sock, undef, 10);
    ok($eof, 'the server closed the connection after the response');
    my ($body) = dechunk($buf);
    is(length($body // ''), 200_000, 'every byte arrived');
    ok(($body // '') eq join('', map { line($_) } 1 .. 2000), 'lines 1..2000, in order');
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'an HTTP/1.0 body sent in many small sends arrives whole before the close' => sub {
    my $server = start_server();
    my $sock = connect_to($server);
    syswrite $sock, "GET /lines/2000 HTTP/1.0\r\nHost: x\r\n\r\n";
    my ($buf, $eof) = read_until($sock, undef, 10);
    ok($eof, 'the server closed the connection after the response');
    my $at = index($buf, "\r\n\r\n");
    my $body = $at >= 0 ? substr($buf, $at + 4) : '';
    is(length $body, 200_000, 'every byte arrived');
    ok($body eq join('', map { line($_) } 1 .. 2000), 'lines 1..2000, in order');
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'a keep-alive connection serves a second response after a burst' => sub {
    my $server = start_server();
    my $sock = connect_to($server);
    my $expect = join '', map { line($_) } 1 .. 500;
    for my $round (1, 2) {
        syswrite $sock, "GET /lines/500 HTTP/1.1\r\nHost: x\r\n\r\n";
        my ($buf, $eof) = read_until($sock, $response_done, 10);
        my ($body) = dechunk($buf);
        ok(!$eof, "response $round: the connection stays open");
        ok(($body // '') eq $expect, "response $round: 500 lines, whole and in order");
    }
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'a 1 MiB body in one send arrives intact over HTTP/1.1' => sub {
    my $server = start_server();
    my $sock = connect_to($server);
    syswrite $sock, "GET /big HTTP/1.1\r\nHost: x\r\n\r\n";
    my ($buf) = read_until($sock, $response_done, 10);
    my ($body) = dechunk($buf);
    is(length($body // ''), 1 << 20, 'every byte arrived');
    ok(($body // '') eq 'x' x (1 << 20), 'unchanged');
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

done_testing;
