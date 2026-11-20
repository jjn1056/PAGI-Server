#!/usr/bin/env perl

# A file body over HTTP/1.1 whose client leaves while the server is still
# reading the file. PAGI::Spec's Send Completion Contract: a send pending when
# the connection ends settles promptly, successfully. The server stops reading
# the file, and says nothing on STDERR about the bytes it no longer writes.

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Socket qw(SO_RCVBUF);
use Future::AsyncAwait;
use Test::TempDir::Tiny qw(tempdir);
use Time::HiRes qw(time);

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 10);
    $loop->loop_once(0.01) until $cond->() || time > $deadline;
    return $cond->();
}

subtest 'a file body whose client leaves settles at once, quietly' => sub {
    # Sparse: 256MB that cost no disk, read in 4096 trips to a file worker.
    my $file = tempdir() . '/big.bin';
    open my $fh, '>', $file or die "create $file: $!";
    truncate $fh, 256 * 1024 * 1024 or die "truncate $file: $!";
    close $fh;

    my %obs;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'http';
        my $conn = $obs{conn} = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { $obs{left_at} = time });
        await $send->({ type => 'http.response.start', status => 200,
            headers => [['content-type', 'application/octet-stream']] });
        $obs{ok} = eval { await $send->({ type => 'http.response.body', file => $file }); 1 };
        $obs{error}      = $@;
        $obs{settled_at} = time;
    };

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
        shutdown_timeout => 1, logger => sub { },
    );
    $loop->add($server);
    $server->listen->get;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2,
    ) or die "connect: $!";
    $sock->sockopt(SO_RCVBUF, 4096);   # the response backs up in the server
    $sock->blocking(0);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    # The client reads until the file's bytes are arriving, so the server is
    # part way through it, then leaves with the rest of the response backed
    # up. Whether a chunk read after the leave then meets a stream still
    # closing (and IO::Async carps) or one already closed is a race on the
    # loop, so the last check below guards the carp but cannot reliably
    # provoke it.
    my $got = '';
    ok(pump_until(sub {
        my $n = sysread($sock, my $buf, 4096);
        $got .= $buf if $n;
        return $got =~ /\r\n\r\n./s;
    }, 30), 'the file is being sent');
    close $sock;

    ok(pump_until(sub { $obs{settled_at} }, 30), 'the file send settled');
    ok($obs{ok}, 'it resolved successfully') or diag "error: $obs{error}";
    ok($obs{left_at}, 'the server saw the client leave');
    cmp_ok($obs{settled_at} - ($obs{left_at} // 0), '<', 0.25,
        'it settled promptly once the client had gone');
    is([grep { /Cannot write data/ } @warnings], [], 'nothing about the unwritten bytes on STDERR');

    $server->shutdown->get;
    $loop->remove($server);
};

done_testing;
