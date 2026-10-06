use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => 'Server integration tests not supported on Windows' if $^O eq 'MSWin32';

# A 204 or 304 response ends at its header section (RFC 9112 6.3; RFC 9110
# 15.3.5, 15.4.5), and a server must not send Transfer-Encoding in a 204
# (RFC 9112 6.1). Whatever the application sends after http.response.start,
# the server writes no framing and no content for these statuses. On a
# keep-alive connection, a stray chunked terminator would be read as the start
# of the next response.

my $loop = IO::Async::Loop->new;

# /<status>         start + an empty body
# /<status>/body    start + a non-empty body, which must not reach the wire
# /200              the follow-up response that proves the connection is in step
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
                return;
            }
        }
    }
    return unless $scope->{type} eq 'http';
    my ($status, $kind) = $scope->{path} =~ m{^/(\d+)(?:/(\w+))?};
    await $send->({ type => 'http.response.start', status => $status, headers => [] });
    my $body = $status == 200 ? 'next' : ($kind // '') eq 'body' ? 'must not be sent' : '';
    await $send->({ type => 'http.response.body', body => $body });
};

my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
);
$loop->add($server);
$server->listen->get;

# Send the requests on one keep-alive connection (the last one closes it) and
# return everything the server wrote.
sub exchange {
    my (@paths) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port)
        or die "connect: $!";
    my $requests = '';
    for my $i (0 .. $#paths) {
        my $connection = $i == $#paths ? 'close' : 'keep-alive';
        $requests .= "GET $paths[$i] HTTP/1.1\r\nHost: localhost\r\nConnection: $connection\r\n\r\n";
    }
    syswrite($sock, $requests);
    $sock->blocking(0);
    my $data = '';
    for (1 .. 100) {
        $loop->loop_once(0.05);
        my $n = sysread($sock, my $buf, 65536);
        if (defined $n) {
            last if $n == 0;
            $data .= $buf;
        }
    }
    close($sock);
    return $data;
}

for my $status (204, 304) {
    subtest "$status ends at its headers" => sub {
        my $data = exchange("/$status", '/200');
        my ($head, $rest) = split /\r\n\r\n/, $data, 2;
        like($head, qr{\AHTTP/1\.1 $status }, "the first response is the $status");
        unlike($head, qr/^transfer-encoding:/mi, 'no Transfer-Encoding');
        like($rest // '', qr{\AHTTP/1\.1 200 }, 'the next response starts right after its headers');
        like($rest // '', qr/\r\n4\r\nnext\r\n0\r\n\r\n\z/, 'and arrives whole (chunked)');
    };

    subtest "$status drops a body the application sends" => sub {
        my $data = exchange("/$status/body", '/200');
        unlike($data, qr/must not be sent/, 'the body is not written');
        my (undef, $rest) = split /\r\n\r\n/, $data, 2;
        like($rest // '', qr{\AHTTP/1\.1 200 }, 'the next response starts right after its headers');
    };
}

subtest '200 without content-length is still chunked' => sub {
    my $data = exchange('/200');
    like($data, qr/^transfer-encoding: chunked/mi, 'chunked framing');
    like($data, qr/\r\n4\r\nnext\r\n0\r\n\r\n\z/, 'the body and terminator');
};

$server->shutdown->get;
done_testing;
