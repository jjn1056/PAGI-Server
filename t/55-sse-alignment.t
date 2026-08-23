use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

# Configure Future::IO for tests that use Future::IO->sleep()
eval { require Future::IO::Impl::IOAsync; 1 }
    or plan skip_all => 'Future::IO::Impl::IOAsync required for SSE tests';

use PAGI::Server;
use IO::Socket::INET;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Media-range SSE detection (design doc section 11.5; PAGI Www.pod "SSE
# Connection Detection"): sse iff the request's Accept header(s) contain the
# exact media range text/event-stream, case-insensitively, with q > 0. This
# is a boolean client-signal test, not content negotiation -- wildcards
# never signal SSE, q=0 is an explicit refusal, and near-miss substrings
# (e.g. text/event-streamer) must not false-positive.

my $loop = IO::Async::Loop->new;

# Observed by the app on every request.
our $DETECTED_TYPE;

sub create_server {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;

        $DETECTED_TYPE = $scope->{type};

        if ($scope->{type} eq 'sse') {
            # Decline cleanly so the client isn't left hanging.
            await $send->({ type => 'sse.http.response.start', status => 204, headers => [] });
            await $send->({ type => 'sse.http.response.body', body => '', more => 0 });
        }
        else {
            await $receive->();
            await $send->({
                type    => 'http.response.start',
                status  => 200,
                headers => [['content-type', 'text/plain']],
            });
            await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
        }
    };

    my $server = PAGI::Server->new(
        app              => $app,
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        shutdown_timeout => 1,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

# Sends a GET request with one Accept header per element of @accepts (so
# repeated Accept headers can be exercised as well as a single combined one).
sub send_request {
    my ($port, @accepts) = @_;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    ) or return;

    my $req = "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\n";
    $req .= "Accept: $_\r\n" for @accepts;
    $req .= "\r\n";
    print $sock $req;

    $sock->blocking(0);
    my $deadline = time + 5;
    while (time < $deadline) {
        $loop->loop_once(0.05);
        last if defined $DETECTED_TYPE;
    }
    my $wire = '';
    while (1) {
        my $n = sysread($sock, my $buf, 4096);
        last unless $n;
        $wire .= $buf;
    }
    close $sock;
    $loop->loop_once(0.05) for 1 .. 10;
    return $wire;
}

my @matrix = (
    ['text/event-stream',                             'sse',  'exact match'],
    ['TEXT/EVENT-STREAM',                             'sse',  'case-insensitive match'],
    ['text/event-stream;q=0.4',                        'sse',  'q>0 signals sse'],
    ['text/event-stream, text/html;q=0.9',             'sse',  'presence, not preference'],
    ['text/event-stream;q=0',                          'http', 'q=0 is an explicit refusal'],
    ['*/*',                                            'http', 'wildcard never signals sse'],
    ['text/*',                                         'http', 'partial wildcard never signals sse'],
    ['application/json, text/event-streamer',           'http', 'substring near-miss must not false-positive'],
);

for my $row (@matrix) {
    my ($accept, $expected, $label) = @$row;
    subtest "Accept: $accept -> $expected ($label)" => sub {
        local $DETECTED_TYPE;
        my $server = create_server();
        my $port   = $server->port;
        send_request($port, $accept);
        is($DETECTED_TYPE, $expected, "scope type is $expected");
        $server->shutdown->get;
    };
}

subtest 'two Accept headers, only the second carries the range -> sse' => sub {
    local $DETECTED_TYPE;
    my $server = create_server();
    my $port   = $server->port;
    send_request($port, 'application/json', 'text/event-stream');
    is($DETECTED_TYPE, 'sse', 'scope type is sse');
    $server->shutdown->get;
};

done_testing;
