#!/usr/bin/env perl

# =============================================================================
# Test: An unrecognized event type fails the send Future (PAGI spec compliance)
#
# Per main.mkdn: "Servers must raise exceptions if... The type field is
# unrecognized"
#
# This is a single end-to-end smoke test: one HTTP/1.1 server, an app that
# sends a misspelled event type, and an assertion that the failed $send
# Future's message names the bad type and protocol. It deliberately does not
# re-cover ground owned elsewhere:
#   - t/40-event-validation.t: unit coverage of
#     PAGI::Server::EventValidator::validate_*_send for every protocol
#     (http, sse, websocket, lifespan), including this exact message wording.
#   - t/52-mandatory-validation.t / t/http2/26-mandatory-validation.t:
#     cross-family behavioral coverage of h1 and h2 send-contract violations
#     (including unrecognized types) end-to-end.
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Net::Async::HTTP;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# The app misspells http.response.start, catches the failed send Future, and
# reports the error in a normal response body so the test observes the
# failure through real server behavior, never source inspection.
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }

    my $err = do {
        local $@;
        eval { await $send->({ type => 'http.resposne.start', status => 200 }) };
        $@;
    };
    $err =~ s/\n/ /g;

    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => $err, more => 0 });
};

my $loop = IO::Async::Loop->new;
my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;

my $http = Net::Async::HTTP->new;
$loop->add($http);

my $response = $http->GET("http://127.0.0.1:$port/")->get;
like(
    $response->content,
    qr/Unrecognized event type .* for http protocol/,
    'a misspelled event type fails the send Future end-to-end',
);

$server->shutdown->get;

done_testing;
