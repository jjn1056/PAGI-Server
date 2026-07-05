#!/usr/bin/env perl

# Minimal PAGI application used by bench/run.sh to measure raw server
# overhead. Endpoints:
#   GET  /hello  -> 13-byte fixed body       (per-request overhead)
#   POST /echo   -> echoes the request body  (request-body read path)
#   GET  /big    -> 64KB fixed body          (response write path)
#   anything else -> 404

use strict;
use warnings;
use Future::AsyncAwait;

my $hello = 'Hello, World!';
my $big   = 'x' x 65536;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Unsupported scope type: $scope->{type}\n"
        if $scope->{type} ne 'http';

    my $path = $scope->{path};

    if ($path eq '/hello') {
        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [ [ 'content-type', 'text/plain' ],
                         [ 'content-length', length $hello ] ],
        });
        await $send->({ type => 'http.response.body', body => $hello, more => 0 });
        return;
    }

    if ($path eq '/echo') {
        my $body = '';
        while (1) {
            my $event = await $receive->();
            last if $event->{type} eq 'http.disconnect';
            if ($event->{type} eq 'http.request') {
                $body .= $event->{body} // '';
                last unless $event->{more};
            }
        }
        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [ [ 'content-type', 'application/octet-stream' ],
                         [ 'content-length', length $body ] ],
        });
        await $send->({ type => 'http.response.body', body => $body, more => 0 });
        return;
    }

    if ($path eq '/big') {
        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [ [ 'content-type', 'application/octet-stream' ],
                         [ 'content-length', length $big ] ],
        });
        await $send->({ type => 'http.response.body', body => $big, more => 0 });
        return;
    }

    await $send->({
        type    => 'http.response.start',
        status  => 404,
        headers => [ [ 'content-type', 'text/plain' ],
                     [ 'content-length', 9 ] ],
    });
    await $send->({ type => 'http.response.body', body => 'Not Found', more => 0 });
};

$app;
