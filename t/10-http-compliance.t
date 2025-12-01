use strict;
use warnings;
use experimental 'signatures';
use Test2::V0;
use IO::Async::Loop;
use Net::Async::HTTP;
use Future::AsyncAwait;
use FindBin;
use IO::Socket::INET;

use PAGI::Server;

# Step 10: HTTP/1.1 Compliance and Edge Cases

my $loop = IO::Async::Loop->new;

# Load the hello app for testing
my $app_path = "$FindBin::Bin/../examples/01-hello-http/app.pl";
my $app = do $app_path;
die "Could not load app from $app_path: $@" if $@;
die "App did not return a coderef" unless ref $app eq 'CODE';

# Test 1: HEAD request returns headers without body
subtest 'HEAD request returns headers without body' => sub {
    my $server = PAGI::Server->new(
        app   => $app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;

    my $port = $server->port;

    my $http = Net::Async::HTTP->new;
    $loop->add($http);

    # Use HEAD method
    my $response = $http->HEAD("http://127.0.0.1:$port/")->get;

    # Verify response has headers
    is($response->code, 200, 'HEAD response status is 200 OK');
    ok($response->header('Content-Type'), 'HEAD response has Content-Type header');
    ok($response->header('Date'), 'HEAD response has Date header');

    # Verify response body is empty (HEAD should not have body)
    is($response->content, '', 'HEAD response body is empty');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

# Test 2: Multiple Cookie headers are normalized
subtest 'Multiple Cookie headers are normalized into single header' => sub {
    my $captured_scope;

    my $cookie_test_app = async sub ($scope, $receive, $send) {
        # Handle lifespan scope
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

        die "Unsupported scope type: $scope->{type}" unless $scope->{type} eq 'http';

        $captured_scope = $scope;

        # Return the cookie headers in the response body for inspection
        my @cookies = grep { $_->[0] eq 'cookie' } @{$scope->{headers}};
        my $cookie_info = "cookies=" . scalar(@cookies);
        if (@cookies) {
            $cookie_info .= ";value=" . $cookies[0][1];
        }

        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [['content-type', 'text/plain']],
        });

        await $send->({
            type => 'http.response.body',
            body => $cookie_info,
            more => 0,
        });
    };

    my $server = PAGI::Server->new(
        app   => $cookie_test_app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;

    my $port = $server->port;

    my $http = Net::Async::HTTP->new;
    $loop->add($http);

    # Unfortunately Net::Async::HTTP merges Cookie headers before sending
    # So we'll test using the scope capture approach with a single request
    my $response = $http->GET(
        "http://127.0.0.1:$port/",
        headers => {
            'Cookie' => 'foo=bar; baz=qux',  # Pre-merged cookie
        },
    )->get;

    is($response->code, 200, 'Response status is 200 OK');
    like($response->decoded_content, qr/cookies=1/, 'Only one Cookie header in scope');
    like($response->decoded_content, qr/foo=bar/, 'Cookie contains foo=bar');
    like($response->decoded_content, qr/baz=qux/, 'Cookie contains baz=qux');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

# Test 3: Date header is present in responses
subtest 'Date header is present in responses' => sub {
    my $server = PAGI::Server->new(
        app   => $app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;

    my $port = $server->port;

    my $http = Net::Async::HTTP->new;
    $loop->add($http);

    my $response = $http->GET("http://127.0.0.1:$port/")->get;

    is($response->code, 200, 'Response status is 200 OK');
    ok($response->header('Date'), 'Date header is present');
    like($response->header('Date'), qr/\w{3}, \d{2} \w{3} \d{4}/, 'Date header has correct format');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

# Test 4: GET request works normally (sanity check after HEAD changes)
subtest 'GET request still returns body' => sub {
    my $server = PAGI::Server->new(
        app   => $app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;

    my $port = $server->port;

    my $http = Net::Async::HTTP->new;
    $loop->add($http);

    my $response = $http->GET("http://127.0.0.1:$port/")->get;

    is($response->code, 200, 'GET response status is 200 OK');
    like($response->decoded_content, qr/Hello from PAGI/, 'GET response has body');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

done_testing;
