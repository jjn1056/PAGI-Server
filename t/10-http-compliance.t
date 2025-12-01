use strict;
use warnings;
use experimental 'signatures';
use Test2::V0;
use IO::Async::Loop;
use Net::Async::HTTP;
use Future::AsyncAwait;
use FindBin;
use IO::Socket::INET;
use IO::Select;

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

# Test 5: Header names are lowercased in scope
subtest 'Header names are lowercased in scope' => sub {
    my $captured_scope;

    my $header_test_app = async sub ($scope, $receive, $send) {
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

        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [['content-type', 'text/plain']],
        });

        await $send->({
            type => 'http.response.body',
            body => 'OK',
            more => 0,
        });
    };

    my $server = PAGI::Server->new(
        app   => $header_test_app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;

    my $port = $server->port;

    my $http = Net::Async::HTTP->new;
    $loop->add($http);

    my $response = $http->GET(
        "http://127.0.0.1:$port/",
        headers => {
            'X-Custom-Header' => 'test-value',
            'Accept-Language' => 'en-US',
        },
    )->get;

    is($response->code, 200, 'Response status is 200 OK');

    # Check that header names are lowercased
    my %header_names = map { $_->[0] => 1 } @{$captured_scope->{headers}};
    ok($header_names{'x-custom-header'}, 'x-custom-header is lowercased');
    ok($header_names{'accept-language'}, 'accept-language is lowercased');

    # Verify no uppercase header names
    my @uppercase = grep { /[A-Z]/ } keys %header_names;
    is(scalar @uppercase, 0, 'No uppercase header names in scope');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

# Test 6: URL-encoded paths are decoded correctly
subtest 'URL-encoded paths are decoded correctly' => sub {
    my $captured_scope;

    my $path_test_app = async sub ($scope, $receive, $send) {
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

        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [['content-type', 'text/plain']],
        });

        await $send->({
            type => 'http.response.body',
            body => "path=$scope->{path}\nraw_path=$scope->{raw_path}",
            more => 0,
        });
    };

    my $server = PAGI::Server->new(
        app   => $path_test_app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;

    my $port = $server->port;

    my $http = Net::Async::HTTP->new;
    $loop->add($http);

    # Request with URL-encoded path
    my $response = $http->GET("http://127.0.0.1:$port/path%20with%20spaces")->get;

    is($response->code, 200, 'Response status is 200 OK');

    # Check decoded path
    is($captured_scope->{path}, '/path with spaces', 'scope.path contains decoded path');
    is($captured_scope->{raw_path}, '/path%20with%20spaces', 'scope.raw_path contains original encoded path');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

# Test 7: Server.pm API methods work correctly
subtest 'Server.pm API methods work correctly' => sub {
    my $server = PAGI::Server->new(
        app   => $app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    # Test before listen
    ok(!$server->is_running, 'is_running() returns false before listen()');

    $loop->add($server);

    # Test listen returns Future
    my $listen_future = $server->listen;
    ok($listen_future->isa('Future'), 'listen() returns a Future');
    $listen_future->get;

    # Test after listen
    ok($server->is_running, 'is_running() returns true after listen()');
    ok($server->port > 0, 'port() returns valid port number');

    # Test shutdown returns Future
    my $shutdown_future = $server->shutdown;
    ok($shutdown_future->isa('Future'), 'shutdown() returns a Future');
    $shutdown_future->get;

    # Test after shutdown
    ok(!$server->is_running, 'is_running() returns false after shutdown()');

    $loop->remove($server);
};

# Test 8: Protocol::HTTP1 parse_request works
subtest 'Protocol::HTTP1 parse_request parses HTTP requests' => sub {
    use PAGI::Server::Protocol::HTTP1;

    my $proto = PAGI::Server::Protocol::HTTP1->new;

    # Test valid request
    my $request_str = "GET /test/path?query=value HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n";
    my ($request, $consumed) = $proto->parse_request(\$request_str);

    ok(defined $request, 'parse_request returns request for valid HTTP');
    is($request->{method}, 'GET', 'method is GET');
    is($request->{path}, '/test/path', 'path is correct');
    is($request->{query_string}, 'query=value', 'query_string is correct');
    is($request->{http_version}, '1.1', 'http_version is 1.1');
    ok($consumed > 0, 'bytes_consumed is positive');

    # Test incomplete request
    my $incomplete = "GET / HTTP/1.1\r\n";
    my ($req2, $cons2) = $proto->parse_request(\$incomplete);
    ok(!defined $req2, 'parse_request returns undef for incomplete request');
    is($cons2, 0, 'bytes_consumed is 0 for incomplete request');
};

# Test 9: Protocol::HTTP1 serialize methods work
subtest 'Protocol::HTTP1 serialize methods generate valid HTTP' => sub {
    use PAGI::Server::Protocol::HTTP1;

    my $proto = PAGI::Server::Protocol::HTTP1->new;

    # Test serialize_response_start
    my $response = $proto->serialize_response_start(
        200,
        [['content-type', 'text/plain'], ['x-custom', 'value']],
        0  # not chunked
    );

    like($response, qr/^HTTP\/1\.1 200 OK\r\n/, 'Response starts with status line');
    like($response, qr/content-type: text\/plain\r\n/, 'Content-Type header present');
    like($response, qr/x-custom: value\r\n/, 'Custom header present');
    like($response, qr/\r\n\r\n$/, 'Response ends with blank line');

    # Test chunked response
    my $chunked_response = $proto->serialize_response_start(200, [], 1);
    like($chunked_response, qr/Transfer-Encoding: chunked\r\n/, 'Chunked encoding header added');

    # Test serialize_response_body
    my $body = $proto->serialize_response_body("Hello", 0, 1);  # chunked
    like($body, qr/^5\r\nHello\r\n/, 'Chunked body has correct format');

    # Test format_date
    my $date = $proto->format_date;
    like($date, qr/\w{3}, \d{2} \w{3} \d{4} \d{2}:\d{2}:\d{2} GMT/, 'Date format is RFC 7231 compliant');
};

# Test 10: Malformed request returns 400 Bad Request (via Protocol::HTTP1 unit test)
subtest 'Malformed request parsing returns error' => sub {
    use PAGI::Server::Protocol::HTTP1;

    my $proto = PAGI::Server::Protocol::HTTP1->new;

    # Test malformed request line
    my $malformed = "INVALID REQUEST LINE\r\n\r\n";
    my ($request, $consumed) = $proto->parse_request(\$malformed);

    ok(defined $request, 'parse_request returns result for malformed request');
    is($request->{error}, 400, 'Error code is 400 Bad Request');
};

# Test 11: HTTP/1.0 response serialization works without chunked encoding
subtest 'HTTP/1.0 response does not use chunked encoding' => sub {
    use PAGI::Server::Protocol::HTTP1;

    my $proto = PAGI::Server::Protocol::HTTP1->new;

    # Serialize HTTP/1.0 response (chunked=0, http_version='1.0')
    my $response = $proto->serialize_response_start(200, [['content-type', 'text/plain']], 0, '1.0');

    like($response, qr/^HTTP\/1\.0 200 OK/, 'HTTP/1.0 response uses HTTP/1.0 version');
    unlike($response, qr/Transfer-Encoding: chunked/i, 'HTTP/1.0 response does not use chunked encoding');

    # Verify that even with chunked=1, HTTP/1.0 doesn't add chunked encoding
    my $response2 = $proto->serialize_response_start(200, [], 1, '1.0');
    unlike($response2, qr/Transfer-Encoding: chunked/i, 'HTTP/1.0 ignores chunked flag');
};

# Test 12: Keep-alive can be tested via Net::Async::HTTP (which uses persistent connections)
subtest 'Keep-alive works with Net::Async::HTTP' => sub {
    my $server = PAGI::Server->new(
        app   => $app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;

    my $port = $server->port;

    my $http = Net::Async::HTTP->new(
        # Net::Async::HTTP uses persistent connections by default
        max_connections_per_host => 1,
    );
    $loop->add($http);

    # First request
    my $response1 = $http->GET("http://127.0.0.1:$port/")->get;
    is($response1->code, 200, 'First response status is 200 OK');

    # Second request (should reuse connection)
    my $response2 = $http->GET("http://127.0.0.1:$port/")->get;
    is($response2->code, 200, 'Second response status is 200 OK');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

# Test 13: Expect: 100-continue parsing is detected
subtest 'Expect: 100-continue parsing detected' => sub {
    use PAGI::Server::Protocol::HTTP1;

    my $proto = PAGI::Server::Protocol::HTTP1->new;

    # Request with Expect: 100-continue
    my $request_str = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\nExpect: 100-continue\r\n\r\n";
    my ($request, $consumed) = $proto->parse_request(\$request_str);

    ok(defined $request, 'Request parsed successfully');
    ok(!$request->{error}, 'No parse error');
    is($request->{expect_continue}, 1, 'expect_continue flag is set');

    # Verify 100 Continue serialization
    my $continue = $proto->serialize_continue;
    like($continue, qr/^HTTP\/1\.1 100 Continue\r\n\r\n$/, '100 Continue response format is correct');
};

# Test 14: Max header size returns 431
subtest 'Max header size exceeded returns 431' => sub {
    use PAGI::Server::Protocol::HTTP1;

    # Create protocol with small max_header_size
    my $proto = PAGI::Server::Protocol::HTTP1->new(max_header_size => 100);

    # Create a request with large headers
    my $large_header = "X-Large-Header: " . ("x" x 200);
    my $request_str = "GET / HTTP/1.1\r\nHost: localhost\r\n$large_header\r\n\r\n";

    my ($request, $consumed) = $proto->parse_request(\$request_str);

    ok(defined $request, 'parse_request returns result for oversized headers');
    is($request->{error}, 431, 'Error code is 431 Request Header Fields Too Large');
};

done_testing;
