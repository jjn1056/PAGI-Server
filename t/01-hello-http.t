use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Timer::Countdown;
use Net::Async::HTTP;
use Future::AsyncAwait;
use URI;

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Step 1: Basic HTTP Server
# Tests for examples/01-hello-http/app.pl

my $loop = IO::Async::Loop->new;

# Load the example app
use FindBin;
use lib "$FindBin::Bin/../lib";
my $app_path = "$FindBin::Bin/../examples/01-hello-http/app.pl";
my $app = do $app_path;
die "Could not load app from $app_path: $@" if $@;
die "App did not return a coderef" unless ref $app eq 'CODE';

subtest 'server construction normalizes application providers once' => sub {
    my $provider_app = sub { return 'provider app' };
    my $provider = bless {
        app   => $provider_app,
        calls => 0,
    }, 'PAGITest::ServerBoundaryProvider';

    my $server = PAGI::Server->new(app => $provider, quiet => 1);
    is($provider->{calls}, 1, 'provider to_app is called exactly once');
    is($server->{app}, $provider_app, 'server stores only the normalized coderef');

    $PAGITest::ServerBoundaryCoderef::TO_APP_CALLS = 0;
    my $blessed_app = sub { };
    bless $blessed_app, 'PAGITest::ServerBoundaryCoderef';
    my $coderef_server = PAGI::Server->new(app => $blessed_app, quiet => 1);
    is($coderef_server->{app}, $blessed_app, 'a blessed coderef remains the native app');
    is($PAGITest::ServerBoundaryCoderef::TO_APP_CALLS, 0,
        'native coderef takes precedence over to_app');

    like(
        dies { PAGI::Server->new(app => 'PAGITest::ServerBoundaryProvider', quiet => 1) },
        qr/instantiated application-provider object/i,
        'a package-name string is not treated as a provider',
    );

    my $replacement_app = sub { return 'replacement app' };
    my $replacement = bless {
        app   => $replacement_app,
        calls => 0,
    }, 'PAGITest::ServerBoundaryProvider';
    $server->configure(app => $replacement);
    is($replacement->{calls}, 1, 'configure normalizes a provider exactly once');
    is($server->{app}, $replacement_app, 'configure stores the normalized coderef');
};

{
    package PAGITest::ServerBoundaryProvider;
    sub to_app {
        my ($self) = @_;
        $self->{calls}++;
        return $self->{app};
    }

    package PAGITest::ServerBoundaryCoderef;
    our $TO_APP_CALLS = 0;
    sub to_app {
        $TO_APP_CALLS++;
        die 'to_app must not be called for a native coderef';
    }
}

# Test 1: Server starts and listens on specified port
subtest 'Server starts and listens on port' => sub {
    my $server = PAGI::Server->new(
        app   => $app,
        host  => '127.0.0.1',
        port  => 0,  # Let OS choose port
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;

    ok($server->is_running, 'Server is running');
    ok($server->port > 0, 'Server has a valid port: ' . $server->port);

    $server->shutdown->get;
    ok(!$server->is_running, 'Server stopped running after shutdown');

    $loop->remove($server);
};

# Test 2-5: Basic HTTP response
subtest 'Basic HTTP response returns 200 OK' => sub {
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

    # Test 2: Response status is 200
    is($response->code, 200, 'Response status is 200 OK');

    # Test 3: Content-Type header is text/plain
    is($response->content_type, 'text/plain', 'Content-Type is text/plain');

    # Test 4: Response body contains 'Hello from PAGI'
    like($response->decoded_content, qr/Hello from PAGI/, 'Response body contains "Hello from PAGI"');

    # Test 5: Date header is present
    ok($response->header('Date'), 'Date header is present');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

# Test 6: HTTP scope contains all required keys
subtest 'HTTP scope contains all required keys' => sub {
    my $captured_scope;

    my $scope_test_app = async sub  {
        my ($scope, $receive, $send) = @_;
        # Handle lifespan scope (required by all apps)
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
        app   => $scope_test_app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;

    my $port = $server->port;

    my $http = Net::Async::HTTP->new;
    $loop->add($http);

    my $response = $http->GET("http://127.0.0.1:$port/test/path?query=value")->get;

    # Verify scope keys
    is($captured_scope->{type}, 'http', 'scope.type is http');
    ok(ref $captured_scope->{pagi} eq 'HASH', 'scope.pagi is a hashref');
    is($captured_scope->{pagi}{version}, '0.5', 'scope.pagi.version is 0.5');
    is($captured_scope->{pagi}{spec_version}, '0.4', 'HTTP scope reports spec_version 0.4');
    is($captured_scope->{http_version}, '1.1', 'scope.http_version is 1.1');
    is($captured_scope->{method}, 'GET', 'scope.method is GET');
    is($captured_scope->{scheme}, 'http', 'scope.scheme is http');
    is($captured_scope->{path}, '/test/path', 'scope.path is decoded');
    is($captured_scope->{query_string}, 'query=value', 'scope.query_string is correct');
    ok(ref $captured_scope->{headers} eq 'ARRAY', 'scope.headers is arrayref');
    ok(ref $captured_scope->{client} eq 'ARRAY', 'scope.client is arrayref');
    ok(ref $captured_scope->{server} eq 'ARRAY', 'scope.server is arrayref');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

# Test 7: App exception results in 500 response
subtest 'App exception results in 500 response' => sub {
    my $error_app = async sub  {
        my ($scope, $receive, $send) = @_;
        # Handle lifespan scope first
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

        # For HTTP requests, throw an error
        die "Intentional test error";
    };

    my $server = PAGI::Server->new(
        app   => $error_app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;

    my $port = $server->port;

    my $http = Net::Async::HTTP->new;
    $loop->add($http);

    # Capture warnings from the app error
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, @_ };

    my $response = $http->GET("http://127.0.0.1:$port/")->get;

    is($response->code, 500, 'Response status is 500 for app exception');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

done_testing;
