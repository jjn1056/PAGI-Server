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

# Each path exercises one send-contract violation. The app catches the failed
# $send Future and reports what happened in a valid response, so the test
# observes validation through real server behavior, never source inspection.
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    my $path = $scope->{path} // '/';

    my $report = async sub {
        my ($err) = @_;
        $err //= 'NO-ERROR';
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type','text/plain']] });
        await $send->({ type => 'http.response.body', body => $err, more => 0 });
    };

    if ($path eq '/bad-status') {
        my $err = do { local $@; eval { await $send->({ type => 'http.response.start', status => "50\n0" }) }; $@ };
        return await $report->($err);
    }
    if ($path eq '/body-before-start') {
        my $err = do { local $@; eval { await $send->({ type => 'http.response.body', body => 'x' }) }; $@ };
        return await $report->($err);
    }
    if ($path eq '/unknown-type') {
        my $err = do { local $@; eval { await $send->({ type => 'http.response.bod', body => 'x' }) }; $@ };
        return await $report->($err);
    }
    if ($path eq '/duplicate-start') {
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type','text/plain']] });
        my $err = do { local $@; eval { await $send->({ type => 'http.response.start', status => 500 }) }; $@ };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.body', body => "dup:$err", more => 0 });
        return;
    }
    if ($path eq '/undeclared-trailers') {
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type','text/plain']] });
        my $err = do { local $@; eval { await $send->({ type => 'http.response.trailers', headers => [['x-t','1']] }) }; $@ };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.body', body => "trailers:$err", more => 0 });
        return;
    }
    if ($path eq '/post-close') {
        # Post-close no-op: complete the response, wait for the client to go
        # away, then send a malformed event. Spec order: closed-check runs
        # BEFORE validation, so this must RESOLVE, not fail.
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type','text/plain'], ['connection','close']] });
        await $send->({ type => 'http.response.body', body => 'bye', more => 0 });
        while (1) {
            my $e = await $receive->();
            last if $e->{type} eq 'http.disconnect';
        }
        $PostClose::RESULT = do {
            local $@;
            eval { await $send->({ type => 'http.response.bod', body => 'zombie' }); 'resolved' }
                // "failed: $@";
        };
        return;
    }
    await $report->(undef);   # control path: no violation
};

my $loop = IO::Async::Loop->new;
my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
    validate_events => 0,   # THE POINT: validation must run anyway
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;
my $http = Net::Async::HTTP->new;
$loop->add($http);

my $get = sub { $http->GET("http://127.0.0.1:$port$_[0]")->get };

like( $get->('/bad-status')->content, qr/must be a non-negative integer/,
    'malformed status fails the send Future with validate_events => 0' );
like( $get->('/body-before-start')->content, qr/before http\.response\.start/,
    'body before start fails' );
like( $get->('/unknown-type')->content, qr/Unrecognized event type/,
    'unknown type fails' );
like( $get->('/duplicate-start')->content, qr/dup:.*duplicate http\.response\.start/,
    'duplicate start fails without disturbing the real response' );
like( $get->('/undeclared-trailers')->content, qr/trailers:.*not declared/,
    'undeclared trailers fail' );
is( $get->('/ok')->content, 'NO-ERROR', 'a conforming app is unaffected' );

# Post-close: malformed send after client disconnect resolves as a no-op
is( $get->('/post-close')->content, 'bye', 'post-close response delivered' );
$loop->loop_once(0.1) for 1..5;   # let the disconnect and probe land
is( $PostClose::RESULT, 'resolved',
    'malformed send after close resolves as a no-op (closed-check precedes validation)' );

# Dev-configuration parity: validate_events => 1 behaves identically
my $dev_server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
    validate_events => 1,
);
$loop->add($dev_server);
$dev_server->listen->get;
my $dev_port = $dev_server->port;
like( $http->GET("http://127.0.0.1:$dev_port/bad-status")->get->content,
    qr/must be a non-negative integer/,
    'development configuration validates identically' );
$dev_server->shutdown->get;

$server->shutdown->get;
done_testing;
