#!/usr/bin/env perl

# =============================================================================
# Test: Disconnect reasons implementation (PAGI spec compliance)
#
# Per PAGI::Spec::Www: Servers must use standard disconnect reason strings
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;
use PAGI::Server::Connection;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

subtest 'server implements disconnect reason code paths' => sub {
    # Read the Connection.pm source
    my $source = do {
        open my $fh, '<', 'lib/PAGI/Server/Connection.pm' or die "Cannot read: $!";
        local $/;
        <$fh>;
    };

    # Verify protocol_error is set on parse failures
    like(
        $source,
        qr/_handle_disconnect\('protocol_error'\)/,
        'protocol_error reason used for parse failures'
    );

    # Verify server_shutdown auto-detection exists
    like(
        $source,
        qr/'server_shutdown'/,
        'server_shutdown reason is referenced'
    );

    like(
        $source,
        qr/\$self->\{server\}\s*&&\s*\$self->\{server\}\{shutting_down\}/,
        'server shutdown state is checked in _handle_disconnect'
    );

    # Verify other disconnect reasons are used
    like(
        $source,
        qr/'client_closed'/,
        'client_closed reason is used as default'
    );

    like(
        $source,
        qr/'idle_timeout'/,
        'idle_timeout reason is used'
    );

    like(
        $source,
        qr/'client_timeout'/,
        'client_timeout reason is used'
    );

    like(
        $source,
        qr/'body_too_large'/,
        'body_too_large reason is used'
    );

    like(
        $source,
        qr/'keepalive_timeout'/,
        'keepalive_timeout reason is used'
    );

    like(
        $source,
        qr/'queue_overflow'/,
        'queue_overflow reason is used (renamed from policy_violation)'
    );

    unlike(
        $source,
        qr/'policy_violation'/,
        'policy_violation is no longer used (renamed to queue_overflow)'
    );
};

subtest '_handle_disconnect method structure' => sub {
    my $source = do {
        open my $fh, '<', 'lib/PAGI/Server/Connection.pm' or die "Cannot read: $!";
        local $/;
        <$fh>;
    };

    # Verify the auto-detect logic comes before the default
    if ($source =~ /(sub _handle_disconnect \{.*?^})/ms) {
        my $method = $1;

        # Check order: server_shutdown check should come before default assignment
        my $shutdown_pos = index($method, 'server_shutdown');
        my $default_pos = index($method, "//= 'client_closed'");

        ok($shutdown_pos > 0, 'server_shutdown check found');
        ok($default_pos > 0, 'default client_closed found');
        ok($shutdown_pos < $default_pos, 'server_shutdown check comes before default');
    }
    else {
        fail('Could not extract _handle_disconnect method');
    }
};

# =============================================================================
# Test: idle_timer reason token depends on whether a request has already
# completed on this connection (spec: idle_timeout before the first request,
# keepalive_timeout between requests on a keep-alive connection).
#
# Neither reason is observable through the app or the wire today for a plain
# HTTP idle disconnect (no live request scope exists at that moment, and no
# access-log entry or event carries it), so this pins the token the same way
# the server's own disconnect machinery decides it: by spying on the
# private _handle_disconnect_and_close call the real idle timer makes.
# =============================================================================

subtest 'between-requests idle timeout reports keepalive_timeout; pre-request idle reports idle_timeout' => sub {
    my $loop = IO::Async::Loop->new;

    my @captured;
    no warnings 'redefine';
    my $orig = \&PAGI::Server::Connection::_handle_disconnect_and_close;
    local *PAGI::Server::Connection::_handle_disconnect_and_close = sub {
        my ($self, $reason) = @_;
        push @captured, $reason;
        return $orig->(@_);
    };

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
                    last;
                }
            }
            return;
        }
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-length', '2']] });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    };

    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, timeout => 0.3,
    );
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    # Case 1: complete one request over keep-alive, then go idle without
    # sending a second request. The idle timer that re-covers the connection
    # after the response has already run once, so it must report
    # keepalive_timeout.
    my $sock1 = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    ) or die "connect failed: $!";
    print $sock1 "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\n\r\n";

    $sock1->blocking(0);
    my $response = '';
    my $deadline = time + 2;
    while (time < $deadline) {
        $loop->loop_once(0.05);
        my $buf;
        my $n = sysread($sock1, $buf, 4096);
        $response .= $buf if defined $n && $n > 0;
        last if $response =~ /\r\n\r\nok\z/;
    }
    like($response, qr/HTTP\/1\.1 200/, 'case 1: request 1 completed');

    @captured = ();   # only the between-requests idle expiry counts from here
    $deadline = time + 2;
    while (time < $deadline && !@captured) {
        $loop->loop_once(0.05);
    }
    # The idle timer's own call is always first; _close's close_when_empty
    # can trigger a second, harmless client_closed call once the stream
    # actually finishes closing (idempotent no-op downstream) -- only the
    # first call reflects the idle timer's own reason decision.
    is($captured[0], 'keepalive_timeout',
        'case 1: idle timer after a completed request reports keepalive_timeout');
    close($sock1);

    # Case 2: a fresh connection that never sends a request at all. Its idle
    # timer must report idle_timeout (no request has ever completed on it).
    @captured = ();
    my $sock2 = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    ) or die "connect failed: $!";
    $sock2->blocking(0);

    $deadline = time + 2;
    while (time < $deadline && !@captured) {
        $loop->loop_once(0.05);
    }
    is($captured[0], 'idle_timeout',
        'case 2: idle timer before any request reports idle_timeout');
    close($sock2);

    $server->shutdown->get;
    eval { $loop->remove($server) };
};

done_testing;
