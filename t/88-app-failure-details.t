use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# An application that dies is reported with server_error and the error's
# first line, over HTTP/1.1 as over HTTP/2 (t/http2/52), and the scope's
# connection object always settles: on_disconnect fires, disconnect_future
# resolves, is_connected is false. The wire is unchanged: a 1011 Close after
# a WebSocket accept, a 500 before accept or before an SSE stream starts.
# ============================================================

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use Protocol::WebSocket::Frame;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

my $UPGRADE = "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
    . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
my $SSE     = "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n";
my $HTTP    = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";

sub pump { my ($n) = @_; $loop->loop_once(0.02) for 1 .. ($n // 25) }

# Runs one connection: sends $request, then whatever $after writes, and
# returns what the app's connection object reported, the error log lines,
# and every byte the server wrote.
sub run_h1 {
    my (%o) = @_;
    my (%r, @log);
    socketpair(my $server_end, my $client, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $server_end->blocking(0);
    $client->blocking(0);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return if $scope->{type} eq 'lifespan';
        my $cs = $r{cs} = $scope->{'pagi.connection'};
        $cs->on_disconnect(sub { $r{on_disconnect}++ });
        $cs->on_complete(sub { $r{on_complete}++ });
        $r{df} = $cs->disconnect_future;
        await $o{app}->($scope, $receive, $send);
    };
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1,
        logger => sub { push @log, $_[0] });
    $loop->add($server);
    my $stream = IO::Async::Stream->new(read_handle => $server_end, write_handle => $server_end,
        on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(stream => $stream, app => $app,
        protocol => $protocol, server => $server);
    $server->add_child($stream);
    $conn->start;

    my $wire = '';
    my $read = sub { my $buf = ''; $wire .= $buf while ($client->sysread($buf, 65536) // 0) > 0 };
    $client->syswrite($o{request});
    pump();
    $read->();
    if ($o{after}) { $o{after}->($client); pump(); $read->() }
    pump();
    $read->();
    my $eof = do { my $buf = ''; my $n = $client->sysread($buf, 1); defined($n) && $n == 0 };
    $loop->remove($server);
    return {
        %r,
        log  => [ map { $_->{message} } grep { ($_->{level} // '') eq 'error' } @log ],
        wire => $wire,
        eof  => $eof,
    };
}

sub masked_text  { Protocol::WebSocket::Frame->new(type => 'text', buffer => $_[0], masked => 1)->to_bytes }
sub masked_close { Protocol::WebSocket::Frame->new(type => 'close', buffer => pack('n', 1000), masked => 1)->to_bytes }

# The Close codes the server sent, in order, from the bytes after the 101.
sub close_codes {
    my ($wire) = @_;
    my (undef, $frames) = split /\r\n\r\n/, $wire, 2;
    my $parser = Protocol::WebSocket::Frame->new;
    $parser->append($frames // '');
    my @codes;
    while (defined(my $bytes = $parser->next_bytes)) {
        push @codes, unpack('n', $bytes) if $parser->is_close;
    }
    return \@codes;
}

sub status_of { my ($wire) = @_; return ($wire =~ m{\AHTTP/1\.1 (\d+)})[0] }

sub settled_ok {
    my ($r, $reason, $detail, $label) = @_;
    is($r->{cs}->disconnect_reason, $reason, "$label: reason");
    is($r->{cs}->disconnect_detail, $detail, "$label: detail");
    ok(!$r->{cs}->is_connected, "$label: the connection object is not connected");
    is($r->{on_disconnect} // 0, 1, "$label: on_disconnect fired once");
    ok(!$r->{on_complete}, "$label: on_complete did not fire");
    ok($r->{df}->is_ready, "$label: disconnect_future resolved");
}

my $dies = sub { die "the handler broke\nsecond line\n" };

subtest 'W1: a WebSocket app that dies after accept' => sub {
    my $r = run_h1(request => $UPGRADE, after => sub { $_[0]->syswrite(masked_text('hello')) },
        app => async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->();
            await $send->({ type => 'websocket.accept' });
            await $receive->();
            $dies->();
        });
    settled_ok($r, 'server_error', 'the handler broke', 'W1');
    is(close_codes($r->{wire}), [1011], 'W1: one Close, 1011');
    is(scalar @{ $r->{log} }, 1, 'W1: one error log line') or diag(join "\n", @{ $r->{log} });
};

subtest 'W2: a WebSocket app that dies before accepting' => sub {
    my $r = run_h1(request => $UPGRADE, app => async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        $dies->();
    });
    settled_ok($r, 'server_error', 'the handler broke', 'W2');
    is(status_of($r->{wire}), 500, 'W2: a 500');
    is(scalar @{ $r->{log} }, 1, 'W2: one error log line') or diag(join "\n", @{ $r->{log} });
};

subtest 'W3: an SSE app that dies before sse.start' => sub {
    my $r = run_h1(request => $SSE, app => async sub { $dies->() });
    settled_ok($r, 'server_error', 'the handler broke', 'W3');
    is(status_of($r->{wire}), 500, 'W3: a 500');
    is(scalar @{ $r->{log} }, 1, 'W3: one error log line') or diag(join "\n", @{ $r->{log} });
};

subtest 'an exception object: its first line, without the location' => sub {
    package Local::Boom { use overload '""' => sub { "boom at x.pl line 3.\n" }, fallback => 1 }
    my $r = run_h1(request => $UPGRADE, after => sub { $_[0]->syswrite(masked_text('hello')) },
        app => async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->();
            await $send->({ type => 'websocket.accept' });
            await $receive->();
            die bless {}, 'Local::Boom';
        });
    is($r->{cs}->disconnect_detail, 'boom', 'the detail');
};

# W4: an unfinished refusal reports the error when the app died, and the
# refusal left undone when it returned.
sub refusing {
    my ($ws, $then) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->() if $ws;
        await $send->({ type => 'http.response.start', status => 403, headers => [] });
        await $send->({ type => 'http.response.body', body => 'no', more => 1 });
        $dies->() if $then eq 'dies';
        return;
    };
}

subtest 'W4: an unfinished refusal' => sub {
    for my $case ([ WebSocket => 1, $UPGRADE ], [ SSE => 0, $SSE ]) {
        my ($label, $ws, $request) = @$case;
        my $r = run_h1(request => $request, app => refusing($ws, 'returns'));
        settled_ok($r, 'server_error', 'refusal started but never completed', "$label, returns");
        $r = run_h1(request => $request, app => refusing($ws, 'dies'));
        settled_ok($r, 'server_error', 'the handler broke', "$label, dies");
        is(scalar @{ $r->{log} }, 1, "$label, dies: one error log line") or diag(join "\n", @{ $r->{log} });
        is(status_of($r->{wire}), 403, "$label: the refusal's own status went out");
    }
};

subtest 'a failure after the peer closed' => sub {
    my $r = run_h1(request => $UPGRADE, after => sub { $_[0]->syswrite(masked_close()) },
        app => async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->();
            await $send->({ type => 'websocket.accept' });
            await $receive->();             # websocket.disconnect, from the peer's Close
            $dies->();
        });
    # The closing handshake completed before the app died, so the exchange
    # is recorded as complete; the failure after it is only logged.
    is(close_codes($r->{wire}), [1000], 'one Close: the reply to the peer, no 1011 after it');
    ok(!$r->{cs}->is_connected, 'settled');
    is($r->{cs}->disconnect_reason, undef, 'a clean end has no reason');
    ok($r->{cs}->response_complete, 'and is complete');
    is(scalar @{ $r->{log} }, 1, 'the failure is logged once') or diag(join "\n", @{ $r->{log} });
};

subtest 'controls: an app that returns without starting' => sub {
    my $ws = run_h1(request => $UPGRADE, app => async sub { my ($s, $receive) = @_; await $receive->() });
    settled_ok($ws, 'server_error', 'no response was started', 'WebSocket');
    my $sse = run_h1(request => $SSE, app => async sub { return });
    settled_ok($sse, 'server_error', 'no response was started', 'SSE');
};

subtest 'W3 does not leave the connection open for another request' => sub {
    my $r = run_h1(request => $SSE . $HTTP, app => async sub { $dies->() });
    my $responses = () = $r->{wire} =~ m{^HTTP/1\.1 \d+}mg;
    is($responses, 1, 'one response only');
    ok($r->{eof}, 'the server closed the connection');
};

subtest 'plain HTTP: an app that dies before http.response.start' => sub {
    my $r = run_h1(request => $HTTP, app => async sub { $dies->() });
    settled_ok($r, 'server_error', 'the handler broke', 'HTTP');
    is(status_of($r->{wire}), 500, 'a 500');
};

done_testing;
