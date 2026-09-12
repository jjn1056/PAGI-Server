#!/usr/bin/env perl

# =============================================================================
# Test: the terminal response event ends the http scope on HTTP/1.1, whether
# or not the application read the request body.
#
# L<PAGI::Spec::Www> "Meaning per scope": the http scope's clean end is its
# terminal body or trailers, and response_complete is true "once the response
# body completed" -- not once the request finished arriving. "Receiving after
# the scope's end" then reports that end to every receive on the scope,
# pending or later, and "Disconnect - receive event" names http.disconnect as
# what a receive gets "after a response has been sent".
#
# The shape here is the one a 401 or a 413 answers: the client sends a body
# and stalls part way through it, and the application replies in full without
# ever reading. The HTTP/2 twin of these cases is in
# t/http2/43-receive-after-stream-end.t.
#
# What remains of the unread request body is the transport's business, not the
# scope's. This file asserts only that the transport's handling of it never
# writes over the response the scope already finished; the handling itself --
# the discard, its bound, and the connection's fate -- is
# t/80-unread-body-keepalive.t.
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

# Every server built here logs into this collector rather than STDERR, so a
# case can assert the exact set of lines its scope produced.
my @LOG;

sub start_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app        => $app,
        host       => '127.0.0.1',
        port       => 0,
        log_level  => 'debug',
        access_log => undef,
        logger     => sub { push @LOG, $_[0] },
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub connect_client {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 2,
    ) or die "Cannot connect: $!";
    $sock->blocking(0);
    return $sock;
}

# Drive the loop until $cond is true or $timeout expires; returns its final
# truth. No case here expects the bound to be reached.
sub pump_until {
    my ($cond, $timeout) = @_;
    my $deadline = time + ($timeout // 5);
    while (time < $deadline) {
        return 1 if $cond->();
        $loop->loop_once(0.05);
    }
    return $cond->() ? 1 : 0;
}

sub read_available {
    my ($sock, $buf_ref) = @_;
    my $chunk = '';
    my $n = sysread($sock, $chunk, 65536);
    $$buf_ref .= $chunk if defined $n && $n > 0;
    return $n;
}

# Every log line the case's scope produced, counted by level.
sub log_levels_since {
    my ($mark) = @_;
    my %by_level;
    $by_level{ $_->{level} }++ for @LOG[$mark .. $#LOG];
    return \%by_level;
}

# The object readings the spec pins at a clean end, in one hash so a failure
# names every one of them at once.
sub readings_of {
    my ($cs) = @_;
    return {
        is_connected      => $cs->is_connected,
        response_started  => $cs->response_started,
        response_complete => $cs->response_complete,
        disconnect_reason => $cs->disconnect_reason,
    };
}

# A request whose body the client starts and never finishes. Content-Length
# promises 100 bytes; five arrive.
sub send_stalled_request {
    my ($sock, $method) = @_;
    my $req = "$method /guarded HTTP/1.1\r\nHost: localhost\r\n"
            . "Content-Length: 100\r\nContent-Type: text/plain\r\n\r\nhello";
    syswrite($sock, $req);
    return;
}

# The same with chunked framing: one chunk, then silence.
sub send_stalled_chunked_request {
    my ($sock) = @_;
    my $req = "POST /guarded HTTP/1.1\r\nHost: localhost\r\n"
            . "Transfer-Encoding: chunked\r\nContent-Type: text/plain\r\n\r\n"
            . "5\r\nhello\r\n";
    syswrite($sock, $req);
    return;
}

# status => the terminal send that ends the response. None of these read the
# request body first.
my %TERMINAL = (
    'inline body' => [
        { type => 'http.response.start', status => 401,
          headers => [['content-type', 'text/plain'], ['content-length', 4]] },
        { type => 'http.response.body', body => 'nope' },
    ],
    'trailers' => [
        { type => 'http.response.start', status => 401, trailers => 1,
          headers => [['content-type', 'text/plain'], ['trailer', 'x-why']] },
        { type => 'http.response.body', body => 'nope' },
        { type => 'http.response.trailers', headers => [['x-why', 'unauthorized']] },
    ],
);

for my $shape (sort keys %TERMINAL) {
    for my $framing (qw(content-length chunked)) {
        subtest "h1: a $shape response over a stalled $framing body ends the scope" => sub {
            @LOG = ();
            my (@events, @completes, @disconnects);
            my ($returned, $readings) = (0, undef);
            my $app = async sub {
                my ($scope, $receive, $send) = @_;
                my $cs = $scope->{'pagi.connection'};
                $cs->on_complete(sub { push @completes, 1 });
                $cs->on_disconnect(sub { push @disconnects, 1 });
                for my $event (@{ $TERMINAL{$shape} }) { await $send->($event) }
                push @events, await $receive->();
                $readings = readings_of($cs);
                $returned = 1;
                return;
            };

            my $server = start_server($app);
            my $mark   = scalar @LOG;
            my $sock   = connect_client($server->port);
            $framing eq 'chunked' ? send_stalled_chunked_request($sock)
                                  : send_stalled_request($sock, 'POST');

            my $response = '';
            ok(pump_until(sub {
                    read_available($sock, \$response);
                    $returned && $response =~ /\r\n\r\n/;
                }), 'the application returned and the response reached the client');

            is(\@events, [ { type => 'http.disconnect' } ],
                'the receive made after the terminal event resolved with http.disconnect');
            is($readings, {
                is_connected => 0, response_started => 1,
                response_complete => 1, disconnect_reason => undef,
            }, 'the object reads complete, with no disconnect reason');
            is(scalar @completes, 1, 'on_complete fired once');
            is(scalar @disconnects, 0, 'on_disconnect never fired');
            like($response, qr{^HTTP/1\.1 401\b}, 'the 401 reached the client');
            like($response, qr{nope}, 'with its body');
            like($response, qr{x-why: unauthorized}i, 'and its trailer')
                if $shape eq 'trailers';
            is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

            # What is left of the request body is the connection's to read
            # before it can take another request (RFC 9112 section 9.6, and
            # t/80-unread-body-keepalive.t for the whole of that behaviour):
            # until the body is gone, what the client sends is that body. A
            # request line written now is swallowed as the rest of the
            # declared length; under chunked framing it is not valid chunk
            # framing at all and the connection ends. Neither writes anything
            # over the response this scope already finished.
            my $before = $response;
            syswrite($sock, "GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n");
            my $eof = 0;
            pump_until(sub {
                my $n = read_available($sock, \$response);
                $eof = 1 if defined $n && $n == 0;
                $eof;
            }, 1);
            is($response, $before, 'the completed response was never written over');
            $framing eq 'chunked'
                ? ok($eof, 'the broken chunk framing ended the connection')
                : ok(!$eof, 'the connection stays open, still reading the body it was promised');

            close $sock;
            $server->shutdown->get;
            $loop->remove($server);
        };
    }
}

subtest 'h1: a HEAD response over a stalled body ends the scope' => sub {
    @LOG = ();
    my (@events, @completes);
    my ($returned, $readings) = (0, undef);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { push @completes, 1 });
        await $send->({ type => 'http.response.start', status => 401,
                        headers => [['content-type', 'text/plain'], ['content-length', 4]] });
        await $send->({ type => 'http.response.body', body => 'nope' });
        push @events, await $receive->();
        $readings = readings_of($cs);
        $returned = 1;
        return;
    };

    my $server = start_server($app);
    my $mark   = scalar @LOG;
    my $sock   = connect_client($server->port);
    send_stalled_request($sock, 'HEAD');

    my $response = '';
    ok(pump_until(sub {
            read_available($sock, \$response);
            $returned && $response =~ /\r\n\r\n/;
        }), 'the application returned and the response reached the client');

    is(\@events, [ { type => 'http.disconnect' } ],
        'the receive made after the terminal event resolved with http.disconnect');
    is($readings, {
        is_connected => 0, response_started => 1,
        response_complete => 1, disconnect_reason => undef,
    }, 'the object reads complete, with no disconnect reason');
    is(scalar @completes, 1, 'on_complete fired once');
    like($response, qr{^HTTP/1\.1 401\b}, 'the 401 reached the client');
    unlike($response, qr{nope}, 'with the body suppressed, as HEAD requires');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'h1: a receive parked on the body is answered by the terminal event' => sub {
    @LOG = ();
    my (@events, @completes);
    my ($returned, $readings) = (0, undef);
    # The receive is armed before the response and only awaited after it, so
    # it is genuinely pending on the request body when the scope ends.
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { push @completes, 1 });
        push @events, await $receive->();          # the first, partial chunk
        my $parked = $receive->();
        await $send->({ type => 'http.response.start', status => 401,
                        headers => [['content-type', 'text/plain'], ['content-length', 4]] });
        await $send->({ type => 'http.response.body', body => 'nope' });
        push @events, await $parked;
        $readings = readings_of($cs);
        $returned = 1;
        return;
    };

    my $server = start_server($app);
    my $mark   = scalar @LOG;
    my $sock   = connect_client($server->port);
    send_stalled_request($sock, 'POST');

    my $response = '';
    ok(pump_until(sub {
            read_available($sock, \$response);
            $returned && $response =~ /\r\n\r\n/;
        }), 'the application returned and the response reached the client');

    is(\@events, [
        { type => 'http.request', body => 'hello', more => 1 },
        { type => 'http.disconnect' },
    ], 'the pending receive resolved with the scope\'s end, not more body');
    is($readings, {
        is_connected => 0, response_started => 1,
        response_complete => 1, disconnect_reason => undef,
    }, 'the object reads complete, with no disconnect reason');
    is(scalar @completes, 1, 'on_complete fired once');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'h1: a client close after the complete response leaves the scope ended' => sub {
    @LOG = ();
    my (@completes, @disconnects);
    my ($returned, $readings) = (0, undef);
    my $closed_future = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { push @completes, 1 });
        $cs->on_disconnect(sub { push @disconnects, 1 });
        await $send->({ type => 'http.response.start', status => 401,
                        headers => [['content-type', 'text/plain'], ['content-length', 4]] });
        await $send->({ type => 'http.response.body', body => 'nope' });
        await $closed_future;                      # the client leaves here
        $readings = readings_of($cs);
        $returned = 1;
        return;
    };

    my $server = start_server($app);
    my $mark   = scalar @LOG;
    my $sock   = connect_client($server->port);
    send_stalled_request($sock, 'POST');

    my $response = '';
    pump_until(sub { read_available($sock, \$response); $response =~ /\r\n\r\n/ });
    close $sock;
    $loop->loop_once(0.05);
    $closed_future->done;
    pump_until(sub { $returned });

    is($readings, {
        is_connected => 0, response_started => 1,
        response_complete => 1, disconnect_reason => undef,
    }, 'the later client close did not reopen the terminal state');
    is(scalar @completes, 1, 'on_complete fired once');
    is(scalar @disconnects, 0, 'on_disconnect never fired');
    is($returned, 1, 'the application returned');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    $server->shutdown->get;
    $loop->remove($server);
};

done_testing;
