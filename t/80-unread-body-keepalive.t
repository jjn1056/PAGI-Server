#!/usr/bin/env perl

# =============================================================================
# Test: an unread request body never becomes the next request (HTTP/1.1)
#
# RFC 9112 section 9.6 ("Tear-down"): a server that sends a response before
# receiving the entire request content MUST either read the whole content or
# close the connection afterwards -- the content is framed into the connection
# (section 6.3, "Message Body Length") and cannot be skipped over. An
# application that refuses an upload without reading it (a 401, a 413) is the
# ordinary way a server ends up in that position.
#
# So the request tail asks whether this request's body was fully consumed, and
# when it was not it discards what is left -- the declared remainder, or the
# chunked body up to its terminator -- before the connection will parse
# anything as a new request. The discard is bounded by max_body_size; over
# that bound, and on a client that stops sending, the connection closes
# instead.
#
# The keep-alive shapes that do not involve a body are pinned elsewhere:
# t/22-content-length-keepalive.t (a read body, then reuse) and
# t/10-http-compliance.t "HTTP pipelining handles multiple requests" (two
# bodiless requests in one write). This file covers the body cases.
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

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

# Every case pumps a loop against a peer that may never speak again; a wedged
# pump must fail this file rather than hang the suite.
$SIG{ALRM} = sub { die "t/80 exceeded its time bound\n" };
alarm 120;

# Nothing here may write to stderr unasked.
my @warnings;
$SIG{__WARN__} = sub { push @warnings, $_[0] };

sub assert_quiet {
    my ($label, $log) = @_;
    my @w = @warnings;
    @warnings = ();
    is(scalar(@w), 0, "$label: nothing was warned to stderr") or diag("unexpected: @w");
    is($log, [], "$label: the server logged nothing at any level")
        or diag(join "\n", map { "[$_->{level}] $_->{message}" } @$log);
    @$log = ();
    return;
}

# The application every case runs: /upload answers without ever calling
# receive() (the unauthenticated upload), every other path reads its body to
# the end and answers 200 with the path it served.
my (@served, @completes, @disconnects, @readings);
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    my $cs = $scope->{'pagi.connection'};
    $cs->on_complete(sub { push @completes, $scope->{path} });
    $cs->on_disconnect(sub { push @disconnects, $scope->{path} });
    push @served, { request => "$scope->{method} $scope->{path}",
                    client_port => $scope->{client}[1] };

    if ($scope->{path} eq '/upload') {
        await $send->({ type => 'http.response.start', status => 401,
                        headers => [['content-type', 'text/plain'], ['content-length', 7]] });
        await $send->({ type => 'http.response.body', body => 'sign in' });
        push @readings, {
            is_connected      => $cs->is_connected,
            response_complete => $cs->response_complete,
            disconnect_reason => $cs->disconnect_reason,
        };
        return;
    }

    my $body = '';
    while (1) {
        my $event = await $receive->();
        last if $event->{type} ne 'http.request';
        $body .= $event->{body} // '';
        last unless $event->{more};
    }
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain'],
                                ['content-length', length("$scope->{path}:$body")]] });
    await $send->({ type => 'http.response.body', body => "$scope->{path}:$body" });
    return;
};

sub start_server {
    my (%o) = @_;
    my $server = PAGI::Server->new(
        app              => $app,
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        log_level        => 'debug',
        access_log       => undef,
        shutdown_timeout => 1,
        logger           => sub { push @{$o{log}}, $_[0] },
        (exists $o{max_body_size} ? (max_body_size => $o{max_body_size}) : ()),
        (exists $o{timeout}       ? (timeout       => $o{timeout})       : ()),
    );
    $loop->add($server);
    $server->listen->get;
    # The banner the server writes at startup is not this file's subject; what
    # follows is, and every case asserts the whole of it.
    @{$o{log}} = ();
    @served = @completes = @disconnects = @readings = ();
    return $server;
}

# A socket plus a pump that accumulates everything the server writes and
# notices the peer's EOF.
sub open_client {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5)
        or die "connect: $!";
    $sock->blocking(0);
    my ($wire, $eof) = ('', 0);
    my $pump = sub {
        my ($cond, $rounds) = @_;
        for (1 .. ($rounds // 60)) {
            $loop->loop_once(0.02);
            my $buf;
            my $n = sysread($sock, $buf, 65536);
            if (defined $n) { $n ? ($wire .= $buf) : ($eof = 1) }
            last if $cond && $cond->();
        }
        return;
    };
    return ($sock, $pump, \$wire, \$eof);
}

# A response's status line can follow the previous response's body with no
# newline between them, so this does not anchor to a line start.
sub statuses { my ($wire) = @_; return [ $wire =~ m{HTTP/1\.1 (\d\d\d) }g ] }
sub chunk        { my ($d) = @_; return sprintf("%x\r\n%s\r\n", length($d), $d) }

my $CHUNKED_HEAD = "POST /upload HTTP/1.1\r\nHost: localhost\r\n"
                 . "Transfer-Encoding: chunked\r\n\r\n";
my $SECOND       = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";

# =============================================================================
# 1. A declared body the application never read: the remainder is discarded,
#    and the request that follows it on the same connection is answered.
# =============================================================================
subtest 'h1: the remainder of an unread declared body is not the next request' => sub {
    my @log;
    my $server = start_server(log => \@log);
    my ($sock, $pump, $wire, $eof) = open_client($server->port);

    # Headers plus the first 100 of the 500 bytes the client promised.
    syswrite($sock, "POST /upload HTTP/1.1\r\nHost: localhost\r\n"
                  . "Content-Length: 500\r\n\r\n" . ('A' x 100));
    $pump->(sub { $$wire =~ /sign in/ });
    like($$wire, qr{^HTTP/1\.1 401\b}, 'the 401 reached the client without the body being read');

    # The client sends the rest of the body it promised, then its next request.
    syswrite($sock, ('B' x 400) . $SECOND);
    $pump->(sub { $$wire =~ /health/ });

    is(statuses($$wire), ['401', '200'],
        'the second request was answered, and nothing was answered twice');
    like($$wire, qr{/health:\s*$}m, 'the second request was GET /health, parsed from the request line');
    is([ map { $_->{request} } @served ], ['POST /upload', 'GET /health'],
        'the application saw exactly those two requests -- the second one parsed'
            . ' from its own request line, not from the body that preceded it');
    is($served[0]{client_port}, $served[1]{client_port},
        'both arrived on the one connection');
    ok(!$$eof, 'which is still open');
    is(\@completes, ['/upload', '/health'], 'both scopes ended with on_complete');
    is(\@disconnects, [], 'neither ended with on_disconnect');
    is($readings[0], { is_connected => 0, response_complete => 1, disconnect_reason => undef },
        'the refusing scope read complete, with no disconnect reason');
    assert_quiet('an unread declared body', \@log);

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

# =============================================================================
# 2. The same for a chunked body: the discard runs to the terminator.
# =============================================================================
subtest 'h1: the remainder of an unread chunked body is not the next request' => sub {
    my @log;
    my $server = start_server(log => \@log);
    my ($sock, $pump, $wire, $eof) = open_client($server->port);

    syswrite($sock, $CHUNKED_HEAD . chunk('A' x 100));
    $pump->(sub { $$wire =~ /sign in/ });
    like($$wire, qr{^HTTP/1\.1 401\b}, 'the 401 reached the client without the body being read');

    # The rest of the chunked body, its terminator, then the next request.
    syswrite($sock, chunk('B' x 400) . "0\r\n\r\n" . $SECOND);
    $pump->(sub { $$wire =~ /health/ });

    is(statuses($$wire), ['401', '200'],
        'the second request was answered, and nothing was answered twice');
    is([ map { $_->{request} } @served ], ['POST /upload', 'GET /health'],
        'the application saw exactly those two requests -- the second one parsed'
            . ' from its own request line, not from the body that preceded it');
    is($served[0]{client_port}, $served[1]{client_port},
        'both arrived on the one connection');
    ok(!$$eof, 'the connection is still open');
    is(\@completes, ['/upload', '/health'], 'both scopes ended with on_complete');
    is(\@disconnects, [], 'neither ended with on_disconnect');
    assert_quiet('an unread chunked body', \@log);

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

# =============================================================================
# 3. The discard is bounded by max_body_size. A chunked body never read by the
#    application is the one shape that can cross the bound after the response:
#    a declared length over it is answered 413 before the application runs
#    (t/22-content-length-keepalive.t), so it never reaches the tail.
# =============================================================================
subtest 'h1: an unread body over max_body_size closes the connection' => sub {
    my @log;
    my $server = start_server(log => \@log, max_body_size => 1000);
    my ($sock, $pump, $wire, $eof) = open_client($server->port);

    syswrite($sock, $CHUNKED_HEAD . chunk('A' x 100));
    $pump->(sub { $$wire =~ /sign in/ });
    like($$wire, qr{^HTTP/1\.1 401\b}, 'the 401 reached the client');

    my ($conn) = values %{ $server->{connections} };
    ok($conn, 'the connection is still on the server');

    # More body than the limit allows, and a request behind it that must never
    # be answered.
    syswrite($sock, chunk('B' x 1500) . "0\r\n\r\n" . $SECOND);
    $pump->(sub { $$eof });

    ok($$eof, 'the connection closed after the response');
    is(statuses($$wire), ['401'],
        'the client saw its 401 and nothing else -- no 413 over a finished response');
    is([ map { $_->{request} } @served ], ['POST /upload'],
        'the request behind the oversized body never became a request');
    is($conn->{end_reason}, 'body_too_large', 'the connection ended with body_too_large');
    is($conn->{end_detail}, 'unread request body exceeded max_body_size (1000 bytes)',
        'and says so');

    # The scope itself ended cleanly with the 401: the close is a transport
    # decision taken after it, so on_complete fired and on_disconnect did not.
    is(\@completes, ['/upload'], 'on_complete fired for the refusing scope');
    is(\@disconnects, [], 'on_disconnect never fired');
    is($readings[0], { is_connected => 0, response_complete => 1, disconnect_reason => undef },
        'and the scope carries no disconnect reason');
    assert_quiet('an unread body over the bound', \@log);

    close $sock;
    undef $conn;
    $server->shutdown->get;
    $loop->remove($server);
};

# =============================================================================
# 4. A client that stops sending mid-body is closed by the idle timer, under
#    the reason that timer already reports for a connection that has served a
#    request. The server itself is unaffected.
# =============================================================================
subtest 'h1: a client that stalls mid-body is closed by the keep-alive timeout' => sub {
    my @log;
    my $server = start_server(log => \@log, timeout => 1);
    my ($sock, $pump, $wire, $eof) = open_client($server->port);

    syswrite($sock, "POST /upload HTTP/1.1\r\nHost: localhost\r\n"
                  . "Content-Length: 500\r\n\r\n" . ('A' x 100));
    $pump->(sub { $$wire =~ /sign in/ });
    like($$wire, qr{^HTTP/1\.1 401\b}, 'the 401 reached the client');

    my ($conn) = values %{ $server->{connections} };
    ok($conn, 'the connection is still on the server');

    # The remaining 400 bytes never arrive.
    $pump->(sub { $$eof }, 150);
    ok($$eof, 'the connection closed');
    is($conn->{end_reason}, 'keepalive_timeout',
        'under the timeout a connection that has served a request already reports');
    is($conn->{end_detail}, 'no traffic for 1s', 'with its usual detail');
    is(\@disconnects, [], 'the completed scope was not reopened as a disconnect');

    # The process is unaffected: a second connection is served normally.
    my ($sock2, $pump2, $wire2) = open_client($server->port);
    syswrite($sock2, "POST /echo HTTP/1.1\r\nHost: localhost\r\n"
                   . "Content-Length: 2\r\n\r\nhi");
    $pump2->(sub { $$wire2 =~ /echo:hi/ });
    like($$wire2, qr{^HTTP/1\.1 200\b}, 'a later connection is served normally');
    assert_quiet('a stalled body', \@log);

    close $sock;
    close $sock2;
    undef $conn;
    $server->shutdown->get;
    $loop->remove($server);
};

# =============================================================================
# 5. Pipelining regression: complete requests written in one go still all
#    answer, whether or not the application reads the bodies they carry.
# =============================================================================
subtest 'h1: pipelined requests carrying bodies all answer' => sub {
    my @log;
    my $server = start_server(log => \@log);
    my ($sock, $pump, $wire, $eof) = open_client($server->port);

    syswrite($sock,
        "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\nhi"
      . "POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nADMIN"
      . $SECOND);
    $pump->(sub { $$wire =~ /health/ });

    is(statuses($$wire), ['200', '401', '200'],
        'a read body, an unread body and a bodiless request all answered in order');
    is([ map { $_->{request} } @served ], ['POST /echo', 'POST /upload', 'GET /health'],
        'the application saw exactly those three requests, each parsed from its'
            . ' own request line');
    ok(!$$eof, 'the connection is still open');
    is(\@completes, ['/echo', '/upload', '/health'], 'every scope ended with on_complete');
    assert_quiet('pipelined bodies', \@log);

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

alarm 0;
done_testing;
