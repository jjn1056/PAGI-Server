use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# HTTP/2 counterpart to t/http-incomplete-response.t: an application that
# returns without ever sending http.response.start must yield a 500 on the
# stream (not a hung or silently-closed stream).

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# --- h2 test harness (same pattern as t/http2/05-request-lifecycle.t) --------

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app   => $args{app} // sub { },
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
        http2 => 1,
        %args,
    );
    $loop->add($server);
    return $server;
}

sub create_h2_connection {
    my (%overrides) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app    = $overrides{app} // sub { };
    my $server = $overrides{server} // create_test_server(app => $app);
    my $stream = IO::Async::Stream->new(
        read_handle  => $sock_a,
        write_handle => $sock_a,
        on_read      => sub { 0 },
    );
    my $conn = PAGI::Server::Connection->new(
        stream        => $stream,
        app           => $app,
        protocol      => $protocol,
        server        => $server,
        h2_protocol   => $server->{http2_protocol},
        alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;
    return ($conn, $stream, $sock_b, $server);
}

sub create_client {
    my (%overrides) = @_;
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(
        callbacks => {
            on_begin_headers   => sub { 0 },
            on_header          => $overrides{on_header}          // sub { 0 },
            on_frame_recv      => sub { 0 },
            on_data_chunk_recv => $overrides{on_data_chunk_recv} // sub { 0 },
            on_stream_close    => $overrides{on_stream_close}    // sub { 0 },
        },
    );
}

# Bidirectional pump (unlike read_response, which only drains server->client):
# needed once a test submits more than one request over the same connection,
# since later requests must flush the client's own queued frames too.
sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    $rounds //= 20;
    for (1 .. $rounds) {
        $loop->loop_once(0.025);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

sub complete_h2_handshake {
    my ($client, $client_sock) = @_;
    $loop->loop_once(0.1);
    my $server_settings = '';
    $client_sock->sysread($server_settings, 4096);
    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($server_settings);
    $loop->loop_once(0.1);
    my $ack = '';
    $client_sock->sysread($ack, 4096);
    $client->mem_recv($ack) if length($ack);
    my $client_ack = $client->mem_send;
    $client_sock->syswrite($client_ack) if length($client_ack);
    $loop->loop_once(0.1);
    my $extra = '';
    $client_sock->sysread($extra, 4096);
    $client->mem_recv($extra) if length($extra);
}

sub read_response {
    my ($client, $client_sock, $rounds) = @_;
    $rounds //= 10;
    for (1 .. $rounds) {
        $loop->loop_once(0.1);
        my $data = '';
        $client_sock->sysread($data, 8192);
        $client->mem_recv($data) if length($data);
    }
}

# --- the test ----------------------------------------------------------------

subtest 'h2: app returning without a response yields 500' => sub {
    my $disconnect_reason;      # undef unless on_disconnect fires
    my $started_at_disconnect;  # response_started as the callback observed it
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $cs = $scope->{'pagi.connection'};
        $cs->on_disconnect(sub {
            $disconnect_reason    = $_[0];
            $started_at_disconnect = $cs->response_started;
        });
        await $receive->();   # consume the request
        return;               # no http.response.start -> incomplete
    };

    my ($conn, $stream, $client_sock, $server) = create_h2_connection(app => $app);

    my %headers;
    my $client = create_client(
        on_header => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
    );

    complete_h2_handshake($client, $client_sock);

    $client->submit_request(
        method    => 'GET',
        path      => '/none',
        scheme    => 'https',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    read_response($client, $client_sock, 12);

    is($headers{':status'}, '500',
        'an h2 app that starts no response gets a 500 backstop');
    is($disconnect_reason, 'server_error',
        'on_disconnect fired with server_error for the no-response path');
    # Spec section 9.1: the server-generated 500 IS this request's response,
    # so a callback watching the disconnect must see response_started true.
    is($started_at_disconnect, 1,
        'on_disconnect observed response_started true (the 500 is the response)');

    $stream->close_now;
    $loop->remove($server);
};

# =============================================================================
# /incomplete and /throw-after-start: a response started but never finished
# (app returned early, or threw after starting) must reset the stream --
# never synthesize END_STREAM over a body the app didn't actually finish
# sending. Both apps route on path so a plain "/ok" request on the SAME
# connection, submitted after the reset, exercises "sibling streams
# untouched" / "the connection stays healthy".
# =============================================================================

package Incomplete;
our $CS;              # connection_state captured by the /incomplete app
our $COMPLETE = 0;     # flips true if on_complete ever fires (it must not)
our $DISCONNECT;       # reason string once on_disconnect fires
our $STARTED = 0;

our $THROW_CS;
our $THROW_COMPLETE = 0;
our $THROW_DISCONNECT;
our $THROW_STARTED = 0;

our $DONE_CS;
our $DONE_COMPLETE = 0;
our $DONE_DISCONNECT;
our $DONE_SENT = 0;

our $TRAILERS_CS;
our $TRAILERS_COMPLETE = 0;
our $TRAILERS_DISCONNECT;
our $TRAILERS_SENT = 0;

package main;

use constant H2_INTERNAL_ERROR_CODE => 2;   # NGHTTP2_INTERNAL_ERROR (RFC 9113 section 7)

my $lifecycle_app = async sub {
    my ($scope, $receive, $send) = @_;
    my $path = $scope->{path};

    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }

    if ($path eq '/incomplete') {
        my $cs = $scope->{'pagi.connection'};
        $Incomplete::CS = $cs;
        $cs->on_complete(sub { $Incomplete::COMPLETE = 1 });
        $cs->on_disconnect(sub { $Incomplete::DISCONNECT = $_[0] });

        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type', 'text/plain']] });
        $Incomplete::STARTED = 1;
        await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
        return;   # never sends the terminal body -- incomplete
    }
    elsif ($path eq '/throw-after-start') {
        my $cs = $scope->{'pagi.connection'};
        $Incomplete::THROW_CS = $cs;
        $cs->on_complete(sub { $Incomplete::THROW_COMPLETE = 1 });
        $cs->on_disconnect(sub { $Incomplete::THROW_DISCONNECT = $_[0] });

        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type', 'text/plain']] });
        $Incomplete::THROW_STARTED = 1;
        await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
        die "boom\n";
    }
    elsif ($path eq '/promised-trailers') {
        my $cs = $scope->{'pagi.connection'};
        $Incomplete::TRAILERS_CS = $cs;
        $cs->on_complete(sub { $Incomplete::TRAILERS_COMPLETE = 1 });
        $cs->on_disconnect(sub { $Incomplete::TRAILERS_DISCONNECT = $_[0] });

        await $send->({ type => 'http.response.start', status => 200,
                         trailers => 1,
                         headers  => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'body-only', more => 0 });
        $Incomplete::TRAILERS_SENT = 1;
        return;   # the promised trailers event never comes
    }
    elsif ($path eq '/throw-after-complete') {
        my $cs = $scope->{'pagi.connection'};
        $Incomplete::DONE_CS = $cs;
        $cs->on_complete(sub { $Incomplete::DONE_COMPLETE = 1 });
        $cs->on_disconnect(sub { $Incomplete::DONE_DISCONNECT = $_[0] });

        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'all-done', more => 0 });
        $Incomplete::DONE_SENT = 1;
        die "boom\n";
    }
    else {
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
        return;
    }
};

subtest 'h2: response started but never finished resets the stream' => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my ($conn, $stream, $client_sock, $server) = create_h2_connection(app => $lifecycle_app);

    my (%headers, %closed);
    my $client = create_client(
        on_header       => sub { my ($sid, $n, $v) = @_; $headers{$sid}{$n} = $v; return 0 },
        on_stream_close => sub { my ($sid, $error_code) = @_; $closed{$sid} = $error_code; return 0 },
    );
    complete_h2_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/incomplete',
        scheme    => 'https',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 30);

    ok($Incomplete::STARTED, 'app reached http.response.start before returning');
    is($headers{$stream_id}{':status'}, '200', 'response.start (200) was delivered before the reset');
    ok(exists $closed{$stream_id}, 'client observed the stream close');
    ok($closed{$stream_id}, 'stream closed with a nonzero error code (RST, not a clean END_STREAM)');
    is($closed{$stream_id}, H2_INTERNAL_ERROR_CODE,
        'RST error code is NGHTTP2_INTERNAL_ERROR (or its literal fallback)');

    ok($Incomplete::CS, 'app captured a connection_state');
    is($Incomplete::COMPLETE, 0, 'on_complete never fired');
    is($Incomplete::DISCONNECT, 'server_error', "on_disconnect fired with 'server_error'");

    my @incomplete_warnings = grep {
        /PAGI application returned with an incomplete response \(HTTP\/2 stream $stream_id\)/
    } @warnings;
    is(scalar(@incomplete_warnings), 1,
        'incomplete-response warning was logged exactly once (no double-warn regression)')
        or diag("warnings: @warnings");

    # A second stream on the SAME connection still serves.
    my $stream_id2 = $client->submit_request(
        method    => 'GET',
        path      => '/ok',
        scheme    => 'https',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    is($headers{$stream_id2}{':status'}, '200',
        'a second stream on the same connection still serves after the reset');

    $stream->close_now;
    $loop->remove($server);
};

subtest 'h2: app throwing after response started resets the stream' => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my ($conn, $stream, $client_sock, $server) = create_h2_connection(app => $lifecycle_app);

    my (%headers, %closed);
    my $client = create_client(
        on_header       => sub { my ($sid, $n, $v) = @_; $headers{$sid}{$n} = $v; return 0 },
        on_stream_close => sub { my ($sid, $error_code) = @_; $closed{$sid} = $error_code; return 0 },
    );
    complete_h2_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/throw-after-start',
        scheme    => 'https',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 30);

    ok($Incomplete::THROW_STARTED, 'app reached http.response.start before throwing');
    is($headers{$stream_id}{':status'}, '200', 'response.start (200) was delivered before the reset');
    ok(exists $closed{$stream_id}, 'client observed the stream close');
    is($closed{$stream_id}, H2_INTERNAL_ERROR_CODE,
        'RST error code is NGHTTP2_INTERNAL_ERROR (or its literal fallback)');

    ok($Incomplete::THROW_CS, 'app captured a connection_state');
    is($Incomplete::THROW_COMPLETE, 0, 'on_complete never fired');
    is($Incomplete::THROW_DISCONNECT, 'server_error', "on_disconnect fired with 'server_error'");

    ok((grep { /PAGI application error after response started \(HTTP\/2 stream $stream_id\): boom/ } @warnings),
        'existing post-start-error warning text was retained');

    # A second stream on the SAME connection still serves.
    my $stream_id2 = $client->submit_request(
        method    => 'GET',
        path      => '/ok',
        scheme    => 'https',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    is($headers{$stream_id2}{':status'}, '200',
        'a second stream on the same connection still serves after the reset');

    $stream->close_now;
    $loop->remove($server);
};

# =============================================================================
# /throw-after-complete: the app delivered a COMPLETE response and only then
# threw. The response is already on the wire, so there is nothing to
# synthesize or reset -- but the exception is still the application's, and it
# must be logged exactly once. The stream itself completed cleanly, so the
# clean-completion half of the lifecycle stands: on_complete fires,
# on_disconnect does not.
# =============================================================================

subtest 'h2: app throwing after a COMPLETE response still logs the error' => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my ($conn, $stream, $client_sock, $server) = create_h2_connection(app => $lifecycle_app);

    my (%headers, %body, %closed);
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{$sid}{$n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $data) = @_; $body{$sid} .= $data; return 0 },
        on_stream_close    => sub { my ($sid, $error_code) = @_; $closed{$sid} = $error_code; return 0 },
    );
    complete_h2_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/throw-after-complete',
        scheme    => 'https',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 30);

    ok($Incomplete::DONE_SENT, 'app delivered the terminal body before throwing');
    is($headers{$stream_id}{':status'}, '200', 'the complete response was delivered');
    is($body{$stream_id}, 'all-done', 'the full body was delivered');
    is($closed{$stream_id}, 0, 'stream closed cleanly (END_STREAM, no RST)');

    my @errors = grep {
        /PAGI application error after response started \(HTTP\/2 stream $stream_id\): boom/
    } @warnings;
    is(scalar(@errors), 1,
        'the post-completion exception is logged exactly once')
        or diag("warnings: @warnings");

    ok($Incomplete::DONE_CS, 'app captured a connection_state');
    is($Incomplete::DONE_COMPLETE, 1, 'on_complete fired (the response did complete)');
    is($Incomplete::DONE_DISCONNECT, undef, 'on_disconnect did NOT fire');

    $stream->close_now;
    $loop->remove($server);
};

# =============================================================================
# /promised-trailers: trailers declared, terminal body sent, trailers event
# never sent (design section 15.3's promised-but-unsent trailers row).
#
# Phase 2b Task 4 changed the wire behavior this subtest pins: design §8.3
# forbids a terminal body event from ending the HTTP/2 stream once trailers
# are declared, so a single-shot terminal body with 'trailers' declared now
# routes through the streaming path and reserves END_STREAM for the
# trailing HEADERS block (same as a chunked body). Since the app never
# sends that trailing HEADERS block, the stream never gets a clean
# END_STREAM at all -- the response sequence is stuck at 'awaiting_trailers'
# when the app returns, so the dispatch wrapper's incomplete-response arm
# resets the stream with NGHTTP2_INTERNAL_ERROR, exactly as it would for a
# chunked body that never sent its promised trailers. (Before Task 4, a
# single-shot terminal body always carried END_STREAM regardless of
# declared trailers, so this same scenario closed "cleanly" with error code
# 0 -- an early END_STREAM the server originated. That bug is what Task 4
# fixes; this subtest now pins the corrected behavior.)
#
# The attribution below is unchanged either way: the client did nothing
# wrong and must not be blamed for it, so the reason is 'server_error'
# (deviation D3), not 'client_closed' -- the dispatch wrapper's
# incomplete-response arm marks this BEFORE issuing the RST (see
# Connection.pm's _h2_dispatch_stream), so the client-gone carve-out must
# not swallow this case: the spec's log carve-out exists only for "the
# client had already disconnected" -- a server_error reason means the
# SERVER caused the abnormal end, so the incomplete-response warning must
# still fire.
# =============================================================================

subtest 'h2: promised-but-unsent trailers report server_error, not client_closed' => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my ($conn, $stream, $client_sock, $server) = create_h2_connection(app => $lifecycle_app);

    my (%headers, %body, %closed);
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{$sid}{$n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $data) = @_; $body{$sid} .= $data; return 0 },
        on_stream_close    => sub { my ($sid, $error_code) = @_; $closed{$sid} = $error_code; return 0 },
    );
    complete_h2_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/promised-trailers',
        scheme    => 'https',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 30);

    ok($Incomplete::TRAILERS_SENT, 'app sent the terminal body and returned');

    # The body still reaches the client byte-exact; only the stream's
    # closing frame differs post-Task-4 (see the header comment above).
    is($headers{$stream_id}{':status'}, '200', 'client still receives status 200');
    is($body{$stream_id}, 'body-only', 'client still receives the full body');
    is($closed{$stream_id}, H2_INTERNAL_ERROR_CODE,
        'stream now resets with NGHTTP2_INTERNAL_ERROR instead of a false-clean END_STREAM (Task 4)');

    ok($Incomplete::TRAILERS_CS, 'app captured a connection_state');
    is($Incomplete::TRAILERS_CS->disconnect_reason, 'server_error',
        "the server-originated early END_STREAM is attributed to the server");
    is($Incomplete::TRAILERS_DISCONNECT, 'server_error',
        "on_disconnect fired with 'server_error'");
    is($Incomplete::TRAILERS_COMPLETE, 0, 'on_complete never fired');

    my @incomplete_warnings = grep {
        /PAGI application returned with an incomplete response \(HTTP\/2 stream $stream_id\)/
    } @warnings;
    is(scalar(@incomplete_warnings), 1,
        'server-caused early END_STREAM still logs the incomplete-response warning')
        or diag("warnings: @warnings");

    $stream->close_now;
    $loop->remove($server);
};

done_testing;
