use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

# ============================================================
# The HTTP/2 twin of t/88-app-failure-details.t: an application that dies is
# reported with server_error and the error's first line, and the scope's
# connection object settles. HTTP/2 already did this; these pin it, so the
# two transports cannot drift apart again.
# ============================================================

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;



# ============================================================
# Helpers -- shape borrowed from t/http2/38-ws-queue-overflow.t
# ============================================================

sub create_h2_connection {
    my (%o) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app    = $o{app} // sub { };
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, http2 => 1,
        ($o{logger} ? (logger => $o{logger}) : ()),
    );
    $loop->add($server);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;
    return ($conn, $stream, $sock_b, $server);
}

sub create_client {
    my (%o) = @_;
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => $o{on_header}          // sub { 0 },
        on_frame_recv      => $o{on_frame_recv}      // sub { 0 },
        on_data_chunk_recv => $o{on_data_chunk_recv} // sub { 0 },
        on_stream_close    => $o{on_stream_close}    // sub { 0 },
    });
}

sub complete_h2_handshake {
    my ($client, $client_sock) = @_;
    $loop->loop_once(0.1);
    my $settings = '';
    $client_sock->sysread($settings, 4096);
    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($settings);
    $loop->loop_once(0.1);
    my $ack = '';
    $client_sock->sysread($ack, 4096);
    $client->mem_recv($ack) if length($ack);
    my $out = $client->mem_send;
    $client_sock->syswrite($out) if length($out);
    $loop->loop_once(0.1);
    my $extra = '';
    $client_sock->sysread($extra, 4096);
    $client->mem_recv($extra) if length($extra);
}

sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    for (1 .. ($rounds // 20)) {
        $loop->loop_once(0.05);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

sub submit_ws {
    my ($client, $path) = @_;
    return $client->submit_request(method => 'CONNECT', path => $path,
        scheme => 'https', authority => 'localhost',
        headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
        body => sub { return undef });   # keep the request body open: this is a tunnel
}

sub send_ws_text {
    my ($client, $client_sock, $stream_id, $text) = @_;
    my $frame = Protocol::WebSocket::Frame->new(
        type => 'text', buffer => $text, masked => 1);
    $client->submit_data($stream_id, $frame->to_bytes, 0);
    my $out = $client->mem_send;
    $client_sock->syswrite($out) if length($out);
}

# Every websocket frame the server wrote on this stream, in order.
sub ws_frames {
    my ($raw) = @_;
    my @frames;
    my $parser = Protocol::WebSocket::Frame->new;
    $parser->append($raw);
    while (defined(my $bytes = $parser->next_bytes)) {
        push @frames, { opcode => $parser->opcode, bytes => $bytes };
    }
    return @frames;
}

sub errors_in { return [map { $_->{message} } grep { ($_->{level} // '') eq 'error' } @{$_[0]}] }

# Runs one stream: $submit opens it on the client, $after runs once it is
# open. Returns what the app's connection object reported, the error log
# lines, and the response status.
sub run_h2 {
    my (%o) = @_;
    my (%r, @log, $status);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return if $scope->{type} eq 'lifespan';
        my $cs = $r{cs} = $scope->{'pagi.connection'};
        $cs->on_disconnect(sub { $r{on_disconnect}++ });
        $cs->on_complete(sub { $r{on_complete}++ });
        $r{df} = $cs->disconnect_future;
        await $o{app}->($scope, $receive, $send);
    };
    my ($conn, $io, $sock) = create_h2_connection(app => $app, logger => sub { push @log, $_[0] });
    my $client = create_client(on_header => sub { my ($sid, $n, $v) = @_; $status = $v if $n eq ':status'; 0 });
    complete_h2_handshake($client, $sock);
    my $sid = $o{submit}->($client);
    $sock->syswrite($client->mem_send);
    exchange_frames($client, $sock, 25);
    if ($o{after}) { $o{after}->($client, $sock, $sid); exchange_frames($client, $sock, 25) }
    return { %r, log => errors_in(\@log), status => $status };
}

sub settled_ok {
    my ($r, $reason, $detail, $label) = @_;
    is($r->{cs}->disconnect_reason, $reason, "$label: reason");
    is($r->{cs}->disconnect_detail, $detail, "$label: detail");
    ok(!$r->{cs}->is_connected, "$label: the connection object is not connected");
    is($r->{on_disconnect} // 0, 1, "$label: on_disconnect fired once");
    ok(!$r->{on_complete}, "$label: on_complete did not fire");
    ok($r->{df}->is_ready, "$label: disconnect_future resolved");
}

my $ws   = sub { submit_ws($_[0], '/ws') };
my $sse  = sub { $_[0]->submit_request(method => 'GET', path => '/events', scheme => 'https',
                    authority => 'localhost', headers => [['accept', 'text/event-stream']]) };
my $http = sub { $_[0]->submit_request(method => 'GET', path => '/', scheme => 'https', authority => 'localhost') };
my $dies = sub { die "the handler broke\nsecond line\n" };

subtest 'W1: a WebSocket app that dies after accept' => sub {
    my $r = run_h2(submit => $ws,
        after => sub { my ($client, $sock, $sid) = @_; send_ws_text($client, $sock, $sid, 'hello') },
        app => async sub {
            my ($scope, $receive, $send) = @_;
            await $receive->();
            await $send->({ type => 'websocket.accept' });
            await $receive->();
            $dies->();
        });
    settled_ok($r, 'server_error', 'the handler broke', 'W1');
};

subtest 'W2: a WebSocket app that dies before accepting' => sub {
    my $r = run_h2(submit => $ws, app => async sub { my ($s, $receive) = @_; await $receive->(); $dies->() });
    settled_ok($r, 'server_error', 'the handler broke', 'W2');
    is($r->{status}, 500, 'W2: a 500');
};

subtest 'W3: an SSE app that dies before sse.start' => sub {
    my $r = run_h2(submit => $sse, app => async sub { $dies->() });
    settled_ok($r, 'server_error', 'the handler broke', 'W3');
    is($r->{status}, 500, 'W3: a 500');
};

subtest 'an exception object: its first line, without the location' => sub {
    package Local::Boom { use overload '""' => sub { "boom at x.pl line 3.\n" }, fallback => 1 }
    my $r = run_h2(submit => $ws, app => async sub { my ($s, $receive) = @_; await $receive->(); die bless {}, 'Local::Boom' });
    is($r->{cs}->disconnect_detail, 'boom', 'the detail');
};

subtest 'plain HTTP: an app that dies before http.response.start' => sub {
    my $r = run_h2(submit => $http, app => async sub { $dies->() });
    settled_ok($r, 'server_error', 'the handler broke', 'HTTP');
    is($r->{status}, 500, 'a 500');
};

done_testing;
