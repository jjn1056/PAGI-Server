#!/usr/bin/env perl

# The HTTP/2 twin of t/receive-after-cancel.t, over a real nghttp2 client
# session on a socketpair: the application races a receive against a timer,
# which cancels the receive, then receives again. Cancelling a pending receive
# must not affect the stream (PAGI::Spec::Www): the later receive gets the
# client's next event -- the request body, a WebSocket message, an SSE
# request body, or the client leaving an SSE stream.

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use Protocol::WebSocket::Frame;
use Socket qw(AF_UNIX SOCK_STREAM);
use Time::HiRes qw(time);
use FindBin;
use lib "$FindBin::Bin/../../lib";
use lib "$FindBin::Bin/../lib";

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

# Nothing may warn. Future::XS before 0.16 warns "lost a sequence Future" here
# (RT #181389), so the file skips under it.
my @warnings;
$SIG{__WARN__} = sub { push @warnings, @_ };

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use PAGITest::FutureXS qw(old_future_xs);

plan skip_all => 'needs Future::XS 0.16 or later (RT #181389)' if old_future_xs();

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;
my (@LOG, @sse_log);

# Races $receive against a 0.1s timer and says whether the receive lost. The
# cancelled receive is dropped on return, before the client's next event
# arrives, as an application would.
async sub race_and_lose {
    my ($receive) = @_;
    my $first = $receive->();
    await Future->wait_any($first, $loop->delay_future(after => 0.1));
    return $first->is_cancelled ? 'cancelled' : 'answered';
}

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    my $type = $scope->{type};
    if ($type eq 'websocket') {
        await $receive->();                              # websocket.connect
        await $send->({ type => 'websocket.accept' });
        my $outcome = await race_and_lose($receive);
        my $event = await $receive->();
        my $got = $event->{type} eq 'websocket.receive' ? $event->{text} : $event->{type};
        await $send->({ type => 'websocket.send', text => "first receive $outcome; got '$got'" });
        await $send->({ type => 'websocket.close' });
        await $receive->();                              # the disconnect
        return;
    }
    if ($type eq 'sse' && $scope->{path} eq '/events-body') {
        # The request body has not arrived yet, so the first receive parks.
        await $send->({ type => 'sse.start', status => 200, headers => [] });
        my $outcome = await race_and_lose($receive);
        my $event = await $receive->();
        push @sse_log, "first receive $outcome; got $event->{type} body '" . ($event->{body} // '') . "'";
        await $send->({ type => 'sse.close' });
        return;
    }
    if ($type eq 'sse') {
        await $send->({ type => 'sse.start', status => 200, headers => [] });
        await $receive->();                              # sse.request, answered at once
        push @sse_log, 'first receive ' . await race_and_lose($receive);
        my $event = await $receive->();                  # parks until the client leaves
        push @sse_log, "second receive: $event->{type}";
        return;
    }
    return unless $type eq 'http';

    my $outcome = await race_and_lose($receive);
    my $body = '';
    while (1) {
        my $event = await $receive->();
        last unless $event->{type} eq 'http.request';
        $body .= $event->{body} // '';
        last unless $event->{more};
    }
    my $out = "first receive $outcome; body '$body'";
    await $send->({ type => 'http.response.start', status => 200,
        headers => [['content-length', length $out]] });
    await $send->({ type => 'http.response.body', body => $out, more => 0 });
};

socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
$sock_a->blocking(0);
$sock_b->blocking(0);
my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, http2 => 1,
    log_level => 'debug', access_log => undef,
    logger => sub { push @LOG, "[$_[0]{level}] $_[0]{message}" },
);
$loop->add($server);
my $stream = IO::Async::Stream->new(
    read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
my $conn = PAGI::Server::Connection->new(
    stream => $stream, app => $app, protocol => $protocol, server => $server,
    max_disconnect_receives => $server->{max_disconnect_receives},
    h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
);
$server->add_child($stream);
$conn->start;

require Net::HTTP2::nghttp2::Session;
my (%headers, %body, %closed);
my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
    on_begin_headers   => sub { 0 },
    on_header          => sub { my ($sid, $name, $value) = @_; $headers{$sid}{$name} = $value; 0 },
    on_frame_recv      => sub { 0 },
    on_data_chunk_recv => sub { my ($sid, $data) = @_; $body{$sid} .= $data; 0 },
    on_stream_close    => sub { my ($sid) = @_; $closed{$sid} = 1; 0 },
});

sub flush_client {
    my $out = $client->mem_send;
    $sock_b->syswrite($out) if length($out);
}

# Runs the loop and the client for up to $seconds, or until $done->() is true.
sub exchange {
    my ($seconds, $done) = @_;
    my $deadline = time + $seconds;
    while (time < $deadline) {
        $loop->loop_once(0.05);
        my $buf = '';
        $sock_b->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        flush_client();
        last if $done && $done->();
    }
}

# handshake
$loop->loop_once(0.1);
my $settings = '';
$sock_b->sysread($settings, 4096);
$client->send_connection_preface;
$sock_b->syswrite($client->mem_send);
$loop->loop_once(0.1);
$client->mem_recv($settings);
exchange(0.3);

subtest 'a receive after a cancelled one gets the body' => sub {
    my $sid = $client->submit_request(
        method => 'POST', path => '/echo', scheme => 'http', authority => 'localhost',
        headers => [['content-length', '4']],
        body => sub { return undef },            # deferred: no DATA yet
    );
    exchange(0.4);                               # the app's timer fires meanwhile
    ok(eval { $client->submit_data($sid, "body", 1); 1 }, 'the DATA frame is queued')   # with END_STREAM
        or diag "submit_data: $@";
    flush_client();
    exchange(3, sub { $closed{$sid} });
    is($headers{$sid}{':status'}, '200', 'the request succeeds');
    is($body{$sid}, "first receive cancelled; body 'body'", 'the later receive got the body');
};

subtest 'WebSocket: a receive after a cancelled one gets the next message' => sub {
    my $sid = $client->submit_request(
        method => 'CONNECT', path => '/ws', scheme => 'https', authority => 'localhost',
        headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
        body => sub { return undef },
    );
    exchange(0.4);                               # accepted; the app's timer fires meanwhile
    is($headers{$sid}{':status'}, '200', 'the WebSocket is accepted');
    $client->submit_data($sid,
        Protocol::WebSocket::Frame->new(buffer => 'hello', type => 'text', masked => 1)->to_bytes, 0);
    flush_client();
    my $reply = '';
    exchange(3, sub {
        my $frames = Protocol::WebSocket::Frame->new;
        $frames->append(my $copy = $body{$sid} // '');
        $reply = '';
        while (defined(my $message = $frames->next)) { $reply .= $message if $frames->is_text }
        $reply =~ /'\z/;
    });
    is($reply, "first receive cancelled; got 'hello'", 'the later receive got the message');
};

subtest 'SSE with a request body: a receive after a cancelled one gets the body' => sub {
    @sse_log = ();
    my $sid = $client->submit_request(
        method => 'POST', path => '/events-body', scheme => 'http', authority => 'localhost',
        headers => [['accept', 'text/event-stream'], ['content-length', '4']],
        body => sub { return undef },
    );
    exchange(0.4);
    $client->submit_data($sid, "body", 1);
    flush_client();
    exchange(3, sub { $closed{$sid} });
    is($headers{$sid}{':status'}, '200', 'the stream starts');
    is(\@sse_log, ["first receive cancelled; got sse.request body 'body'"], 'the later receive got the body');
};

subtest 'SSE: a receive after a cancelled one gets the disconnect' => sub {
    @sse_log = ();
    my $sid = $client->submit_request(
        method => 'GET', path => '/events', scheme => 'http', authority => 'localhost',
        headers => [['accept', 'text/event-stream']],
    );
    exchange(0.4);
    is($headers{$sid}{':status'}, '200', 'the stream starts');
    $client->submit_rst_stream($sid, 8);         # CANCEL: the client leaves
    flush_client();
    exchange(2, sub { @sse_log == 2 });
    is(\@sse_log, ['first receive cancelled', 'second receive: sse.disconnect'],
        'the later receive got the disconnect');
};

is([grep { /^\[error\]/ } @LOG], [], 'no application error was logged');

eval { $server->shutdown->get };
eval { $loop->remove($server) };
is(\@warnings, [], 'no warnings');
done_testing;
