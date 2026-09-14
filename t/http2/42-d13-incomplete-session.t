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
# Test: HTTP/2 D13 -- an accepted WebSocket the application walked away from
# ============================================================
# PAGI::Spec::Www, "Application Left a Response Incomplete": returning from an
# accepted socket without sending websocket.close and without having received
# websocket.disconnect is an incomplete response, and the spec names one wire
# form for both transports -- "On an accepted WebSocket the server sends a
# Close frame with code 1011 ... and then closes the transport (the stream, on
# HTTP/2)". It also requires the object to report server_error, on_complete not
# to fire, and the scope's disconnect event to carry "the same token".
#
# t/69 pins all of that for HTTP/1.1. This is its HTTP/2 twin: the wire form
# (a Close frame with code 1011 in a DATA frame, then the stream ending), the
# object's own report, the event a parked receive() gets, and the single error
# log line.

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

use constant H2_DATA_FRAME  => 0;      # NGHTTP2_DATA (RFC 9113 section 6.1)
use constant H2_RST_STREAM  => 3;      # NGHTTP2_RST_STREAM (RFC 9113 section 6.4)
use constant H2_END_STREAM  => 0x1;    # END_STREAM flag

# ============================================================
# 1. the wire and the object
# ============================================================
# The application accepts, echoes one frame so the session is unambiguously
# live, and returns without websocket.close and without ever receiving
# websocket.disconnect.

subtest 'h2: an accepted socket abandoned by the app is closed with 1011 and reported server_error' => sub {
    my (%r, @log);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $c = $scope->{'pagi.connection'};
        $r{cs} = $c;
        $c->on_complete(sub { $r{complete}++ });
        $c->on_disconnect(sub { push @{$r{disc}}, [@_] });
        await $receive->();                                  # websocket.connect
        await $send->({ type => 'websocket.accept' });
        my $msg = await $receive->();                        # the client's one frame
        $r{echoed} = $msg->{text};
        await $send->({ type => 'websocket.send', text => "echo: $msg->{text}" });
        $r{returned} = 1;
        return;   # no websocket.close, no websocket.disconnect received (D13)
    };

    my ($conn, $stream_io, $client_sock, $server)
        = create_h2_connection(app => $app, logger => sub { push @log, $_[0] });

    my ($wsid, %data, @data_frames, @rst_frames);
    my $client = create_client(
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $data{$sid} .= $d; 0 },
        on_frame_recv      => sub {
            my ($f) = @_;
            return 0 unless defined $wsid && $f->{stream_id} == $wsid;
            push @data_frames, { flags => $f->{flags}, length => $f->{length} }
                if $f->{type} == H2_DATA_FRAME;
            push @rst_frames, { flags => $f->{flags} }
                if $f->{type} == H2_RST_STREAM;
            return 0;
        },
    );
    complete_h2_handshake($client, $client_sock);
    $wsid = submit_ws($client, '/ws');
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 25);

    send_ws_text($client, $client_sock, $wsid, 'hello');
    exchange_frames($client, $client_sock, 25);

    is($r{echoed}, 'hello', 'the session was live: the app received the client frame');
    ok($r{returned}, 'the app returned without a closing handshake');

    my @frames = ws_frames($data{$wsid} // '');
    my @close  = grep { $_->{opcode} == 8 } @frames;
    is(scalar(@frames), 2, 'the server wrote exactly two frames: the echo and a Close');
    is(scalar(@close), 1, 'exactly one Close frame reached the client');
    is(unpack('n', substr($close[0]{bytes}, 0, 2)), 1011,
        'the Close frame carries code 1011, not a bare RST_STREAM')
        if @close;
    ok(scalar(@data_frames), 'the client saw DATA frames on the websocket stream');
    ok($data_frames[-1]{flags} & H2_END_STREAM,
        'the last DATA frame carries END_STREAM: the stream ended after the Close frame')
        if @data_frames;
    is(scalar(grep { $_->{flags} & H2_END_STREAM } @data_frames), 1,
        'exactly one DATA frame ended the stream');
    # An accepted socket is closed by the WebSocket handshake and an END_STREAM
    # from each side; RFC 8441 section 5 reserves RST_STREAM for the exception
    # path, and RFC 9113 section 8.5 expects the peer's own END_STREAM in reply.
    is(scalar(@rst_frames), 0,
        'and no RST_STREAM followed it: the Close frame and END_STREAM are the whole ending');

    ok($r{cs}, 'the app captured its connection object');
    is($r{cs}->is_connected, 0, 'is_connected is false');
    is($r{cs}->response_complete, 0, 'response_complete is false: this was not a clean end');
    is($r{cs}->disconnect_reason, 'server_error', 'disconnect_reason is server_error');
    is($r{cs}->disconnect_detail, 'started with WebSocket accept but never ended cleanly',
        'disconnect_detail names the condition');
    is(scalar(@{$r{disc} // []}), 1, 'on_disconnect fired exactly once');
    is($r{disc}[0][0], 'server_error', 'with reason server_error') if $r{disc};
    is($r{complete}, undef, 'on_complete never fired');

    is(errors_in(\@log), ['PAGI application returned after WebSocket accept without ending the scope cleanly (HTTP/2 stream ' . $wsid . ')'],
        'exactly one error log line, naming the condition');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# 2. the event a parked receive() gets
# ============================================================
# Www.pod "Meaning per scope", Agreement with disconnect events: the event's
# reason and the object's disconnect_reason MUST be the same token; and
# "Application Left a Response Incomplete" names the event on an accepted
# socket as websocket.disconnect with code 1011 and reason server_error.
#
# The application parks a receive() it never awaits and returns, which is the
# only way to hold a receive open across a D13 ending.

subtest 'h2: the parked receive gets websocket.disconnect 1011 / server_error' => sub {
    my (%r, @log);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $c = $scope->{'pagi.connection'};
        $r{cs} = $c;
        await $receive->();                                  # websocket.connect
        await $send->({ type => 'websocket.accept' });
        # Parked, deliberately not awaited: the app returns while it is still
        # outstanding, which is exactly the D13 shape. The Future is kept in
        # %r because nothing else holds it once the app returns.
        $r{parked} = $receive->();
        $r{parked}->on_done(sub { $r{event} = $_[0] });
        $r{returned} = 1;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server)
        = create_h2_connection(app => $app, logger => sub { push @log, $_[0] });

    my ($wsid, %data);
    my $client = create_client(
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $data{$sid} .= $d; 0 },
    );
    complete_h2_handshake($client, $client_sock);
    $wsid = submit_ws($client, '/ws');
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 25);

    ok($r{returned}, 'the app returned with a receive still parked');
    ok($r{event}, 'the parked receive resolved');
    is($r{event}{type}, 'websocket.disconnect', 'it resolved with websocket.disconnect')
        if $r{event};
    is($r{event}{code}, 1011, 'the event carries code 1011, matching the Close frame')
        if $r{event};
    is($r{event}{reason}, 'server_error', 'the event reason is server_error')
        if $r{event};
    is($r{event}{reason}, $r{cs}->disconnect_reason,
        'the event reason and the object disconnect_reason are the same token')
        if $r{event} && $r{cs};

    is(errors_in(\@log), ['PAGI application returned after WebSocket accept without ending the scope cleanly (HTTP/2 stream ' . $wsid . ')'],
        'exactly one error log line');

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
