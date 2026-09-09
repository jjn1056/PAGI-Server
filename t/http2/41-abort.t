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

# ============================================================
# Test: abort() over HTTP/2 resets one stream and nothing else
# ============================================================
# PAGI::Spec::Www, "Connection Object Interface": abort "Requests that the
# server end this scope's transport now: ... reset only this stream
# (RST_STREAM, e.g. CANCEL) on HTTP/2". Every row therefore runs two streams
# on one connection: the one that aborts, and a sibling that must be
# untouched by it -- still connected with no disconnect reason at the moment
# of the reset, and still able to finish its own response afterwards.
#
# "The server MUST NOT log an incomplete-response or no-response error for an
# aborted scope", so every row also asserts that the dispatch wrapper's
# incomplete-response arms did not fire: zero error lines.
#
# CANCEL is error code 8 (RFC 9113 section 7). A stream that ends with
# END_STREAM closes with code 0, which is what the sibling asserts in each
# row -- so "reset, not ended" and "ended, not reset" are distinguished by
# the same observation on the same connection.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers -- shape borrowed from t/http2/22-denial-response.t
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
        on_frame_recv      => sub { 0 },
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

sub submit_http {
    my ($client, $path) = @_;
    return $client->submit_request(method => 'GET', path => $path,
        scheme => 'http', authority => 'localhost', headers => []);
}
sub submit_ws {
    my ($client, $path) = @_;
    return $client->submit_request(method => 'CONNECT', path => $path,
        scheme => 'https', authority => 'localhost',
        headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
        body => sub { return undef });   # keep the request body open: this is a tunnel
}
sub submit_sse {
    my ($client, $path) = @_;
    return $client->submit_request(method => 'GET', path => $path,
        scheme => 'http', authority => 'localhost',
        headers => [['accept', 'text/event-stream']]);
}

sub errors_in { return [map { $_->{message} } grep { ($_->{level} // '') eq 'error' } @{$_[0]}] }

# ============================================================
# 1. an http stream, mid-body
# ============================================================

subtest 'http stream: abort resets only its own stream, with CANCEL' => sub {
    my (%r, @log);
    my $abort_gate = $loop->new_future;
    my $sib_gate   = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $c     = $scope->{'pagi.connection'};
        my $which = $scope->{path} eq '/abort' ? 'abort' : 'sib';
        $r{"${which}_conn"} = $c;
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => "$which-1", more => 1 });
        $r{"${which}_started"} = 1;
        if ($which eq 'abort') {
            await $abort_gate;
            $c->abort('cut');
            $r{abort_reason} = $c->disconnect_reason;
            $r{abort_detail} = $c->disconnect_detail;
            $r{abort_done}   = 1;
            return;                       # no terminal body: the scope is already over
        }
        await $sib_gate;
        await $send->({ type => 'http.response.body', body => 'sib-2', more => 0 });
        $r{sib_done} = 1;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server)
        = create_h2_connection(app => $app, logger => sub { push @log, $_[0] });
    my (%data, %closed);
    my $client = create_client(
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $data{$sid} .= $d; 0 },
        on_stream_close    => sub { my ($sid, $c) = @_; $closed{$sid} = $c; 0 },
    );
    complete_h2_handshake($client, $client_sock);
    my $asid = submit_http($client, '/abort');
    my $ssid = submit_http($client, '/sibling');
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 25);

    ok($r{abort_started} && $r{sib_started}, 'both streams started their responses');
    $abort_gate->done;
    exchange_frames($client, $client_sock, 20);

    ok($r{abort_done}, 'the aborting application returned');
    is($closed{$asid}, 8, 'the aborted stream was reset with CANCEL');
    is($r{abort_reason}, 'app_abort', 'its object reports app_abort');
    is($r{abort_detail}, 'cut', 'with the application detail');
    is($data{$asid}, 'abort-1', 'only the chunk it had already sent reached the client');
    is($closed{$ssid}, undef, 'the sibling stream was not closed by the reset');
    is($r{sib_conn}->is_connected, 1, 'the sibling scope is still connected');
    is($r{sib_conn}->disconnect_reason, undef, 'and carries no disconnect reason');

    $sib_gate->done;
    exchange_frames($client, $client_sock, 20);
    ok($r{sib_done}, 'the sibling ran to completion after the reset');
    is($data{$ssid}, 'sib-1sib-2', 'and delivered its whole body');
    is($closed{$ssid}, 0, 'the sibling stream ended with END_STREAM, not a reset');
    is($r{sib_conn}->response_complete, 1, 'the sibling scope ended cleanly');
    is(errors_in(\@log), [], 'no error line was logged for either stream');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# 2. a websocket stream, after accept
# ============================================================

subtest 'websocket stream: abort resets with CANCEL and sends no Close frame' => sub {
    my (%r, @log);
    my $abort_gate = $loop->new_future;
    my $sib_gate   = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $c     = $scope->{'pagi.connection'};
        my $which = $scope->{path} eq '/abort' ? 'abort' : 'sib';
        $r{"${which}_conn"} = $c;
        await $receive->();                          # websocket.connect
        await $send->({ type => 'websocket.accept' });
        await $send->({ type => 'websocket.send', text => "$which-live" });
        $r{"${which}_started"} = 1;
        if ($which eq 'abort') {
            await $abort_gate;
            my $parked = $receive->();               # parked before the abort
            $c->abort('cut');
            $r{abort_event}  = await $parked;
            $r{abort_reason} = $c->disconnect_reason;
            $r{abort_detail} = $c->disconnect_detail;
            $r{abort_done}   = 1;
            return;
        }
        await $sib_gate;
        await $send->({ type => 'websocket.close', code => 1000 });
        $r{sib_done} = 1;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server)
        = create_h2_connection(app => $app, logger => sub { push @log, $_[0] });
    my (%data, %closed);
    my $client = create_client(
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $data{$sid} .= $d; 0 },
        on_stream_close    => sub { my ($sid, $c) = @_; $closed{$sid} = $c; 0 },
    );
    complete_h2_handshake($client, $client_sock);
    my $asid = submit_ws($client, '/abort');
    my $ssid = submit_ws($client, '/sibling');
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 25);

    ok($r{abort_started} && $r{sib_started}, 'both sockets were accepted and carried a frame');
    $abort_gate->done;
    exchange_frames($client, $client_sock, 20);

    ok($r{abort_done}, 'the aborting application returned');
    is($closed{$asid}, 8, 'the aborted stream was reset with CANCEL');
    is($r{abort_reason}, 'app_abort', 'its object reports app_abort');
    is($r{abort_detail}, 'cut', 'with the application detail');
    is($r{abort_event}{type}, 'websocket.disconnect', 'the parked receive got websocket.disconnect');
    is($r{abort_event}{reason}, 'app_abort', 'the event reason agrees with the object');
    # The only bytes this stream carried are an ASCII text frame, so a 0x88
    # octet in its DATA could only be a Close frame opcode.
    unlike($data{$asid}, qr/\x88/, 'no Close frame was sent on the aborted socket');
    is($r{sib_conn}->is_connected, 1, 'the sibling scope is still connected');
    is($r{sib_conn}->disconnect_reason, undef, 'and carries no disconnect reason');

    $sib_gate->done;
    exchange_frames($client, $client_sock, 20);
    ok($r{sib_done}, 'the sibling ran to completion after the reset');
    like($data{$ssid}, qr/\x88/, 'the sibling did send a Close frame');
    # nghttp2 reports on_stream_close only once both sides have ended, and this
    # client deliberately keeps its CONNECT request body open -- that is what
    # makes the stream a tunnel -- so a clean server-side END_STREAM leaves the
    # code undefined here rather than 0. (Same limitation as t/71 case (h)'s h2
    # half.) The contrast that matters survives: the aborted stream carries a
    # nonzero error code, this one carries none at all, and the scope's own
    # clean end is asserted directly on the next line.
    is($closed{$ssid}, undef, 'and its stream was never reset');
    is($r{sib_conn}->response_complete, 1, 'the sibling scope ended cleanly');
    is(errors_in(\@log), [], 'no error line was logged for either stream');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# 3. an sse stream, after sse.start
# ============================================================

subtest 'sse stream: abort resets with CANCEL instead of ending the stream' => sub {
    my (%r, @log);
    my $abort_gate = $loop->new_future;
    my $sib_gate   = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $c     = $scope->{'pagi.connection'};
        my $which = $scope->{path} eq '/abort' ? 'abort' : 'sib';
        $r{"${which}_conn"} = $c;
        await $receive->();                          # sse.request
        await $send->({ type => 'sse.start', status => 200,
                        headers => [['content-type', 'text/event-stream']] });
        await $send->({ type => 'sse.send', data => "$which-1" });
        $r{"${which}_started"} = 1;
        if ($which eq 'abort') {
            await $abort_gate;
            my $parked = $receive->();               # parked before the abort
            $c->abort('cut');
            $r{abort_event}  = await $parked;
            $r{abort_reason} = $c->disconnect_reason;
            $r{abort_detail} = $c->disconnect_detail;
            $r{abort_done}   = 1;
            return;
        }
        await $sib_gate;
        await $send->({ type => 'sse.send', data => 'sib-2' });
        await $send->({ type => 'sse.close' });
        $r{sib_done} = 1;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server)
        = create_h2_connection(app => $app, logger => sub { push @log, $_[0] });
    my (%data, %closed);
    my $client = create_client(
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $data{$sid} .= $d; 0 },
        on_stream_close    => sub { my ($sid, $c) = @_; $closed{$sid} = $c; 0 },
    );
    complete_h2_handshake($client, $client_sock);
    my $asid = submit_sse($client, '/abort');
    my $ssid = submit_sse($client, '/sibling');
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 25);

    ok($r{abort_started} && $r{sib_started}, 'both streams started and sent an event');
    $abort_gate->done;
    exchange_frames($client, $client_sock, 20);

    ok($r{abort_done}, 'the aborting application returned');
    is($closed{$asid}, 8,
        'the aborted stream was reset with CANCEL: it did not end with END_STREAM');
    is($r{abort_reason}, 'app_abort', 'its object reports app_abort');
    is($r{abort_detail}, 'cut', 'with the application detail');
    is($r{abort_event}{type}, 'sse.disconnect', 'the parked receive got sse.disconnect');
    is($r{abort_event}{reason}, 'app_abort', 'the event reason agrees with the object');
    like($data{$asid}, qr/data: abort-1/, 'only the event it had already sent reached the client');
    is($r{sib_conn}->is_connected, 1, 'the sibling scope is still connected');
    is($r{sib_conn}->disconnect_reason, undef, 'and carries no disconnect reason');

    $sib_gate->done;
    exchange_frames($client, $client_sock, 20);
    ok($r{sib_done}, 'the sibling ran to completion after the reset');
    like($data{$ssid}, qr/data: sib-2/, 'and delivered its later event');
    is($closed{$ssid}, 0, 'the sibling stream ended with END_STREAM, not a reset');
    is($r{sib_conn}->response_complete, 1, 'the sibling scope ended cleanly');
    is(errors_in(\@log), [], 'no error line was logged for either stream');

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
