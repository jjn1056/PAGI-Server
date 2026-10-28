use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use Time::HiRes qw(time);
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

# PAGI::Spec::Www 0.7 "Sends Are Sequential" over HTTP/2 (h2c): an
# application may issue a send while an earlier one still waits on its
# stream's backpressure; the server processes the stream's sends in the
# order they were called. The client here reads only when the test pumps
# it, so the server's per-stream send queue stays above its high watermark
# for as long as a case needs (curl's window would hide the backpressure).

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

my $loop = IO::Async::Loop->new;
my $HIGH = 256 * 1024;
my $LOW  = 16 * 1024;

# An in-process connection on a socketpair, with the watermarks above.
sub open_h2c {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
        http2 => 1, write_high_watermark => $HIGH, write_low_watermark => $LOW,
    );
    $loop->add($server);
    socketpair(my $server_sock, my $client_sock, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $_->blocking(0) for $server_sock, $client_sock;
    my $stream = IO::Async::Stream->new(
        read_handle => $server_sock, write_handle => $server_sock, on_read => sub { 0 },
    );
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => PAGI::Server::Protocol::HTTP1->new,
        server => $server, h2_protocol => $server->{http2_protocol},
        h2c_enabled => $server->{h2c_enabled}, extensions => $server->{extensions},
        write_high_watermark => $HIGH, write_low_watermark => $LOW,
    );
    $server->add_child($stream);
    $conn->start;
    return { server => $server, conn => $conn, stream => $stream, sock => $client_sock };
}

# A client that records the stream's body and frame events.
sub h2_client {
    my ($h) = @_;
    require Net::HTTP2::nghttp2::Session;
    $h->{body} = '';
    $h->{events} = [];
    $h->{client} = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { 0 },
        on_data_chunk_recv => sub { $h->{body} .= $_[1]; 0 },
        on_frame_recv      => sub {
            my ($frame) = @_;
            push @{ $h->{events} }, 'END_STREAM at ' . length($h->{body})
                if $frame->{stream_id} && ($frame->{flags} & 1);
            0;
        },
        on_stream_close    => sub { $h->{closed} = 1; 0 },
    });
    $h->{client}->send_connection_preface;
    return $h;
}

# One round: the server runs, then the client reads at most $slice bytes
# (none with 0) and writes what it has to say (WINDOW_UPDATEs, RST_STREAM).
sub pump {
    my ($h, $slice) = @_;
    $loop->loop_once(0.005);
    if ($slice) {
        my $n = sysread($h->{sock}, my $buf, $slice);
        $h->{client}->mem_recv($buf) if $n;
    }
    my $out = $h->{client}->mem_send;
    syswrite($h->{sock}, $out) if length $out;
}

sub pump_until {
    my ($h, $slice, $cond, $secs) = @_;
    my $deadline = time + ($secs // 20);
    pump($h, $slice) until $cond->() || time > $deadline;
    return $cond->();
}

sub request {
    my ($h, %headers) = @_;
    $h->{client}->submit_request(
        method => 'GET', path => '/', scheme => 'http', authority => 'localhost',
        headers => [ map { [$_ => $headers{$_}] } sort keys %headers ],
    );
    pump($h, 16384) for 1 .. 5;
}

sub queue_bytes {
    my ($h) = @_;
    my $ss = $h->{conn}{h2_streams}{1} or return 0;
    return $ss->{send_queue_bytes} // 0;
}

sub finish {
    my ($h) = @_;
    $h->{stream}->close_now;
    $loop->remove($h->{server});
}

subtest 'a send issued while an earlier one waits on backpressure goes out after it' => sub {
    my %seen;
    my $go = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my $first = $send->({ type => 'http.response.body', body => 'x' x 1_000_000, more => 1 });
        my $a = $send->({ type => 'http.response.body', body => "\nMARK-A\n", more => 1 });
        $seen{a} = $a;
        await $go;
        $seen{a_pending_at_b} = !$a->is_ready;
        my $b = $send->({ type => 'http.response.body', body => "\nMARK-B\n", more => 1 });
        await Future->needs_all($first, $a, $b);
        await $send->({ type => 'http.response.body', body => "\nEND\n", more => 0 });
    };
    my $h = h2_client(open_h2c($app));
    request($h);
    # B is issued once the queue has drained below the high watermark while
    # A still waits (for the low one).
    ok(pump_until($h, 4096, sub {
        $go->done if !$go->is_ready && $seen{a} && !$seen{a}->is_ready && queue_bytes($h) < $HIGH;
        $go->is_ready;
    }), 'B was issued while A waited');
    ok(pump_until($h, 65536, sub { $h->{closed} }), 'the response completed');
    ok($seen{a_pending_at_b}, 'A was still pending when B was issued');
    my ($a, $b) = (index($h->{body}, 'MARK-A'), index($h->{body}, 'MARK-B'));
    ok($a > 0 && $b > 0 && $a < $b, "A before B, the order they were called (A at $a, B at $b)");
    finish($h);
};

subtest 'sse.close behind a waiting send ends the stream after it' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'sse';
        await $send->({ type => 'sse.start' });
        my $big = $send->({ type => 'sse.send', data => 'B' x 1_000_000 });
        my $a = $send->({ type => 'sse.send', event => 'marker', data => 'AFTER-PARK-MARKER' });
        $seen{a_pending} = !$a->is_ready;
        my $close = $send->({ type => 'sse.close' });
        await Future->needs_all($big, $a, $close);
    };
    my $h = h2_client(open_h2c($app));
    request($h, accept => 'text/event-stream');
    ok(pump_until($h, 4096, sub { $h->{closed} }), 'the stream ended');
    ok($seen{a_pending}, 'the marker was still pending when sse.close was issued');
    my $marker = index($h->{body}, 'AFTER-PARK-MARKER');
    ok($marker > 0, 'the marker arrived');
    is($h->{events}, ['END_STREAM at ' . length $h->{body}], 'END_STREAM came after every byte, the marker included');
    finish($h);
};

subtest 'an SSE stream ends -- for pagi.connection too -- only after the sends before sse.close' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'sse';
        my $conn = $scope->{'pagi.connection'};
        await $send->({ type => 'sse.start' });
        my $big = $send->({ type => 'sse.send', data => 'B' x 1_000_000 });
        my $a = $send->({ type => 'sse.send', event => 'marker', data => 'MARKER' });
        my $close = $send->({ type => 'sse.close' });
        $seen{a_pending} = !$a->is_ready;
        $seen{complete_at_close} = $conn->response_complete ? 1 : 0;
        $seen{connected_at_close} = $conn->is_connected ? 1 : 0;
        await Future->needs_all($big, $a, $close);
        $seen{complete_after} = $conn->response_complete ? 1 : 0;
    };
    my $h = h2_client(open_h2c($app));
    request($h, accept => 'text/event-stream');
    ok(pump_until($h, 4096, sub { exists $seen{complete_after} }), 'every send settled');
    ok($seen{a_pending}, 'the marker was still pending when sse.close was issued');
    is($seen{complete_at_close}, 0, 'the stream was not yet complete when sse.close was called');
    is($seen{connected_at_close}, 1, 'nor was the connection reported closed');
    is($seen{complete_after}, 1, 'it was complete once the sends before it were written');
    finish($h);
};

subtest 'buffered_amount counts every send accepted and not yet written' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my @sent = map { $send->({ type => 'http.response.body', body => 'x' x 10_000, more => 1 }) } 1 .. 50;
        $seen{buffered} = $scope->{'pagi.transport'}->buffered_amount;
        push @sent, $send->({ type => 'http.response.body', body => '', more => 0 });
        await Future->needs_all(@sent);
    };
    my $h = h2_client(open_h2c($app));
    request($h);
    ok(pump_until($h, 65536, sub { $h->{closed} }), 'the response completed');
    is(length $h->{body}, 500_000, 'every byte arrived');
    # A stream's buffered_amount is its own queue; what nghttp2 has already
    # framed onto the shared connection -- at most the client's initial
    # flow-control window, 65,535 bytes -- has left it.
    ok($seen{buffered} >= 50 * 10_000 - 65_535,
        "right after 50 unawaited 10 KB sends, buffered_amount is $seen{buffered}");
    finish($h);
};

subtest 'a cancelled send does not hold up the sends behind it' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my $big = $send->({ type => 'http.response.body', body => '1' x 1_000_000, more => 1 });
        my $a = $send->({ type => 'http.response.body', body => "\nMARK-A\n", more => 1 });
        $seen{a_pending} = !$a->is_ready;
        $a->cancel;
        my $b = $send->({ type => 'http.response.body', body => "\nMARK-B\n", more => 1 });
        await Future->needs_all($big, $b);
        $seen{b_done} = $b->is_done;
        await $send->({ type => 'http.response.body', body => "\nEND\n", more => 0 });
    };
    my $h = h2_client(open_h2c($app));
    request($h);
    ok(pump_until($h, 65536, sub { $h->{closed} }), 'the response completed');
    ok($seen{a_pending}, 'A was pending when the application cancelled it');
    ok($seen{b_done}, 'B completed');
    like($h->{body}, qr/MARK-B\n\nEND\n\z/, 'and arrived, before the end');
    finish($h);
};

subtest 'a stream reset while sends are queued: they and later sends settle quietly' => sub {
    my (%seen, @after);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my @queued = map { $send->({ type => 'http.response.body', body => 'q' x 60_000, more => 1 }) } 1 .. 30;
        $seen{pending} = grep { !$_->is_ready } @queued;
        my $e = await $receive->();
        $e = await $receive->() while $e->{type} eq 'http.request';
        $seen{event} = $e->{type};
        push @after, $send->({ type => 'http.response.body', body => 'AFTER-RESET', more => 1 }) for 1 .. 3;
        await Future->wait_all(@queued, @after);
        $seen{failed} = grep { $_->is_failed } @queued, @after;
    };
    my $h = h2_client(open_h2c($app));
    request($h);
    ok(pump_until($h, 0, sub { $seen{pending} }, 5), 'sends were queued');
    $h->{client}->submit_rst_stream(1, 8);    # CANCEL
    ok(pump_until($h, 65536, sub { exists $seen{failed} }), 'every send settled');
    ok($seen{pending} > 0, "$seen{pending} sends were queued when the client reset the stream");
    is($seen{event}, 'http.disconnect', 'the application saw the disconnect');
    is($seen{failed}, 0, 'none of them failed');
    finish($h);
};

done_testing;
