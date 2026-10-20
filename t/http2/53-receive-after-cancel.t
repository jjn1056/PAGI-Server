#!/usr/bin/env perl

# The HTTP/2 twin of t/receive-after-cancel.t, over a real nghttp2 client
# session on a socketpair: the application races a receive against a timer,
# which cancels the receive, then receives again. Cancelling a pending receive
# must not affect the stream (PAGI::Spec::Www): the later receive gets the DATA
# frame the client sends.

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use Socket qw(AF_UNIX SOCK_STREAM);
use Time::HiRes qw(time);
use FindBin;
use lib "$FindBin::Bin/../../lib";

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

# Future::XS, when it is in use, warns "lost a sequence Future" when a
# without_cancel view is dropped before its original resolves (the upstream
# bug PAGI::Server's POD notes); below Perl 5.24 a cancelled async sub warns
# "lost its returning future". Nothing else may warn.
my @warnings;
$SIG{__WARN__} = sub { push @warnings, @_ };
use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;
my @LOG;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'http';

    my $first = $receive->();
    await Future->wait_any($first, $loop->delay_future(after => 0.1));
    my $outcome = $first->is_cancelled ? 'cancelled' : 'answered';

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
my (%headers, $response_body, $closed);
my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
    on_begin_headers   => sub { 0 },
    on_header          => sub { my ($sid, $name, $value) = @_; $headers{$name} = $value; 0 },
    on_frame_recv      => sub { 0 },
    on_data_chunk_recv => sub { my ($sid, $data) = @_; $response_body .= $data; 0 },
    on_stream_close    => sub { $closed = 1; 0 },
});

sub exchange {
    my ($seconds) = @_;
    my $deadline = time + $seconds;
    while (time < $deadline) {
        $loop->loop_once(0.05);
        my $buf = '';
        $sock_b->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $sock_b->syswrite($out) if length($out);
        last if $closed;
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

my $stream_id = $client->submit_request(
    method => 'POST', path => '/echo', scheme => 'http', authority => 'localhost',
    headers => [['content-length', '4']],
    body => sub { return undef },            # deferred: no DATA yet
);
exchange(0.4);                               # the app's timer fires meanwhile
ok(eval { $client->submit_data($stream_id, "body", 1); 1 }, 'the DATA frame is queued')   # with END_STREAM
    or diag "submit_data: $@";
$sock_b->syswrite($client->mem_send);
exchange(3);

is($headers{':status'}, '200', 'the request succeeds');
is($response_body, "first receive cancelled; body 'body'", 'the later receive got the body');
is([grep { /^\[error\]/ } @LOG], [], 'no application error was logged');

eval { $server->shutdown->get };
eval { $loop->remove($server) };
my @unexpected = grep {
    !($] < 5.024 && /lost its returning future/) && !($INC{'Future/XS.pm'} && /lost a sequence Future/)
} @warnings;
is(\@unexpected, [], 'no unexpected warnings');
done_testing;
