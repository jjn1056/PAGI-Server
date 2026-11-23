use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use IO::Socket::INET;
use Socket qw(AF_UNIX SOCK_STREAM);
use Time::HiRes qw(time);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.014+ required)');
}

# max_sends_per_turn over HTTP/2. As on HTTP/1.1, a send that carries data and
# leaves its stream open counts against the worker's limit and may wait for
# the loop once its work is done (t/99-send-budget-h1.t). HTTP/2 adds one
# thing: while the limit is on, such a send does not ask nghttp2 for frames
# itself; the connection asks once per loop pass, for all of them. Whatever
# else asks takes the waiting frames along -- a scope's last send, and a reset
# of the stream, which would otherwise discard them.
#
# Every case counts sends or loop turns, never time, and builds its own server.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Net::HTTP2::nghttp2::Session;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;
my @log;
my $LINE = ('x' x 99) . "\n";      # a 100-byte send
my $BOUND = 3000;                  # streaming sends allowed before another request is answered

sub new_server {
    my ($app, %opts) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
        http2 => 1, shutdown_timeout => 1, logger => sub { push @log, $_[0] }, %opts,
    );
    $loop->add($server);
    return $server;
}

sub stop_server {
    my ($server) = @_;
    $server->shutdown->get;
    $loop->remove($server);
}

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 20);
    $loop->loop_once(0.005) until $cond->() || time > $deadline;
    return $cond->();
}

# One HTTP/2 connection over a socket pair, driven by hand: the test decides
# when the client reads. Returns the client's socket, its nghttp2 session, what
# it has seen, and the server and connection (which the caller keeps alive).
sub h2_pair {
    my ($app, %opts) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $_->blocking(0) for $sock_a, $sock_b;
    my $server = new_server($app, %opts);
    my $stream = IO::Async::Stream->new(read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;

    my %seen = (body => {}, closed => {}, order => []);
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { 0 },
        on_frame_recv      => sub { 0 },
        on_data_chunk_recv => sub { $seen{body}{ $_[0] } .= $_[1]; push @{ $seen{order} }, "data $_[0]"; 0 },
        on_stream_close    => sub { $seen{closed}{ $_[0] } = $_[1]; push @{ $seen{order} }, "close $_[0]"; 0 },
    });
    # The server's SETTINGS are fed to the client only after its own preface,
    # so the client's first frame is its SETTINGS, not an ACK.
    $loop->loop_once(0.1);
    $sock_b->sysread(my $server_settings, 65536);
    $client->send_connection_preface;
    $sock_b->syswrite($client->mem_send);
    $client->mem_recv($server_settings) if length($server_settings // '');
    my $pair = { sock => $sock_b, client => $client, seen => \%seen, server => $server, conn => $conn };
    exchange($pair) for 1 .. 3;
    return $pair;
}

# One loop turn, then whatever is waiting each way between client and server.
sub exchange {
    my ($pair) = @_;
    $loop->loop_once(0.01);
    while (defined(my $n = $pair->{sock}->sysread(my $buf, 65536))) {
        last unless $n;
        $pair->{client}->mem_recv($buf);
    }
    my $out = $pair->{client}->mem_send;
    $pair->{sock}->syswrite($out) if length $out;
    return;
}

sub submit {
    my ($pair, %request) = @_;
    my $sid = $pair->{client}->submit_request(
        method => 'GET', path => '/', scheme => 'https', authority => 'localhost', %request,
    );
    $pair->{sock}->syswrite($pair->{client}->mem_send);
    return $sid;
}

my @SSE = (headers => [['accept', 'text/event-stream']]);

subtest 'one paced send reaches the client without another send' => sub {
    my ($sent, $release) = (0, $loop->new_future);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'sse';
        await $send->({ type => 'sse.start' });
        await $send->({ type => 'sse.send', data => 'hello' });
        $sent = 1;
        await $release;
        await $send->({ type => 'sse.close' });
    };
    my $pair = h2_pair($app);
    my $sid = submit($pair, @SSE);
    my $turns = 0;
    until ($sent || ++$turns > 200) { exchange($pair) }
    ok($sent, 'the application sent its event and is waiting');
    exchange($pair) for 1 .. 5;
    like($pair->{seen}{body}{$sid} // '', qr/data: hello\n\n/, 'five loop turns later the client has it');
    $release->done;
    pump_until(sub { exchange($pair); exists $pair->{seen}{closed}{$sid} }, 5);
    stop_server($pair->{server});
};

subtest 'an event sent just before the server resets the stream arrives first' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'sse';
        await $send->({ type => 'sse.start' });
        await $send->({ type => 'sse.send', data => 'last words' });
        return;   # without sse.close: the server resets the stream
    };
    my $pair = h2_pair($app);
    my $sid = submit($pair, @SSE);
    pump_until(sub { exchange($pair); exists $pair->{seen}{closed}{$sid} }, 5);
    like($pair->{seen}{body}{$sid} // '', qr/data: last words\n\n/, 'the event arrived');
    is($pair->{seen}{order}, ["data $sid", "close $sid"], 'before the stream closed');
    isnt($pair->{seen}{closed}{$sid}, 0, 'and the stream was reset, not ended');
    my $unclean = qr/returned after sse\.start without ending the scope cleanly/;
    is([map { $_->{level} } grep { $_->{message} =~ $unclean } @log], ['error'],
        'the application returning without sse.close was logged once');
    @log = grep { $_->{message} !~ $unclean } @log;
    stop_server($pair->{server});
};

# The streaming sends that run between the first of $k streams on one HTTP/2
# connection starting and a ping on another connection being answered. Real
# sockets, clients on the test's own loop, as stall-repro.pl has it.
sub sends_before_ping {
    my ($k, %opts) = @_;
    my ($sends, $started) = (0, undef);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        if ($scope->{path} eq '/ping') {
            await $send->({ type => 'http.response.body', body => "pong\n" });
            return;
        }
        $started->done if $started && !$started->is_ready;
        for my $n (1 .. 2000) {
            $sends++;
            await $send->({ type => 'http.response.body', body => $LINE, more => 1 });
        }
        await $send->({ type => 'http.response.body', body => '' });
    };
    my $server = new_server($app, %opts);
    $server->listen->get;
    my $port = $server->port;

    # The ping's connection: HTTP/1.1, already open and used once.
    my $ping_sock = IO::Socket::INET->new(PeerAddr => "127.0.0.1:$port") or die "connect: $!";
    my ($pong, $ping_buf) = (undef, '');
    my $ping_stream = IO::Async::Stream->new(handle => $ping_sock, on_read => sub {
        my (undef, $bref) = @_;
        $ping_buf .= $$bref; $$bref = '';
        if ($pong && !$pong->is_ready && $ping_buf =~ /pong\n/) { $ping_buf = ''; $pong->done }
        return 0;
    });
    $loop->add($ping_stream);
    my $ping = sub {
        ($pong, $ping_buf) = ($loop->new_future, '');
        syswrite $ping_sock, "GET /ping HTTP/1.1\r\nHost: x\r\n\r\n" or die "write: $!";
        return $pong;
    };
    $ping->()->get;

    # The streams' connection: HTTP/2 with prior knowledge, settled.
    my $h2_sock = IO::Socket::INET->new(PeerAddr => "127.0.0.1:$port") or die "connect: $!";
    my (%closed, $settled);
    my $session = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { 0 },
        on_frame_recv      => sub { $settled->done if $_[0]{type} == 4 && !$settled->is_ready; 0 },
        on_data_chunk_recv => sub { 0 },
        on_stream_close    => sub { $closed{ $_[0] }->done if $closed{ $_[0] }; 0 },
    });
    my $flush = sub { my $out = $session->mem_send; syswrite $h2_sock, $out or die "write: $!" if length $out };
    my $h2_stream = IO::Async::Stream->new(handle => $h2_sock, on_read => sub {
        my (undef, $bref) = @_;
        $session->mem_recv($$bref); $$bref = '';
        $flush->();
        return 0;
    });
    $loop->add($h2_stream);
    $settled = $loop->new_future;
    $session->send_connection_preface;
    $flush->();
    $settled->get;

    $started = $loop->new_future;
    my ($sends_at, $answered);
    $started->on_done(sub { $sends_at = $sends; $answered = $ping->() });
    my @done = map {
        my $sid = $session->submit_request(method => 'GET', path => '/stream', scheme => 'http', authority => 'x');
        $closed{$sid} = $loop->new_future;
    } 1 .. $k;
    $flush->();
    $loop->loop_once until $answered;
    $answered->get;
    my $ran = $sends - $sends_at;
    Future->wait_all(@done)->get;
    $loop->remove($_) for $ping_stream, $h2_stream;
    close $_ for $ping_sock, $h2_sock;
    stop_server($server);
    return $ran;
}

subtest 'max_sends_per_turn => 0 is the server without the limit: frames are extracted inside the send' => sub {
    # The first stream's sends fill the connection's 64 KiB buffer as well as
    # its own queue (1,258 sends of 100 bytes); with the buffer full, every
    # other stream fills its queue alone (657).
    is(sends_before_ping(10, max_sends_per_turn => 0), 1258 + 9 * 657,
        '10 streams run 1,258 + 9 x 657 sends before the ping is answered');
};

is([map { "$_->{level}: $_->{message}" } @log], [], 'nothing else was logged');

done_testing;
