use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use IO::Socket::INET;
use Socket qw(AF_UNIX SOCK_STREAM);
use Time::HiRes qw(time);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# How a scope's sends hand the turn on (PAGI::Spec::Www 0.7 "Sends Are
# Sequential"): however many sends are queued, they are woken one after
# another without the stack growing; a send issued from an earlier send's
# callback still goes after the sends queued before it; an application
# callback that dies does not stall the sends behind it; and the server's own
# output does not count a waiting send's payload as written.

my $loop = IO::Async::Loop->new;
my @log;
my @warnings;
$SIG{__WARN__} = sub { push @warnings, $_[0] };

sub start_server {
    my ($app, %opts) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
        shutdown_timeout => 1, logger => sub { push @log, $_[0] }, %opts,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub stop_server {
    my ($server) = @_;
    $server->shutdown->get;
    $loop->remove($server);
}

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 30);
    $loop->loop_once(0.01) until $cond->() || time > $deadline;
    return $cond->();
}

# Sends a request on a fresh socket and reads, while the loop runs, until
# $done says the bytes so far are the whole answer (or $secs pass).
sub fetch {
    my ($server, $request, $done, $secs) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2,
    ) or die "connect: $!";
    $sock->blocking(0);
    syswrite($sock, $request);
    my $wire = '';
    pump_until(sub {
        while (defined(my $n = sysread($sock, my $buf, 1 << 20))) { last unless $n; $wire .= $buf }
        $done->($wire);
    }, $secs);
    close $sock;
    return $wire;
}

my $GET = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
my $chunked_end = sub { $_[0] =~ /\r\n0\r\n\r\n\z/ };

# The numbered bodies a burst sent, in the order they arrived.
sub numbers { my @n = $_[0] =~ /\bN(\d+)\n/g; return \@n }

subtest 'HTTP/1.1: 50,000 overlapping sends all arrive, in order, without deep recursion' => sub {
    @warnings = ();
    my $N = 50_000;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my @sent = map { $send->({ type => 'http.response.body', body => "N$_\n", more => 1 }) } 1 .. $N;
        push @sent, $send->({ type => 'http.response.body', body => '', more => 0 });
        await Future->needs_all(@sent);
    };
    my $server = start_server($app);
    my $wire = fetch($server, $GET, $chunked_end, 120);
    is(numbers($wire), [1 .. $N], 'every send arrived, in the order it was called');
    is([grep { /Deep recursion/ } @warnings], [], 'no deep recursion');
    stop_server($server);
};

subtest 'HTTP/2: 50,000 overlapping sends on a stream all arrive, in order, without deep recursion' => sub {
    skip_all 'HTTP/2 not available' unless PAGI::Server::Protocol::HTTP2->available;
    require Net::HTTP2::nghttp2::Session;
    @warnings = ();
    my $N = 50_000;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my @sent = map { $send->({ type => 'http.response.body', body => "N$_\n", more => 1 }) } 1 .. $N;
        push @sent, $send->({ type => 'http.response.body', body => '', more => 0 });
        await Future->needs_all(@sent);
    };
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1,
        access_log => undef, http2 => 1, logger => sub { push @log, $_[0] });
    $loop->add($server);
    socketpair(my $server_sock, my $client_sock, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $_->blocking(0) for $server_sock, $client_sock;
    my $stream = IO::Async::Stream->new(read_handle => $server_sock, write_handle => $server_sock, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => PAGI::Server::Protocol::HTTP1->new, server => $server,
        h2_protocol => $server->{http2_protocol}, h2c_enabled => $server->{h2c_enabled},
        extensions => $server->{extensions},
    );
    $server->add_child($stream);
    $conn->start;
    my ($body, $closed) = ('', 0);
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers => sub { 0 }, on_header => sub { 0 }, on_frame_recv => sub { 0 },
        on_data_chunk_recv => sub { $body .= $_[1]; 0 }, on_stream_close => sub { $closed = 1; 0 },
    });
    $client->send_connection_preface;
    $client->submit_request(method => 'GET', path => '/', scheme => 'http', authority => 'localhost', headers => []);
    pump_until(sub {
        my $out = $client->mem_send;
        syswrite($client_sock, $out) if length $out;
        while (defined(my $n = sysread($client_sock, my $buf, 1 << 20))) { last unless $n; $client->mem_recv($buf) }
        $closed;
    }, 120);
    ok($closed, 'the stream completed');
    is(numbers($body), [1 .. $N], 'every send arrived, in the order it was called');
    is([grep { /Deep recursion/ } @warnings], [], 'no deep recursion');
    $stream->close_now;
    $loop->remove($server);
};

subtest 'a send issued from an earlier send\'s callback goes after the sends queued before it' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my $big = $send->({ type => 'http.response.body', body => 'x' x 1_000_000, more => 1 });
        my @w = map { $send->({ type => 'http.response.body', body => "\nW$_\n", more => 1 }) } 1 .. 4;
        $seen{queued} = grep { !$_->is_ready } @w;
        my $x;
        $w[1]->on_done(sub {
            $seen{w3_w4_pending} = (!$w[2]->is_ready && !$w[3]->is_ready) ? 1 : 0;
            $x = $send->({ type => 'http.response.body', body => "\nX\n", more => 1 });
        });
        await Future->needs_all($big, @w);
        await $x;
        await $send->({ type => 'http.response.body', body => '', more => 0 });
    };
    my $server = start_server($app);
    my $wire = fetch($server, $GET, $chunked_end);
    is($seen{queued}, 4, 'the four sends were queued behind the large one');
    is($seen{w3_w4_pending}, 1, 'W3 and W4 were still queued when X was issued');
    my @order = $wire =~ /\n(W\d|X)\n/g;
    is(\@order, [qw(W1 W2 W3 W4 X)], 'X went after every send issued before it');
    stop_server($server);
};

subtest 'an application callback that dies does not stall the sends behind it' => sub {
    my %seen;
    @log = ();
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        # The large send fills the buffer; G then waits for it to drain while
        # holding the turn, and W1-W3 queue behind G. When the buffer drains,
        # G finishes and the hand-off wakes W1-W3, each of which finishes at
        # once -- so W1's callback runs, and dies, inside the hand-off.
        my $big = $send->({ type => 'http.response.body', body => 'x' x 1_000_000, more => 1 });
        my $g = $send->({ type => 'http.response.body', body => "\nG\n", more => 1 });
        my @w = map { $send->({ type => 'http.response.body', body => "\nW$_\n", more => 1 }) } 1 .. 3;
        $seen{queued} = grep { !$_->is_ready } @w;
        # Registered after needs_all: a Future stops running its callbacks at
        # the first that dies, so one registered earlier would keep needs_all
        # from ever seeing W1 finish -- the application's own composition,
        # which no server can repair.
        my $all = Future->needs_all($big, $g, @w);
        $w[0]->on_done(sub { die "callback boom\n" });
        await $all;
        $seen{all_done} = (grep { $_->is_done } @w) == 3 ? 1 : 0;
        await $send->({ type => 'http.response.body', body => '', more => 0 });
    };
    my $server = start_server($app);
    my $wire = fetch($server, $GET, $chunked_end, 10);
    is($seen{queued}, 3, 'the three sends were queued behind the large one');
    my @order = $wire =~ /\n(G|W\d)\n/g;
    is(\@order, [qw(G W1 W2 W3)], 'every send after the dying callback was still delivered');
    is($seen{all_done}, 1, 'and completed');
    like($wire, qr/\r\n0\r\n\r\n\z/, 'the response ended');
    my @errors = grep { ($_->{level} // '') eq 'error' } @log;
    is(scalar @errors, 1, 'the callback\'s error was logged once');
    like($errors[0]{message} // '', qr/callback boom/, 'with its message');
    stop_server($server);
};

subtest 'a waiting send the application cancels stops counting at once' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        my $transport = $scope->{'pagi.transport'};
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my $big = $send->({ type => 'http.response.body', body => 'x' x 1_000_000, more => 1 });
        my $g = $send->({ type => 'http.response.body', body => "\nG\n", more => 1 });
        my $w = $send->({ type => 'http.response.body', body => 'W' x 50_000, more => 1 });
        $seen{w_pending} = !$w->is_ready;
        my $before = $transport->buffered_amount;
        $w->cancel;
        $seen{dropped} = $before - $transport->buffered_amount;
        await Future->needs_all($big, $g);
        await $send->({ type => 'http.response.body', body => "\nEND\n", more => 0 });
    };
    my $server = start_server($app);
    my $wire = fetch($server, $GET, $chunked_end);
    ok($seen{w_pending}, 'the send was waiting its turn when the application cancelled it');
    is($seen{dropped}, 50_000, 'buffered_amount dropped by its payload at once');
    like($wire, qr/\nEND\n\r\n0\r\n\r\n\z/, 'the response went on without it');
    stop_server($server);
};

subtest 'the server\'s own output does not count a waiting send\'s payload as written' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'sse';
        await $send->({ type => 'sse.start' });
        await $send->({ type => 'sse.keepalive', interval => 0.02, comment => 'ka' });
        my $big = $send->({ type => 'sse.send', data => 'B' x 1_000_000 });
        my $waiting = $send->({ type => 'sse.send', data => 'W' x 50_000 });
        $seen{waiting} = $waiting;
        await Future->needs_all($big, $waiting);
        await $send->({ type => 'sse.close' });
    };
    my $server = start_server($app);
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp')
        or die "connect: $!";
    $sock->blocking(0);
    syswrite($sock, "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n");
    my $conn;
    ok(pump_until(sub { ($conn) = values %{ $server->{connections} }; $conn && $seen{waiting} }, 5),
        'the second event is waiting');
    # Several keepalive intervals pass while the client reads nothing.
    my $until = time + 0.3;
    $loop->loop_once(0.01) while time < $until;
    ok(!$seen{waiting}->is_ready, 'the second event is still waiting');
    ok(($conn->{_held_bytes} // 0) >= 50_000,
        'its payload still counts as not yet written (' . ($conn->{_held_bytes} // 0) . ' bytes held)');
    close $sock;
    stop_server($server);
};

done_testing;
