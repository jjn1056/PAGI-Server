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
use Socket qw(AF_UNIX SOCK_STREAM SOL_SOCKET SO_SNDBUF SO_RCVBUF);
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
use Protocol::WebSocket::Frame;

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
# socket_buffer => $bytes makes the pair hold that little, so that output the
# client does not read backs up in the server on any platform.
sub h2_pair {
    my ($app, %opts) = @_;
    my $socket_buffer = delete $opts{socket_buffer};
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $_->blocking(0) for $sock_a, $sock_b;
    if ($socket_buffer) {
        setsockopt($sock_a, SOL_SOCKET, SO_SNDBUF, $socket_buffer) or die "SO_SNDBUF: $!";
        setsockopt($sock_b, SOL_SOCKET, SO_RCVBUF, $socket_buffer) or die "SO_RCVBUF: $!";
    }
    my $server = new_server($app, %opts);
    my $stream = IO::Async::Stream->new(read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
        (map { exists $opts{$_} ? ($_ => $opts{$_}) : () } qw(write_high_watermark write_low_watermark)),
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
# A reset first sends the frames that were waiting for this pass's extraction
# (the case above). Those can end the very stream being reset: here an earlier
# reset of it was waiting behind held output. nghttp2 must not then be handed
# a reset for a stream it has released (PAGI::Server::Connection's
# _h2_reset_stream says why). None of this shows on the wire -- nghttp2 drops
# the late frame -- so the case watches the server's own calls: the session's
# submit_rst_stream is wrapped, calling through, and notes whether the
# connection still had the stream. The resets are write_timeout's, and its
# sweep is called here as the server's timer calls it, with a clock of the
# test's own: the second reset has to come in the same loop pass as a paced
# send, before that pass's extraction.
subtest 'a reset is not submitted for a stream the waiting frames have just ended' => sub {
    my (%go, %ran, @resets);
    $go{$_} = $loop->new_future for qw(tick finish);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'sse') {
            await $send->({ type => 'sse.start' });
            await $go{tick};
            await $send->({ type => 'sse.send', data => 'tick' });   # its frames wait for this pass's extraction
            $ran{tick} = 1;
            await $go{finish};
            await $send->({ type => 'sse.close' });
            $ran{events} = 1;
            return;
        }
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        # /fill sends more than the connection's buffer takes, so its output
        # is held from here on; /slow's bytes then wait in its stream.
        await $send->({ type => 'http.response.body', more => 1,
            body => $scope->{path} eq '/fill' ? 'f' x 60_000 : 'x' x 1000 });
        await $go{finish};
        $ran{ $scope->{path} } = 1;
    };
    # The marks are far enough apart that what the client reads below makes
    # room for more than one whole DATA frame (part of one may be waiting
    # ahead of the resets) without reaching the low mark.
    my $pair = h2_pair($app, write_high_watermark => 40_000, write_low_watermark => 5_000, socket_buffer => 4096);
    my $conn = $pair->{conn};
    my $submit = \&PAGI::Server::Protocol::HTTP2::Session::submit_rst_stream;
    no warnings 'redefine';
    local *PAGI::Server::Protocol::HTTP2::Session::submit_rst_stream = sub {
        push @resets, [$_[1], $conn->_h2_stream_alive($_[1]) ? 'held by nghttp2' : 'released'];
        goto &$submit;
    };
    submit($pair, path => '/events', @SSE);
    exchange($pair) for 1 .. 3;
    my @stalled = (submit($pair, path => '/fill'));
    $loop->loop_once(0.01) for 1 .. 10;      # the client reads nothing
    push @stalled, submit($pair, path => '/slow');
    $loop->loop_once(0.01) for 1 .. 5;

    my $now = time;
    $conn->_h2_reset_write_stalled_streams($now, 1);          # the streams' clocks start
    $conn->_h2_reset_write_stalled_streams($now + 100, 1);    # a hundred seconds on: both are reset
    is([sort { $a->[0] <=> $b->[0] } @resets], [map { [$_, 'held by nghttp2'] } @stalled],
        'write_timeout reset the two streams the client was not reading');
    # The client takes some of what is waiting: the connection's buffer has
    # room again, but is not yet low enough for the server to carry on by
    # itself, so the two resets still wait in nghttp2.
    my ($got, $tries) = (0, 0);
    while ($got < 25_000 && ++$tries < 500) {
        my $n = $pair->{sock}->sysread(my $buf, 25_000 - $got);
        if ($n) { $got += $n; $pair->{client}->mem_recv($buf) }
        $loop->loop_once(0.01);
    }
    is([grep { $conn->_h2_stream_alive($_) } @stalled], \@stalled,
        "after the client read $got bytes both resets are still waiting");

    # One pass: a paced send on the third stream, then the sweep again. Its
    # reset sends the waiting frames first, the two earlier resets among them.
    @resets = ();
    $go{tick}->done;
    ok($ran{tick}, 'the event was sent');
    $conn->_h2_reset_write_stalled_streams($now + 200, 1);
    is([grep { $conn->_h2_stream_alive($_) } @stalled], [], 'the sweep went to reset a stream again, which ended both');
    is(\@resets, [], 'and no reset was submitted for a stream nghttp2 had released');

    $go{finish}->done;
    my $turns = 0;
    exchange($pair) until ($ran{events} && $ran{'/fill'} && $ran{'/slow'}) || ++$turns > 5000;
    ok($ran{events} && $ran{'/fill'} && $ran{'/slow'}, 'every application ran to its end');
    stop_server($pair->{server});
};

sub sends_before_ping {
    my (%arg) = @_;
    my ($k, $path) = ($arg{k}, $arg{stream} // '/stream');
    my ($sends, $started) = (0, undef);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'sse') {
            await $send->({ type => 'sse.start' });
            $started->done if $started && !$started->is_ready;
            for my $n (1 .. 2000) {
                $sends++;
                await $send->($scope->{path} eq '/comments'
                    ? { type => 'sse.comment', comment => 'x' x 96 }
                    : { type => 'sse.send', data => 'x' x 93 });
            }
            await $send->({ type => 'sse.close' });
            return;
        }
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        my $status = { '/unchanged' => 304, '/empty' => 204 }->{ $scope->{path} } // 200;
        await $send->({ type => 'http.response.start', status => $status,
            headers => $status == 200 ? [['content-type', 'text/plain']] : [] });
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
    my $server = new_server($app, %{ $arg{server} // {} });
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
        my $sid = $session->submit_request(method => $arg{method} // 'GET', path => $path, scheme => 'http', authority => 'x',
            ($path =~ m{\A/(?:events|comments)\z} ? (headers => [['accept', 'text/event-stream']]) : ()));
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
    is(sends_before_ping(k => 10, server => { max_sends_per_turn => 0 }), 1258 + 9 * 657,
        '10 streams run 1,258 + 9 x 657 sends before the ping is answered');
};

subtest 'a ping is answered within a bounded number of streaming sends' => sub {
    for my $k (10, 50) {
        my $ran = sends_before_ping(k => $k);
        cmp_ok($ran, '<=', $BOUND, "$k streams on one connection: $ran streaming sends ran before the ping was answered");
    }
};

subtest 'sse.send and sse.comment are bounded alike' => sub {
    for my $path ('/events', '/comments') {
        my $ran = sends_before_ping(k => 10, stream => $path);
        cmp_ok($ran, '<=', $BOUND, "$path: $ran sends ran before the ping was answered");
    }
};

# A response to HEAD, or one with status 204 or 304, has no body: the server
# discards its body sends. An application that streams one all the same sends
# as often and holds the loop as long, so its sends are counted like any
# others.
subtest 'a stream whose body is not sent is bounded alike' => sub {
    for my $case (
        ['a response to HEAD',         method => 'HEAD'],
        ['a response with status 304', stream => '/unchanged'],
        ['a response with status 204', stream => '/empty'],
    ) {
        my ($what, @arg) = @$case;
        my $ran = sends_before_ping(k => 50, @arg);
        cmp_ok($ran, '<=', $BOUND, "$what: $ran streaming sends ran before the ping was answered");
    }
};

# With a budget of 1 and thirty streams on one connection, a stream's send is
# held almost all the time and its turn comes round every thirty-odd loop turns.
sub endless_streams {
    my ($count, $state) = @_;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        my ($id) = $scope->{query_string} =~ /id=(\d+)/;
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        if ($scope->{path} eq '/burst') {
            # Twelve sends at once: with the count spent, four go through and eight are held.
            $state->{burst} = [map { $send->({ type => 'http.response.body', body => $LINE, more => 1 }) } 1 .. 12];
            await $receive->();                              # the request
            $state->{burst_event} = await $receive->();      # parked until the stream ends
            await $send->({ type => 'http.response.body', body => '' });
            $state->{burst_finished} = 1;
            return;
        }
        until ($state->{stop}) {
            $state->{current}{$id} = $send->({ type => 'http.response.body', body => $LINE, more => 1 });
            $state->{count}{$id}++;
            await $state->{current}{$id};
        }
        $state->{finished}++;
        await $send->({ type => 'http.response.body', body => '' });
    };
    my $pair = h2_pair($app, max_sends_per_turn => 1);
    my @sids = map { submit($pair, path => "/?id=$_") } 0 .. $count - 1;
    my $turns = 0;
    until ($count == grep { $_ && !$_->is_ready } values %{ $state->{current} }) {
        die "the streams never all had a send held\n" if ++$turns > 5000;
        exchange($pair);
    }
    # Stream 0 has just had its turn: its next send is held with the others ahead of it.
    my $was = $state->{count}{0};
    $turns = 0;
    until ($state->{count}{0} > $was) {
        die "stream 0 never had another turn\n" if ++$turns > 5000;
        exchange($pair);
    }
    return ($pair, @sids);
}

subtest 'a held send resolves when the client resets its stream' => sub {
    my %state;
    my ($pair, @sids) = endless_streams(30, \%state);
    my $held = $state{current}{0};
    ok(!$held->is_ready, 'the send is held');
    $pair->{client}->submit_rst_stream($sids[0], 8);   # CANCEL
    $pair->{sock}->syswrite($pair->{client}->mem_send);
    $loop->loop_once(0.01) for 1 .. 5;
    ok($held->is_done, 'five loop turns after the reset, it has resolved, not failed');
    # Every application runs to its end before the server stops, the reset
    # stream's included (its sends are no-ops now).
    $state{stop} = 1;
    my $turns = 0;
    exchange($pair) until ($state{finished} // 0) >= 30 || ++$turns > 5000;
    is($state{finished}, 30, 'and every application ran to its end');
    stop_server($pair->{server});
};

# A callback the application put on a held send runs when the stream's end
# resolves it. One that dies must not strand the stream's other held sends,
# nor stop the rest of the stream's ending.
subtest 'a callback that dies on a held send when the client resets its stream does not strand the others' => sub {
    my %state;
    my ($pair, @sids) = endless_streams(30, \%state);
    my $burst = submit($pair, path => '/burst');
    my $turns = 0;
    until ($state{burst}) {
        die "the burst never ran\n" if ++$turns > 5000;
        exchange($pair);
    }
    my @held = grep { !$_->is_ready } @{ $state{burst} };
    cmp_ok(scalar @held, '>=', 2, scalar(@held) . ' of its twelve sends are held');
    $held[0]->on_done(sub { die "callback boom\n" });
    $pair->{client}->submit_rst_stream($burst, 8);   # CANCEL
    $pair->{sock}->syswrite($pair->{client}->mem_send);
    my $escaped;
    for my $turn (1 .. 5) { eval { $loop->loop_once(0.01); 1 } or $escaped = $@ }
    is($escaped, undef, 'the death does not reach the event loop');
    is(scalar(grep { !$_->is_done } @held), 0, 'five loop turns after the reset, every held send has resolved');
    is($state{burst_event}{type}, 'http.disconnect', 'and its application was told the stream ended');
    is([map { "$_->{level}: $_->{message}" } grep { $_->{message} =~ /callback boom/ } @log],
        ["error: PAGI application error in a callback on a send's Future: callback boom"],
        'the death was logged once, as an error');
    @log = grep { $_->{message} !~ /callback boom/ } @log;
    $state{stop} = 1;
    $turns = 0;
    exchange($pair) until (($state{finished} // 0) >= 30 && $state{burst_finished}) || ++$turns > 5000;
    is($state{finished}, 30, 'and every application ran to its end');
    stop_server($pair->{server});
};

subtest 'a held send resolves when the connection closes' => sub {
    my %state;
    my ($pair, @sids) = endless_streams(30, \%state);
    my @held = grep { !$_->is_ready } values %{ $state{current} };
    cmp_ok(scalar @held, '>=', 29, scalar(@held) . ' sends are held');
    close $pair->{sock};
    $loop->loop_once(0.01) for 1 .. 5;
    is(scalar(grep { !$_->is_done } @held), 0, 'five loop turns after the client closed, every one has resolved');
    $state{stop} = 1;
    pump_until(sub { ($state{finished} // 0) >= 30 }, 10);
    is($state{finished}, 30, 'and every application ran to its end');
    stop_server($pair->{server});
};

subtest 'sends stay in order and the last one is not held back' => sub {
    my (@futures, $issued);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        @futures = map { $send->({ type => 'http.response.body', body => sprintf("N%05d\n", $_), more => 1 }) } 1 .. 200;
        # Counted before the last send: it ends the stream, and the stream's
        # end resolves whatever is still held.
        $issued = grep { !$_->is_ready } @futures;
        push @futures, $send->({ type => 'http.response.body', body => "END\n" });
        await Future->wait_all(@futures);
    };
    my $pair = h2_pair($app, max_sends_per_turn => 1);
    my $sid = submit($pair);
    my $turns = 0;
    exchange($pair) until defined $issued || ++$turns > 200;
    cmp_ok($issued, '>=', 190, "$issued of the 200 sends before the last were pending when it was issued");
    exchange($pair) for 1 .. 10;
    my $body = $pair->{seen}{body}{$sid} // '';
    is($pair->{seen}{closed}{$sid}, 0, 'ten loop turns later the stream has ended cleanly');
    like($body, qr/END\n\z/, 'with the last send');
    is([$body =~ /N(\d{5})\n/g], [map { sprintf '%05d', $_ } 1 .. 200], 'after every send, in the order issued');
    $turns = 0;
    exchange($pair) until !(grep { !$_->is_ready } @futures) || ++$turns > 2000;
    is(scalar(grep { !$_->is_done } @futures), 0, 'and every Future resolves');
    stop_server($pair->{server});
};

# As over HTTP/1.1 (Ruled by John 2026-11-22): a held send whose Future the
# application dropped has written its bytes and given up its place; when it
# resumes, Future::AsyncAwait warns that its Future was lost.
subtest 'a held send whose Future the application dropped still delivers, and warns' => sub {
    my (@warnings, $finished, $release);
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    $release = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        $send->({ type => 'http.response.body', body => sprintf("N%05d\n", $_), more => 1 }) for 1 .. 50;
        # The stream stays open over a serving of the queue, so it is the
        # serving, not the stream's end, that resolves the dropped sends.
        await $release;
        await $send->({ type => 'http.response.body', body => "END\n" });
        $finished = 1;
    };
    my $pair = h2_pair($app, max_sends_per_turn => 1);
    my $sid = submit($pair);
    exchange($pair) for 1 .. 10;
    is([($pair->{seen}{body}{$sid} // '') =~ /N(\d{5})\n/g], [map { sprintf '%05d', $_ } 1 .. 50],
        'every send arrived, in order, while the stream is still open');
    # One counted, four unheld, forty-five held.
    is(scalar(grep { /\ASuspended async sub .+? lost its returning future/ } @warnings), 45,
        'each of the 45 held sends warns that its Future was lost');
    is(scalar @warnings, 45, 'and nothing else warns');
    $release->done;
    my $turns = 0;
    exchange($pair) until exists $pair->{seen}{closed}{$sid} || ++$turns > 200;
    ok($finished, 'the application ran to its end');
    is($pair->{seen}{closed}{$sid}, 0, 'and the stream ended cleanly');
    stop_server($pair->{server});
};

subtest 'websocket.send is paced and keeps its order' => sub {
    my $held = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'websocket';
        await $receive->();
        await $send->({ type => 'websocket.accept' });
        for my $n (1 .. 2000) {
            my $f = $send->({ type => 'websocket.send', text => "M$n" });
            $held++ unless $f->is_ready;
            await $f;
        }
        await $send->({ type => 'websocket.close', code => 1000 });
        while (1) { last if (await $receive->())->{type} eq 'websocket.disconnect' }
    };
    my $pair = h2_pair($app, max_sends_per_turn => 1);
    my $sid = submit($pair, method => 'CONNECT', path => '/ws',
        headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
        body => sub { return undef });   # the request stream stays open
    my $parser = Protocol::WebSocket::Frame->new(max_payload_size => 1 << 20);
    my ($fed, @texts, $closed, $turns) = (0);
    until ($closed || ++$turns > 5000) {
        exchange($pair);
        my $body = $pair->{seen}{body}{$sid} // '';
        next unless length($body) > $fed;
        my $new = substr($body, $fed);   # a copy: append empties what it is given
        $fed = length $body;
        $parser->append($new);
        while (defined(my $payload = $parser->next_bytes)) {
            if ($parser->opcode == 8) { $closed = 1 } else { push @texts, $payload }
        }
    }
    ok($closed, 'the Close arrived');
    is(\@texts, [map { "M$_" } 1 .. 2000], 'after all 2,000 messages, in order');
    cmp_ok($held, '>=', 300, "$held of the sends were held");
    $pair->{client}->submit_rst_stream($sid, 8);
    $pair->{sock}->syswrite($pair->{client}->mem_send);
    exchange($pair) for 1 .. 5;
    stop_server($pair->{server});
};

# A held send has given up its place among its stream's sends: the sends
# behind it are written without waiting for it, whatever their type. (For
# body sends that is 'sends stay in order and the last one is not held back'.)
for my $case (
    ['sse.send',       sub { +{ type => 'sse.send', data => $_[0] } }],
    ['sse.comment',    sub { +{ type => 'sse.comment', comment => $_[0] } }],
    ['websocket.send', sub { +{ type => 'websocket.send', text => $_[0] } }],
) {
    my ($type, $event) = @$case;
    subtest "$type: the sends behind a held one are written without waiting for it" => sub {
        my (@futures, $issued);
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            die "unsupported scope type\n" unless $scope->{type} =~ /\A(?:sse|websocket)\z/;
            my $sse = $scope->{type} eq 'sse';
            await $receive->() unless $sse;
            await $send->({ type => $sse ? 'sse.start' : 'websocket.accept' });
            @futures = map { $send->($event->(sprintf 'N%05d', $_)) } 1 .. 200;
            $issued = grep { !$_->is_ready } @futures;
            await Future->wait_all(@futures);
            if ($sse) { await $send->({ type => 'sse.close' }); return }
            await $send->({ type => 'websocket.close', code => 1000 });
            while (1) { last if (await $receive->())->{type} eq 'websocket.disconnect' }
        };
        my $pair = h2_pair($app, max_sends_per_turn => 1);
        my $sid = $type eq 'websocket.send'
            ? submit($pair, method => 'CONNECT', path => '/ws',
                headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
                body => sub { return undef })   # the request stream stays open
            : submit($pair, @SSE);
        my $turns = 0;
        exchange($pair) until defined $issued || ++$turns > 200;
        cmp_ok($issued, '>=', 190, "$issued of the 200 sends were pending once all were issued");
        exchange($pair) for 1 .. 10;
        is([($pair->{seen}{body}{$sid} // '') =~ /N(\d{5})/g], [map { sprintf '%05d', $_ } 1 .. 200],
            'ten loop turns later the client has every one, in the order issued');
        $turns = 0;
        exchange($pair) until !(grep { !$_->is_ready } @futures) || ++$turns > 2000;
        is(scalar(grep { !$_->is_done } @futures), 0, 'and every Future resolves');
        if ($type eq 'websocket.send') {
            $pair->{client}->submit_rst_stream($sid, 8);
            $pair->{sock}->syswrite($pair->{client}->mem_send);
            exchange($pair) for 1 .. 5;
        }
        else {
            pump_until(sub { exchange($pair); exists $pair->{seen}{closed}{$sid} }, 5);
        }
        stop_server($pair->{server});
    };
}

is([map { "$_->{level}: $_->{message}" } @log], [], 'nothing else was logged');

done_testing;
