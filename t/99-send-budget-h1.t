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
use IO::Socket::INET;
use Protocol::WebSocket::Frame;
use Time::HiRes qw(time);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# max_sends_per_turn over HTTP/1.1: a worker runs that many sends that leave
# their scope open -- http.response.body with more, sse.send, sse.comment,
# websocket.send -- and then returns to the event loop for its other
# connections. A send past the count (and past its scope's four) has done all
# its work; its Future stays pending until the server has polled again.
#
# Every case counts sends or loop turns, never time, and builds its own server:
# the count carries over from one burst to the next. No case opens more than
# 100 connections before the loop runs (macOS queues 128).

my $loop = IO::Async::Loop->new;
my @log;
my $LINE = ('x' x 99) . "\n";      # a 100-byte send
my $BOUND = 3000;                  # streaming sends allowed before another request is answered

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
    my $deadline = time + ($secs // 20);
    $loop->loop_once(0.005) until $cond->() || time > $deadline;
    return $cond->();
}

# A client on the test's own loop. request($head, $want) writes a request and
# returns a Future resolved once the response read so far matches $want;
# leave() closes the socket.
sub client {
    my ($server) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1:' . $server->port) or die "connect: $!";
    my ($want, $done, $buf) = (undef, undef, '');
    my $stream = IO::Async::Stream->new(handle => $sock, on_read => sub {
        my (undef, $bref) = @_;
        $buf = substr($buf . $$bref, -4096); $$bref = '';
        if ($done && !$done->is_ready && $buf =~ $want) { $buf = ''; $done->done }
        return 0;
    });
    $loop->add($stream);
    return {
        request => sub {
            (my $head, $want) = @_;
            ($done, $buf) = ($loop->new_future, '');
            syswrite $sock, $head or die "write: $!";
            return $done;
        },
        leave => sub { $loop->remove($stream) if $stream->loop; close $sock },
    };
}

sub get { "GET $_[0] HTTP/1.1\r\nHost: x\r\n\r\n" }
sub events { "GET $_[0] HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n" }
my $ENDED = qr/\r\n0\r\n\r\n\z/;

# A raw non-blocking socket, for the cases that count loop turns themselves.
sub raw_connect {
    my ($server) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2,
    ) or die "connect: $!";
    $sock->blocking(0);
    return $sock;
}

sub read_now {
    my ($sock) = @_;
    my $buf = '';
    while (1) {
        my $n = sysread($sock, my $chunk, 65536);
        last unless defined $n && $n > 0;
        $buf .= $chunk;
    }
    return $buf;
}

# The streaming application every bound case uses. /stream, /events and
# /comments send 2,000 x 100 bytes, each send awaited and counted; /ping and
# /three are the requests that must not wait for them.
sub streaming_app {
    my ($sends, $started) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'sse') {
            await $receive->();
            await $send->({ type => 'sse.start' });
            $$started->done if $$started && !$$started->is_ready;
            for my $n (1 .. 2000) {
                $$sends++;
                await $send->($scope->{path} eq '/comments'
                    ? { type => 'sse.comment', comment => 'x' x 96 }
                    : { type => 'sse.send', data => 'x' x 93 });
            }
            await $send->({ type => 'sse.close' });
            return;
        }
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        if ($scope->{path} eq '/ping') {
            await $send->({ type => 'http.response.body', body => "pong\n" });
            return;
        }
        if ($scope->{path} eq '/three') {
            for my $n (1 .. 3) {
                await $send->({ type => 'http.response.body', body => "part\n", more => 1 });
            }
            await $send->({ type => 'http.response.body', body => "pong\n" });
            return;
        }
        $$started->done if $$started && !$$started->is_ready;
        for my $n (1 .. 2000) {
            $$sends++;
            await $send->({ type => 'http.response.body', body => $LINE, more => 1 });
        }
        await $send->({ type => 'http.response.body', body => '' });
    };
}

# The streaming sends that run between the first of $k streams starting and
# the probe request's whole response arriving. The probe is written from
# inside the loop turn in which the first stream starts, on a connection
# already open, as are the streams' own connections.
sub sends_before_answer {
    my (%arg) = @_;
    my ($sends, $started) = (0, undef);
    my $server = start_server(streaming_app(\$sends, \$started), %{ $arg{server} // {} });
    my $probe = client($server);
    $probe->{request}->(get('/ping'), qr/pong\n/)->get;
    my @streams = map { client($server) } 1 .. $arg{k};
    Future->wait_all(map { $_->{request}->(get('/ping'), qr/pong\n/) } @streams)->get;

    my $stream_head = ($arg{stream} // '/stream') eq '/stream' ? get('/stream') : events($arg{stream});
    my $probe_path  = $arg{probe} // '/ping';
    $started = $loop->new_future;
    my ($sends_at, $answered);
    $started->on_done(sub {
        $sends_at = $sends;
        $answered = $probe->{request}->(get($probe_path), qr/pong\n\r\n0\r\n\r\n\z/);
    });
    my @done = map { $_->{request}->($stream_head, $ENDED) } @streams;
    $loop->loop_once until $answered;
    $answered->get;
    my $ran = $sends - $sends_at;
    Future->wait_all(@done)->get;
    $_->{leave}->() for $probe, @streams;
    stop_server($server);
    return $ran;
}

subtest 'a ping is answered within a bounded number of streaming sends' => sub {
    for my $k (10, 50) {
        my $ran = sends_before_answer(k => $k);
        cmp_ok($ran, '<=', $BOUND, "$k streams: $ran streaming sends ran before the ping was answered");
    }
};

subtest 'max_sends_per_turn => 0 is the server without the limit' => sub {
    # 618 sends of 100 bytes (106 with chunk framing) fill a 64 KiB buffer.
    for my $k (10, 50) {
        is(sends_before_answer(k => $k, server => { max_sends_per_turn => 0 }), $k * 618,
            "$k streams each run a whole buffer before the ping is answered");
    }
};

subtest 'a response of three body sends completes among the streams' => sub {
    my $ran = sends_before_answer(k => 50, probe => '/three');
    cmp_ok($ran, '<=', $BOUND, "$ran streaming sends ran before the four-send response was complete");
};

subtest 'sse.send and sse.comment are bounded alike' => sub {
    for my $path ('/events', '/comments') {
        my $ran = sends_before_answer(k => 10, stream => $path);
        cmp_ok($ran, '<=', $BOUND, "$path: $ran sends ran before the ping was answered");
    }
};

subtest 'a worker that is not stalling holds nothing' => sub {
    my ($held, $sends, $started) = (0, 0, 0);
    my @tick;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        $started++;
        my $quiet = $scope->{path} eq '/quiet';
        for my $i (1 .. ($quiet ? 3000 : 60)) {
            if ($quiet) { await $loop->delay_future(after => 0) }
            else        { await $tick[$i] }
            my $f = $send->({ type => 'http.response.body', body => $LINE, more => 1 });
            $sends++;
            $held++ unless $f->is_ready;
            await $f;
        }
        await $send->({ type => 'http.response.body', body => '' });
    };

    my $server = start_server($app);
    my $one = client($server);
    $one->{request}->(get('/quiet'), $ENDED)->get;
    is([$sends, $held], [3000, 0], 'one stream sending once per loop turn: 3,000 sends, none pending at return');
    $one->{leave}->();
    stop_server($server);

    ($held, $sends, $started) = (0, 0, 0);
    @tick = map { $loop->new_future } 0 .. 60;
    $server = start_server($app);
    my @clients = map { client($server) } 1 .. 100;
    my @done = map { $_->{request}->(get('/fan'), $ENDED) } @clients;
    pump_until(sub { $started == 100 });
    for my $i (1 .. 60) {
        $loop->delay_future(after => 0.005)->get;
        $tick[$i]->done;
    }
    Future->wait_all(@done)->get;
    is([$sends, $held], [6000, 0], 'one event fanned out to 100 scopes, 60 times: 6,000 sends, none pending at return');
    $_->{leave}->() for @clients;
    stop_server($server);
};

# With a budget of 1 and fifty other streams running, a stream's send is held
# almost all the time and its turn comes round every fifty-odd loop turns.
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
            await($state->{parked} = $loop->new_future);
            await $send->({ type => 'http.response.body', body => '' });
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
    my $server = start_server($app, max_sends_per_turn => 1);
    my @clients = map { client($server) } 1 .. $count;
    $clients[$_]->{request}->(get("/?id=$_"), $ENDED) for 0 .. $#clients;
    pump_until(sub { $count == grep { $_ && !$_->is_ready } values %{ $state->{current} } })
        or die "the streams never all had a send held\n";
    return ($server, @clients);
}

subtest 'a held send resolves when its client leaves' => sub {
    my %state;
    my ($server, @clients) = endless_streams(51, \%state);
    # Stream 0 has just had its turn: its next send is held with fifty scopes ahead of it.
    my $was = $state{count}{0};
    pump_until(sub { $state{count}{0} > $was });
    my $held = $state{current}{0};
    ok(!$held->is_ready, 'the send is held');
    $clients[0]->{leave}->();
    $loop->loop_once(0.01) for 1 .. 5;
    ok($held->is_done, 'five loop turns after its client left, it has resolved, not failed');
    $state{stop} = 1;
    pump_until(sub { ($state{finished} // 0) >= 50 });
    $_->{leave}->() for @clients[1 .. $#clients];
    stop_server($server);
};

subtest 'held sends resolve at graceful shutdown' => sub {
    my %state;
    my ($server, @clients) = endless_streams(100, \%state);
    my $shutdown = $server->shutdown;
    ok(pump_until(sub { $shutdown->is_ready }, 5), 'the server stops (shutdown_timeout is 1 s)');
    # With a hundred scopes queued and one served per loop turn, five turns
    # are far too few for the queue to reach them: an application gets to its
    # end here only because closing its connection resolved its held send.
    $state{stop} = 1;
    $loop->loop_once(0.01) for 1 .. 5;
    is($state{finished}, 100, 'five loop turns later every application has run to its end');
    $_->{leave}->() for @clients;
    $loop->remove($server);
};

subtest 'sends stay in order and the last one is not held back' => sub {
    my (@futures, $issued);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        @futures = map { $send->({ type => 'http.response.body', body => sprintf("N%05d\n", $_), more => 1 }) } 1 .. 200;
        push @futures, $send->({ type => 'http.response.body', body => "END\n" });
        $issued = grep { !$_->is_ready } @futures;
        await Future->wait_all(@futures);
    };
    my $server = start_server($app, max_sends_per_turn => 1);
    my $sock = raw_connect($server);
    syswrite($sock, get('/'));
    pump_until(sub { defined $issued });
    cmp_ok($issued, '>=', 190, "$issued of the 201 sends were pending once all were issued");
    my $wire = '';
    for (1 .. 10) {
        $loop->loop_once(0.01);
        $wire .= read_now($sock);
    }
    like($wire, qr/END\n\r\n0\r\n\r\n\z/, 'ten loop turns later the client has the whole response');
    is([$wire =~ /N(\d{5})\n/g], [map { sprintf '%05d', $_ } 1 .. 200], 'every send, in the order issued');
    ok(pump_until(sub { !grep { !$_->is_ready } @futures }), 'every Future settles');
    is(scalar(grep { !$_->is_done } @futures), 0, 'successfully');
    close $sock;
    stop_server($server);
};

subtest 'cancelling a held send does not stall the sends behind it' => sub {
    my (@futures, @warnings, $issued);
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        @futures = map { $send->({ type => 'http.response.body', body => sprintf("N%05d\n", $_), more => 1 }) } 1 .. 10;
        $issued = grep { !$_->is_ready } @futures;
        my ($cancelled) = grep { !$_->is_ready } @futures;
        $cancelled->cancel;
        push @futures, $send->({ type => 'http.response.body', body => "END\n" });
        await Future->wait_all(@futures);
    };
    my $server = start_server($app, max_sends_per_turn => 1);
    my $sock = raw_connect($server);
    syswrite($sock, get('/'));
    my $wire = '';
    pump_until(sub { $wire .= read_now($sock); $wire =~ /END\n\r\n0\r\n\r\n\z/ }, 10);
    like($wire, qr/END\n\r\n0\r\n\r\n\z/, 'the response completed');
    cmp_ok($issued, '>=', 2, "$issued sends were held, one of them cancelled");
    is([$wire =~ /N(\d{5})\n/g], [map { sprintf '%05d', $_ } 1 .. 10], 'every send arrived, in order');
    ok(pump_until(sub { !grep { !$_->is_ready } @futures }), 'the sends behind the cancelled one settle');
    is(scalar(grep { $_->is_cancelled } @futures), 1, 'only the cancelled one is cancelled');
    is(scalar(grep { $_->is_failed } @futures), 0, 'none failed');
    is(\@warnings, [], 'no warnings');
    close $sock;
    stop_server($server);
};

# An application is to await every send. One that drops a send's Future
# instead (Ruled by John 2026-11-22): a held send has written its bytes before
# it waits and gives up its place, so nothing is lost and the sends behind it
# go out; when it resumes, Future::AsyncAwait says its Future was lost, as it
# does for any dropped send that had to wait.
subtest 'a held send whose Future the application dropped still delivers, and warns' => sub {
    my (@warnings, $finished);
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        $send->({ type => 'http.response.body', body => sprintf("N%05d\n", $_), more => 1 }) for 1 .. 50;
        await $send->({ type => 'http.response.body', body => "END\n" });
        $finished = 1;
    };
    my $server = start_server($app, max_sends_per_turn => 1);
    my $sock = raw_connect($server);
    syswrite($sock, get('/'));
    my $wire = '';
    pump_until(sub { $wire .= read_now($sock); $wire =~ /END\n\r\n0\r\n\r\n\z/ }, 10);
    is([$wire =~ /N(\d{5})\n/g], [map { sprintf '%05d', $_ } 1 .. 50], 'every send arrived, in order');
    ok($finished, 'the application ran to its end');
    $loop->loop_once(0.01) for 1 .. 5;     # the queue is served meanwhile
    # One counted, four unheld, forty-five held.
    is(scalar(grep { /\ASuspended async sub .+? lost its returning future/ } @warnings), 45,
        'each of the 45 held sends warns that its Future was lost');
    is(scalar @warnings, 45, 'and nothing else warns');
    is($server->{_paced_scopes} // [], [], 'nothing is left waiting on the server');
    close $sock;
    stop_server($server);
};

subtest 'a callback that dies on a held send does not strand the others' => sub {
    my %futures;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        my $who = $scope->{path};
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        my @sent = map { $send->({ type => 'http.response.body', body => $LINE, more => 1 }) } 1 .. 10;
        $futures{$who} = [@sent];
        # The application's own wait is attached first: a callback that dies
        # stops the callbacks behind it on the same Future, which is the
        # application's affair. What the server owes is the other sends.
        my $all = Future->wait_all(@sent);
        if ($who eq '/first') {
            my ($held) = grep { !$_->is_ready } @sent;
            $held->on_done(sub { die "callback boom\n" });
        }
        await $all;
        await $send->({ type => 'http.response.body', body => '' });
    };
    my $server = start_server($app, max_sends_per_turn => 1);
    my @clients = map { client($server) } 1 .. 2;
    my $first  = $clients[0]->{request}->(get('/first'), $ENDED);
    pump_until(sub { $futures{'/first'} });
    my $second = $clients[1]->{request}->(get('/second'), $ENDED);
    ok(pump_until(sub { $first->is_ready && $second->is_ready }, 10), 'both responses completed');
    is(scalar(grep { !$_->is_done } map { @$_ } values %futures), 0, 'every send of both scopes resolved');
    is([map { "$_->{level}: $_->{message}" } grep { $_->{message} =~ /callback boom/ } @log],
        ["error: PAGI application error in a callback on a send's Future: callback boom"],
        'and the death was logged once, as an error');
    @log = grep { $_->{message} !~ /callback boom/ } @log;
    $_->{leave}->() for @clients;
    stop_server($server);
};

# The same death when the held sends are resolved by their client leaving, not
# by the queue being served.
subtest 'a callback that dies on a held send when its client leaves does not strand the others' => sub {
    my %state;
    my ($server, @clients) = endless_streams(50, \%state);
    my $burst = client($server);
    $burst->{request}->(get('/burst'), $ENDED);
    pump_until(sub { $state{burst} }) or die "the burst never ran\n";
    my @held = grep { !$_->is_ready } @{ $state{burst} };
    cmp_ok(scalar @held, '>=', 2, scalar(@held) . ' of its twelve sends are held');
    $held[0]->on_done(sub { die "callback boom\n" });
    $burst->{leave}->();
    my $escaped;
    for my $turn (1 .. 5) { eval { $loop->loop_once(0.01); 1 } or $escaped = $@ }
    is($escaped, undef, 'the death does not reach the event loop');
    is(scalar(grep { !$_->is_done } @held), 0, 'five loop turns after its client left, every held send has resolved');
    is([map { "$_->{level}: $_->{message}" } grep { $_->{message} =~ /callback boom/ } @log],
        ["error: PAGI application error in a callback on a send's Future: callback boom"],
        'and the death was logged once, as an error');
    @log = grep { $_->{message} !~ /callback boom/ } @log;
    $state{stop} = 1;
    $state{parked}->done;
    pump_until(sub { ($state{finished} // 0) >= 50 });
    $_->{leave}->() for @clients;
    stop_server($server);
};

subtest 'a second request on the connection is answered whole' => sub {
    my $held = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        if ($scope->{path} eq '/second') {
            await $send->({ type => 'http.response.body', body => "SECOND\n" });
            return;
        }
        for my $n (1 .. 50) {
            my $f = $send->({ type => 'http.response.body', body => sprintf("N%05d\n", $n), more => 1 });
            $held++ unless $f->is_ready;
            await $f;
        }
        await $send->({ type => 'http.response.body', body => "FIRST\n" });
    };
    my $server = start_server($app, max_sends_per_turn => 1);
    my $sock = raw_connect($server);
    syswrite($sock, get('/first'));
    my $wire = '';
    pump_until(sub { $wire .= read_now($sock); $wire =~ /FIRST\n\r\n0\r\n\r\n\z/ }, 10);
    is([$wire =~ /N(\d{5})\n/g], [map { sprintf '%05d', $_ } 1 .. 50], 'the first response, whole and in order');
    cmp_ok($held, '>=', 5, "$held of its sends were held");
    syswrite($sock, get('/second'));
    $wire = '';
    pump_until(sub { $wire .= read_now($sock); $wire =~ /\r\n0\r\n\r\n\z/ }, 10);
    like($wire, qr{\AHTTP/1\.1 200 .*\r\n\r\n7\r\nSECOND\n\r\n0\r\n\r\n\z}s, 'then the second, whole');
    close $sock;
    stop_server($server);
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
    my $server = start_server($app, max_sends_per_turn => 1);
    my $sock = raw_connect($server);
    syswrite($sock, join "\r\n", 'GET /ws HTTP/1.1', 'Host: localhost', 'Upgrade: websocket',
        'Connection: Upgrade', 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==', 'Sec-WebSocket-Version: 13', '', '');
    my $parser = Protocol::WebSocket::Frame->new(max_payload_size => 1 << 20);
    my ($head, @texts, $closed) = ('');
    pump_until(sub {
        my $buf = read_now($sock);
        if (defined $head) {
            $head .= $buf;
            return 0 unless $head =~ s/\A.*?\r\n\r\n//s;
            ($buf, $head) = ($head, undef);
        }
        $parser->append($buf) if length $buf;
        while (defined(my $payload = $parser->next_bytes)) {
            if ($parser->opcode == 8) { $closed = 1 } else { push @texts, $payload }
        }
        return $closed;
    }, 30);
    ok($closed, 'the Close arrived');
    is(\@texts, [map { "M$_" } 1 .. 2000], 'after all 2,000 messages, in order');
    cmp_ok($held, '>=', 300, "$held of the sends were held");
    close $sock;
    stop_server($server);
};

# PAGI::Spec::Www "Transport Flow Control": on_high_water when the buffer
# reaches the high mark, on_drain when it falls back below the low mark, then
# the cycle re-arms. A held send lets the buffer be seen below the low mark
# before the application refills it, so an application that awaits its sends
# gets the pair every cycle, as one that pauses on the callbacks does.
for my $mode (qw(awaits pauses)) {
    subtest "an application that $mode gets one on_drain per on_high_water" => sub {
        my ($high, $drain) = (0, 0);
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            die "unsupported scope type\n" unless $scope->{type} eq 'http';
            my $t = $scope->{'pagi.transport'};
            my $resume;
            $t->on_high_water(sub { $high++ });
            $t->on_drain(sub { $drain++; $resume->done if $resume && !$resume->is_ready });
            await $send->({ type => 'http.response.start', status => 200, headers => [] });
            for my $n (1 .. 20_000) {
                if ($mode eq 'pauses' && $t->buffered_amount >= $t->high_water_mark) {
                    $resume = $loop->new_future;
                    await $resume;
                }
                await $send->({ type => 'http.response.body', body => $LINE, more => 1 });
            }
            await $send->({ type => 'http.response.body', body => '' });
        };
        my $server = start_server($app);
        my $reader = client($server);
        $reader->{request}->(get('/'), $ENDED)->get;
        cmp_ok($high, '>', 1, "on_high_water fired $high times over the response");
        is($drain, $high, 'and on_drain as often');
        $reader->{leave}->();
        stop_server($server);
    };
}

is([map { "$_->{level}: $_->{message}" } @log], [], 'nothing else was logged');

done_testing;
