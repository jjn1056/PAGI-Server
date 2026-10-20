#!/usr/bin/env perl

# An application may give up on a receive while it waits -- Future->wait_any
# against a timer cancels the receive that lost -- and receive again later.
# Cancelling a pending receive must not affect the connection: the later
# receive still gets the client's next event, and another pending receive is
# unaffected (PAGI::Spec::Www). The HTTP/2 twin is t/http2/53.

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use Protocol::WebSocket::Frame;
use Time::HiRes qw(time);
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Two known warnings, from the libraries rather than the server, and nothing
# else, on any perl: below Perl 5.24 a cancelled async sub stops but stays
# suspended (Future::AsyncAwait does not pass the cancel on there), so dropping
# its Future warns "lost its returning future"; and Future::XS, when it is in
# use, warns "lost a sequence Future" when a without_cancel view is dropped
# before its original resolves (the upstream bug PAGI::Server's POD notes).
my @warnings;
$SIG{__WARN__} = sub { push @warnings, @_ };

my $loop = IO::Async::Loop->new;
my @sse_log;
my @sse_body_log;

# Races $receive against a 0.1s timer and says whether the receive lost.
async sub race_and_lose {
    my ($receive) = @_;
    my $first = $receive->();
    await Future->wait_any($first, $loop->delay_future(after => 0.1));
    return $first->is_cancelled ? 'cancelled' : 'answered';
}

async sub read_body {
    my ($receive) = @_;
    my $body = '';
    while (1) {
        my $event = await $receive->();
        last unless $event->{type} eq 'http.request';
        $body .= $event->{body} // '';
        last unless $event->{more};
    }
    return $body;
}

async sub respond {
    my ($send, $out) = @_;
    await $send->({ type => 'http.response.start', status => 200,
        headers => [['content-length', length $out]] });
    await $send->({ type => 'http.response.body', body => $out, more => 0 });
}

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    my $type = $scope->{type};
    if ($type eq 'lifespan') {
        while (1) {
            my $event = await $receive->();
            if ($event->{type} eq 'lifespan.startup') {
                await $send->({ type => 'lifespan.startup.complete' });
            }
            elsif ($event->{type} eq 'lifespan.shutdown') {
                await $send->({ type => 'lifespan.shutdown.complete' });
                last;
            }
        }
        return;
    }
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
        push @sse_body_log, "first receive $outcome; got $event->{type} body '" . ($event->{body} // '') . "'";
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

    my $path = $scope->{path};
    if ($path eq '/two') {
        # Two receives parked; the app gives up on one of them.
        my ($f1, $f2) = ($receive->(), $receive->());
        $f1->cancel;
        my $event = await $f2;
        await respond($send, "other receive got '" . ($event->{body} // $event->{type}) . "'");
        return;
    }
    if ($path eq '/after-body') {
        # The body is in; a receive now waits for the client to leave.
        my $body = await read_body($receive);
        my $outcome = await race_and_lose($receive);
        my $later = $receive->();
        await Future->wait_any($later, $loop->delay_future(after => 0.3));
        my $state = $later->is_cancelled ? 'still waiting' : 'answered at once';
        await respond($send, "first receive $outcome; later receive $state; body '$body'");
        return;
    }
    my $outcome = await race_and_lose($receive);         # /body, /chunked
    my $body = await read_body($receive);
    await respond($send, "first receive $outcome; body '$body'");
};

my @errors;
my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, timeout => 5,
    access_log => undef, logger => sub { push @errors, $_[0]{message} if $_[0]{level} eq 'error' });
$loop->add($server);
$server->listen->get;

sub client {
    my $s = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port,
        Proto => 'tcp', Timeout => 5) or die "connect failed: $!";
    $s->blocking(0);
    return $s;
}

# Runs the loop for up to $seconds, collecting what arrives on $s, until
# $done->($received) is true or the peer closes.
sub pump {
    my ($s, $seconds, $done) = @_;
    my $got = '';
    my $deadline = time + $seconds;
    while (time < $deadline) {
        $loop->loop_once(0.02);
        my $n = sysread($s, my $buf, 65536);
        last if defined $n && $n == 0;
        $got .= $buf if $n;
        last if $done && $done->($got);
    }
    return $got;
}

# Sends $head, lets the app's timer win, sends $body, returns the response.
sub request {
    my ($head, $body) = @_;
    my $s = client();
    print $s $head;
    my $got = pump($s, 0.3);
    syswrite($s, $body) if length $body;
    $got .= pump($s, 3, sub { $_[0] =~ /\r\n\r\n.*'\z/s });
    close $s;
    return $got;
}

subtest 'HTTP/1.1: a receive after a cancelled one gets the body' => sub {
    my $got = request("POST /body HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\n", 'body');
    like($got, qr{\AHTTP/1\.1 200 }, 'the request succeeds');
    like($got, qr{first receive cancelled; body 'body'\z}, 'the later receive got the body');
};

subtest 'HTTP/1.1 chunked: a receive after a cancelled one gets the body' => sub {
    my $got = request("POST /chunked HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n",
        "4\r\nbody\r\n0\r\n\r\n");
    like($got, qr{\AHTTP/1\.1 200 }, 'the request succeeds');
    like($got, qr{first receive cancelled; body 'body'\z}, 'the later receive got the body');
};

subtest 'HTTP/1.1 chunked, a chunk split across reads: the later receive gets it' => sub {
    # Half of the chunk is here, so the first receive parks waiting for the rest.
    my $got = request("POST /chunked HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nbo",
        "dy\r\n0\r\n\r\n");
    like($got, qr{\AHTTP/1\.1 200 }, 'the request succeeds');
    like($got, qr{first receive cancelled; body 'body'\z}, 'the later receive got the whole body');
};

subtest 'HTTP/1.1 after the body: a later receive still waits for the client' => sub {
    my $s = client();
    print $s "POST /after-body HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\nbody";
    my $got = pump($s, 3, sub { $_[0] =~ /\r\n\r\n.*'\z/s });
    close $s;
    like($got, qr{\AHTTP/1\.1 200 }, 'the request succeeds');
    like($got, qr{first receive cancelled; later receive still waiting; body 'body'\z},
        'the later receive parks instead of failing');
};

subtest 'two receives pending: cancelling one leaves the other' => sub {
    my $got = request("POST /two HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\n", 'body');
    like($got, qr{\AHTTP/1\.1 200 }, 'the request succeeds');
    like($got, qr{other receive got 'body'\z}, 'the other receive got the body');
};

subtest 'WebSocket: a receive after a cancelled one gets the next message' => sub {
    my $s = client();
    print $s "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    like(pump($s, 2, sub { $_[0] =~ /\r\n\r\n/ }), qr{\AHTTP/1\.1 101 }, 'the handshake is accepted');
    pump($s, 0.3);
    syswrite($s, Protocol::WebSocket::Frame->new(buffer => 'hello', masked => 1)->to_bytes);
    my ($frames, $reply, $fed) = (Protocol::WebSocket::Frame->new, '', 0);
    pump($s, 3, sub {
        $frames->append(my $new = substr($_[0], $fed));    # only the bytes not yet parsed
        $fed = length $_[0];
        while (defined(my $message = $frames->next)) { $reply .= $message if $frames->is_text }
        $reply =~ /'\z/;
    });
    close $s;
    is($reply, "first receive cancelled; got 'hello'", 'the later receive got the message');
};

subtest 'SSE: a receive after a cancelled one gets the disconnect' => sub {
    my $s = client();
    print $s "GET /events HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n";
    like(pump($s, 0.5), qr{\AHTTP/1\.1 200 }, 'the stream starts');
    close $s;                                            # the client leaves
    $loop->loop_once(0.05) for 1 .. 20;
    is(\@sse_log, ['first receive cancelled', 'second receive: sse.disconnect'],
        'the later receive got the disconnect');
};

subtest 'SSE with a chunked request body: a receive after a cancelled one gets the body' => sub {
    my $s = client();
    print $s "POST /events-body HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n"
        . "Transfer-Encoding: chunked\r\n\r\n";
    my $got = pump($s, 0.3);
    syswrite($s, "4\r\nbody\r\n0\r\n\r\n");
    $got .= pump($s, 3);
    close $s;
    like($got, qr{\AHTTP/1\.1 200 }, 'the stream starts');
    is(\@sse_body_log, ["first receive cancelled; got sse.request body 'body'"],
        'the later receive got the body');
};

is(\@errors, [], 'no application error was logged');

$loop->remove($server);
$loop->loop_once(0.05) for 1 .. 5;
my @unexpected = grep {
    !($] < 5.024 && /lost its returning future/) && !($INC{'Future/XS.pm'} && /lost a sequence Future/)
} @warnings;
is(\@unexpected, [], 'no unexpected warnings');
done_testing;
