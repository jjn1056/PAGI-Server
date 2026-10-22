#!/usr/bin/env perl

# While an application waits for HTTP/1.x request body bytes the client has
# not sent, it is the client's turn: the idle timer (timeout) runs, and
# body_min_rate counts the wait. A receive the application cancels while it
# waits (Future->wait_any cancels the loser) ends its wait, and when no other
# receive is still waiting the application has the turn back: neither clock
# charges the application's own work. The time it did wait still counts.

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use Time::HiRes qw(time);
use FindBin;
use lib "$FindBin::Bin/../lib";
use lib "$FindBin::Bin/lib";

use PAGI::Server;
use PAGITest::FutureXS qw(known_future_xs_warning);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Nothing may warn but the known warning of Future::XS before 0.16
# (RT #181389; see PAGITest::FutureXS).
my @warnings;
$SIG{__WARN__} = sub { push @warnings, @_ };

my $loop = IO::Async::Loop->new;
my @app_log;

# Races $receive against a timer of $seconds and says whether the receive
# lost. The cancelled receive is dropped on return, as an application would.
async sub race_and_lose {
    my ($receive, $seconds) = @_;
    my $first = $receive->();
    await Future->wait_any($first, $loop->delay_future(after => $seconds));
    return $first->is_cancelled ? 'cancelled' : 'answered';
}

async sub read_body {
    my ($receive) = @_;
    my ($body, $event) = ('');
    while (1) {
        $event = await $receive->();
        last unless $event->{type} =~ /\.request\z/;
        $body .= $event->{body} // '';
        last unless $event->{more};
    }
    return ($body, $event->{type});
}

my $running = 0;    # applications not yet returned; see done_serving

my $handler = async sub {
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
    if ($type eq 'sse') {
        # Reads the whole request body before it starts the stream.
        my ($body, $last) = await read_body($receive);
        push @app_log, "sse got $last";
        return if $last eq 'sse.disconnect';
        await $send->({ type => 'sse.start', status => 200, headers => [] });
        await $send->({ type => 'sse.close' });
        return;
    }

    my $path = $scope->{path};
    if ($path eq '/sibling') {
        # Two receives wait; the application gives up on one of them.
        my $kept = $receive->();
        push @app_log, 'first receive ' . await race_and_lose($receive, 0.1);
        my $event = await $kept;
        push @app_log, "kept receive got $event->{type}";
        return;
    }
    my $wait = $path eq '/fold' ? 0.5 : 0.1;
    my $work = $path eq '/fold' ? 0   : 0.6;
    my $outcome = await race_and_lose($receive, $wait);
    await $loop->delay_future(after => $work) if $work;    # the application's own work
    my ($body, $last) = await read_body($receive);
    push @app_log, "$path got $last";
    return if $last eq 'http.disconnect';
    my $out = "first receive $outcome; body '$body'";
    await $send->({ type => 'http.response.start', status => 200,
        headers => [['content-length', length $out]] });
    await $send->({ type => 'http.response.body', body => $out, more => 0 });
};

my $app = async sub {
    my ($scope) = @_;
    return await $handler->(@_) if $scope->{type} eq 'lifespan';
    $running++;
    my $ok = eval { await $handler->(@_); 1 };
    $running--;
    die $@ unless $ok;
};

my @errors;

sub serve {
    my (%opts) = @_;
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0,
        access_log => undef, %opts,
        logger => sub { push @errors, $_[0]{message} if $_[0]{level} eq 'error' });
    $loop->add($server);
    $server->listen->get;
    return $server;
}

# Waits for every application to return -- one may work on after its client
# has gone -- so nothing it logs lands in the next subtest.
sub done_serving {
    my ($server) = @_;
    my $deadline = time + 3;
    $loop->loop_once(0.02) while $running && time < $deadline;
    $loop->remove($server);
    $loop->loop_once(0.05) for 1 .. 3;
}

# Sends $head, then each [$at, $bytes] in @sends at $at seconds, collecting the
# response until the peer closes or $limit seconds pass. Returns what arrived
# and when the peer closed (undef if it did not).
sub exchange {
    my ($server, $head, $limit, @sends) = @_;
    my $s = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port,
        Proto => 'tcp', Timeout => 5) or die "connect failed: $!";
    $s->blocking(0);
    print $s $head;
    my ($got, $closed, $t0) = ('', undef, time);
    while (time - $t0 < $limit) {
        $loop->loop_once(0.01);
        while (@sends && time - $t0 >= $sends[0][0]) {
            syswrite($s, (shift @sends)->[1]);
        }
        my $n = sysread($s, my $buf, 65536);
        if (defined $n && $n == 0) { $closed = time - $t0; last }
        $got .= $buf if $n;
        last if $got =~ /\r\n\r\n.*'\z/s;
    }
    close $s;
    return ($got, $closed);
}

subtest 'timeout: the idle timer does not charge the work after a cancelled receive' => sub {
    @app_log = ();
    my $server = serve(timeout => 0.3);
    my ($got) = exchange($server, "POST /idle HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\n",
        3, [0.8, 'body']);
    done_serving($server);
    like($got, qr{\AHTTP/1\.1 200 }, 'the request succeeds');
    like($got, qr{first receive cancelled; body 'body'\z}, 'the body reached the application');
};

subtest 'timeout, chunked body: the same' => sub {
    @app_log = ();
    my $server = serve(timeout => 0.3);
    my ($got) = exchange($server, "POST /chunked HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n",
        3, [0.8, "4\r\nbody\r\n0\r\n\r\n"]);
    done_serving($server);
    like($got, qr{\AHTTP/1\.1 200 }, 'the request succeeds');
    like($got, qr{first receive cancelled; body 'body'\z}, 'the body reached the application');
};

subtest 'body_min_rate: body bytes arriving during the work are not charged to the client' => sub {
    @app_log = ();
    my $server = serve(timeout => 5, body_min_rate => 1000, body_rate_grace => 0.3);
    my ($got) = exchange($server, "POST /rate HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\n",
        3, [0.4, 'body']);
    done_serving($server);
    like($got, qr{\AHTTP/1\.1 200 }, 'the request succeeds');
    like($got, qr{first receive cancelled; body 'body'\z}, 'the body reached the application');
};

subtest 'body_min_rate: time waited before a cancel still counts' => sub {
    # 0.5s waited, cancelled, then waiting again: 0.6s in all is past the
    # 0.3s grace with 4 bytes, so the body is too slow.
    @app_log = ();
    my $server = serve(timeout => 5, body_min_rate => 1000, body_rate_grace => 0.3);
    my ($got) = exchange($server, "POST /fold HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\n",
        3, [0.6, 'body']);
    done_serving($server);
    like($got, qr{\AHTTP/1\.1 408 }, 'the request times out');
    is(\@app_log, ['/fold got http.disconnect'], 'the application is told the client is gone');
};

subtest 'a receive still waiting keeps the turn with the client' => sub {
    @app_log = ();
    my $server = serve(timeout => 0.3);
    my ($got, $closed) = exchange($server, "POST /sibling HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\n", 2);
    done_serving($server);
    ok(defined $closed && $closed < 1, 'the idle timer still ends the connection')
        or diag('closed at ' . ($closed // 'never'));
    is(\@app_log, ['first receive cancelled', 'kept receive got http.disconnect'],
        'the receive still waiting gets the disconnect');
};

subtest 'body_min_rate: a trickled SSE Content-Length body is too slow, as a chunked one is' => sub {
    @app_log = ();
    my $server = serve(timeout => 5, body_min_rate => 1000, body_rate_grace => 0.3);
    my @trickle = map { [0.25 * $_, 'a'] } 1 .. 8;
    my ($got, $closed) = exchange($server,
        "POST /events HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\nContent-Length: 100\r\n\r\n",
        2.5, @trickle);
    done_serving($server);
    ok(defined $closed && $closed < 1.5, 'the connection is closed')
        or diag('closed at ' . ($closed // 'never'));
    like($got, qr{\AHTTP/1\.1 408 }, 'with a 408');
    is(\@app_log, ['sse got sse.disconnect'], 'the application is told the client is gone');
};

is(\@errors, [], 'no application error was logged');

my @unexpected = grep { !known_future_xs_warning($_) } @warnings;
is(\@unexpected, [], 'no unexpected warnings');
done_testing;
