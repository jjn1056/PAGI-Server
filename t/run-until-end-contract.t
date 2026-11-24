#!/usr/bin/env perl

# PAGI::Spec::Www 0.7, "Connection Object Interface": run_until_end($code).
# Each case asserts only what the spec promises, through the public
# connection object and real requests to a real server, so the file can move
# into a conformance suite for any PAGI server.

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future;
use Future::AsyncAwait;
use Scalar::Util qw(weaken);
use Time::HiRes qw(time);

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $NOT_A_FUTURE = "run_until_end's callback must return a Future, the work to stop when the scope ends (an async sub returns one)\n";
my $CANCELLED    = "the work given to run_until_end was cancelled while the scope was still open\n";

my $loop = IO::Async::Loop->new;
my (%obs, @log);

sub pause { $loop->delay_future(after => $_[0]) }
sub start { $_[0]->({ type => 'http.response.start', status => 200, headers => [ [ 'content-type', 'text/plain' ] ] }) }
sub finish { $_[0]->({ type => 'http.response.body', body => $_[1] // "done\n", more => 0 }) }

# One case per path. Each records what it saw in $obs{$case}.
my %case;

$case{results} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    $obs{results} = [ await $conn->run_until_end(async sub { (7, 'seven') }) ];
    await finish($send);
};

$case{'end-cancels'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    await $send->({ type => 'http.response.body', body => "started\n", more => 1 });
    my $work;
    my @r = await $conn->run_until_end(sub { $work = pause(30) });
    $obs{'end-cancels'} = { result => \@r, cancelled => $work->is_cancelled ? 1 : 0,
                            connected => $conn->is_connected, reason => $conn->disconnect_reason };
};

$case{'clean-end-cancels'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    my $work;
    my @r = await $conn->run_until_end(sub {
        $work = (async sub { await pause(0.05); await finish($send, "last\n"); await pause(30) })->();   # the end arrives after the call is pending
    });
    $obs{'clean-end-cancels'} = { result => \@r, cancelled => $work->is_cancelled ? 1 : 0,
                                  connected => $conn->is_connected, reason => $conn->disconnect_reason };
};

sub caught { my ($f) = @_; return eval { $f->get; 1 } ? 'no error' : $@ }

$case{'work-fails'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    my $call = $conn->run_until_end(async sub { await pause(0.05); die "database went away\n" });
    await $call->else(sub { Future->done });
    $obs{'work-fails'} = caught($call);
    await finish($send);
};

$case{'code-dies'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    my $call = $conn->run_until_end(sub { die "no database handle\n" });
    await $call->else(sub { Future->done });
    $obs{'code-dies'} = caught($call);
    await finish($send);
};

$case{'not-a-future'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    my $call = $conn->run_until_end(sub { 42 });
    await $call->else(sub { Future->done });
    $obs{'not-a-future'} = caught($call);
    await finish($send);
};

$case{'outside-cancel'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    my $work = pause(10);
    my $t0   = time;
    pause(0.3)->on_done(sub { $work->cancel })->retain;
    my $call = $conn->run_until_end(sub { $work });
    await $call->else(sub { Future->done });
    $obs{'outside-cancel'} = { error => caught($call), secs => time - $t0 };
    await finish($send);
};

$case{'caller-cancel'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    my $work;
    my $call = $conn->run_until_end(sub { $work = pause(30) });
    await pause(0.2);
    $call->cancel;
    $obs{'caller-cancel'} = { cancelled => $work->is_cancelled ? 1 : 0, connected => $conn->is_connected };
    await finish($send, "still serving\n");
};

$case{'already-ended'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    await finish($send);
    my $ran  = 0;
    my $call = $conn->run_until_end(sub { $ran = 1; Future->done('ran') });
    my $ready_in_call = $call->is_ready ? 1 : 0;
    my @r = await $call;
    $obs{'already-ended'} = { ran => $ran, ready_in_call => $ready_in_call, result => \@r };
};

$case{'abort-done'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    $obs{'abort-done'} = [ await $conn->run_until_end(sub { $conn->abort('stopping'); Future->done(42) }) ];
};

$case{'abort-fail'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    my $call = $conn->run_until_end(sub { $conn->abort('stopping'); Future->fail("database failed\n") });
    await $call->else(sub { Future->done });
    $obs{'abort-fail'} = caught($call);
};

$case{'abort-pending'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    my $work;
    my @r = await $conn->run_until_end(sub { $conn->abort('stopping'); $work = pause(30) });
    $obs{'abort-pending'} = { result => \@r, cancelled => $work->is_cancelled ? 1 : 0 };
};

$case{'from-on-end'} = async sub {
    my ($conn, $send) = @_;
    $conn->on_end(sub {
        my $ran  = 0;
        my $call = $conn->run_until_end(sub { $ran = 1; Future->done });
        my $ready_in_call = $call->is_ready ? 1 : 0;
        $call->on_done(sub { $obs{'from-on-end'} = { ran => $ran, ready_in_call => $ready_in_call, result => [@_] } })->retain;
    });
    await start($send);
    await finish($send);
};

$case{release} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    # Weak references only: once a call settles, nothing but the server could
    # keep its $code or work alive. Checked a turn later, since the work is
    # finished from a loop callback that the loop still holds on that turn.
    my @weak;
    for my $i (1 .. 1000) {
        my $work = Future->new;
        my $code = sub { $loop->later(sub { $work->done($i) }); $work };
        push @weak, $code, $work;
        weaken($weak[-1]); weaken($weak[-2]);
        await $conn->run_until_end($code);
    }
    await pause(0.01);
    $obs{release} = { held => scalar(grep { defined } @weak), connected => $conn->is_connected };
    await finish($send);
};

# Application code that dies while the end is delivered -- a work's
# on_cancel, a call's on_done -- must not keep the other calls, on_end or
# end_future from settling.
$case{'callback-dies'} = async sub {
    my ($conn, $send) = @_;
    await start($send);
    await $send->({ type => 'http.response.body', body => "started\n", more => 1 });
    my %seen;
    $conn->on_end(sub { $seen{on_end} = 1 });
    $conn->end_future->on_done(sub { $seen{end_future} = 1 })->retain;
    my $exploding_work = pause(30);
    $exploding_work->on_cancel(sub { die "on_cancel exploded\n" });
    my $first  = $conn->run_until_end(sub { $exploding_work });
    my $second = $conn->run_until_end(sub { pause(30) });
    $second->on_done(sub { die "on_done exploded\n" });
    my @third = await $conn->run_until_end(sub { pause(30) });
    await pause(0.05);
    $obs{'callback-dies'} = { %seen, third => \@third,
        first  => [ $first->is_done ? $first->get : 'not done' ],
        second => [ $second->is_done ? $second->get : 'not done' ] };
};

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'http';
    my $case = $case{ $scope->{path} =~ s{^/}{}r } or die "no case $scope->{path}\n";
    await $case->($scope->{'pagi.connection'}, $send);
};

my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
    shutdown_timeout => 1, logger => sub { push @log, $_[0] },
);
$loop->add($server);
$server->listen->get;

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 10);
    $loop->loop_once(0.01) until $cond->() || time > $deadline;
    return $cond->();
}

# Request PATH; read the whole response, or leave once the first body bytes
# arrive when $leave is set.
sub request {
    my ($path, $leave) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2)
        or die "connect: $!";
    $sock->blocking(0);
    syswrite $sock, "GET /$path HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    my $got = '';
    pump_until(sub {
        my $n = sysread($sock, my $buf, 65536);
        $got .= $buf if $n;
        return 1 if defined $n && $n == 0;              # the server closed
        return $leave && $got =~ /\r\n\r\n.*\S/s;      # body bytes arrived
    });
    close $sock;
    return $got;
}

subtest 'the work finishes: its results come back' => sub {
    like request('results'), qr/done/, 'response completed';
    pump_until(sub { $obs{results} });
    is $obs{results}, [ 7, 'seven' ], 'both results';
};

subtest 'the client leaves: the work is cancelled before the call resolves' => sub {
    request('end-cancels', 1);
    ok pump_until(sub { $obs{'end-cancels'} }), 'the call returned';
    is $obs{'end-cancels'}, { result => [], cancelled => 1, connected => 0, reason => 'client_closed' },
        'empty list; the work already cancelled on the first line after await; the end reported';
};

subtest 'a clean end cancels pending work too' => sub {
    like request('clean-end-cancels'), qr/last/, 'the terminal body went';
    ok pump_until(sub { $obs{'clean-end-cancels'} }), 'the call returned';
    is $obs{'clean-end-cancels'}, { result => [], cancelled => 1, connected => 0, reason => undef },
        'empty list; cancelled on the first line after await; a clean end';
};

subtest 'failures keep their error' => sub {
    request($_) for qw(work-fails code-dies not-a-future);
    pump_until(sub { exists $obs{'not-a-future'} });
    is $obs{'work-fails'},   "database went away\n", 'the work failing';
    is $obs{'code-dies'},    "no database handle\n", '$code dying';
    is $obs{'not-a-future'}, $NOT_A_FUTURE,          'a non-Future';
};

subtest 'cancelled from outside while the scope is open: fails at once' => sub {
    request('outside-cancel');
    pump_until(sub { $obs{'outside-cancel'} });
    is $obs{'outside-cancel'}{error}, $CANCELLED, 'the cancellation, named';
    ok $obs{'outside-cancel'}{secs} < 1, 'not left waiting for the end';
};

subtest 'the caller cancels: the work is cancelled, the scope stays open' => sub {
    like request('caller-cancel'), qr/still serving/, 'the app answered afterwards';
    is $obs{'caller-cancel'}, { cancelled => 1, connected => 1 }, 'work cancelled; scope open';
};

subtest 'a call on an ended scope skips $code and resolves on a later turn' => sub {
    request('already-ended');
    pump_until(sub { $obs{'already-ended'} });
    is $obs{'already-ended'}, { ran => 0, ready_in_call => 0, result => [] }, 'skipped; later; empty';
};

subtest '$code ends its own scope' => sub {
    request($_) for qw(abort-done abort-fail abort-pending);
    pump_until(sub { $obs{'abort-pending'} });
    is $obs{'abort-done'}, [42], 'a settled Future keeps its results';
    is $obs{'abort-fail'}, "database failed\n", 'a settled failure keeps its error';
    is $obs{'abort-pending'}, { result => [], cancelled => 1 }, 'a pending Future is cancelled by the end';
};

subtest 'a call from an on_end callback' => sub {
    request('from-on-end');
    ok pump_until(sub { $obs{'from-on-end'} }), 'it resolved';
    is $obs{'from-on-end'}, { ran => 0, ready_in_call => 0, result => [] }, 'skipped; later; empty';
};

subtest 'many calls on one open scope hold nothing once settled' => sub {
    request('release');
    ok pump_until(sub { $obs{release} }, 30), 'all calls ran';
    is $obs{release}, { held => 0, connected => 1 }, 'no $code or work kept, while the scope is open';
};

subtest 'application code dying in the end delivery strands nothing' => sub {
    request('callback-dies', 1);
    ok pump_until(sub { $obs{'callback-dies'} }), 'the last call returned';
    is $obs{'callback-dies'},
        { first => [], second => [], third => [], on_end => 1, end_future => 1 },
        'every call resolved empty; on_end ran; end_future resolved';
};

$server->shutdown->get;
$loop->remove($server);
my @messages = map { $_->{message} // '' } @log;
my @exploded = grep { /\Arun_until_end .*exploded\z/ } @messages;
is scalar(@exploded), 2, 'each death in the delivery was logged' or diag join "\n", @messages;
is [ grep { !/\A(?:listening|Using default max_connections)/ && !/\Arun_until_end .*exploded\z/ } @messages ], [],
    'nothing else logged';
done_testing;
