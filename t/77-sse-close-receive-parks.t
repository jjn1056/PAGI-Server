use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Future::AsyncAwait;
use Future;
use Socket qw(AF_UNIX SOCK_STREAM);
use Scalar::Util qw(weaken);
use FindBin;
use lib "$FindBin::Bin/../lib";

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Test: a receive() after the application's own sse.close parks
# ============================================================
# L<PAGI::Spec::Www> ("SSE Disconnect - receive event") names the two things
# that deliver sse.disconnect: the client disconnecting, and the server
# shutting down a started event stream. The application ending its own stream
# with sse.close is neither -- it is this scope's clean end ("Meaning per
# scope") -- and "Agreement with disconnect events" binds the event's reason
# to the token the connection object was marked with, which after a clean end
# is no token at all. A receive() made after sse.close, or already parked when
# it is sent, therefore has no event left to answer it: it parks, exactly as a
# receive() after a completed refusal does.
#
# The control is the other half of the same rule: a stream the transport takes
# away still delivers sse.disconnect, with the reason the scope recorded.
#
# The retention assertions (the parked Future, the application coroutine and
# the scope are all released when the application returns) use t/73's
# technique; the park bound and the alarmed pump are t/76's. Kept in its own
# file because t/73 drives every case through a refusal that never starts a
# stream, and every case here starts one.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

my $have_h2 = do {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available ? 1 : 0;
};

# Bumps a counter when it is collected, so a scalar can stand in for "this
# thing was freed" (t/73).
package RetentionGuard {
    sub new     { my ($class, $flag) = @_; return bless { flag => $flag }, $class }
    sub DESTROY { ${ $_[0]{flag} }++ }
}

# Every server built here logs into this collector rather than STDERR, so a
# case can assert its scope produced no complaint.
my @LOG;

sub logged_at_warn_or_above {
    my ($mark) = @_;
    my %rank = (warn => 1, error => 1, fatal => 1);
    return [map { "$_->{level}: $_->{message}" }
            grep { $rank{ $_->{level} // '' } } @LOG[$mark .. $#LOG]];
}

# A regression here is a receive() that never answers, so every pump carries an
# alarm: the file must fail by assertion, never hang.
my $ALARM_FIRED = 0;

sub bounded {
    my ($body, $seconds) = @_;
    local $SIG{ALRM} = sub { $ALARM_FIRED = 1; die "pump alarm\n" };
    alarm($seconds // 30);
    my $ok  = eval { $body->(); 1 };
    my $err = $@;
    alarm(0);
    die $err if !$ok && $err ne "pump alarm\n";
    return $ALARM_FIRED;
}

# The bound on one receive() call. A call that has not answered within it is
# parked, and says so instead of stalling the file.
my $PARK_BOUND = 1;

sub bounded_wait {
    my ($future) = @_;
    return Future->wait_any(
        $future->without_cancel,
        $loop->delay_future(after => $PARK_BOUND)
             ->then(sub { Future->done({ type => 'PARKED' }) }),
    );
}

# ============================================================
# The application every case runs
# ============================================================
# It starts a stream and sends one event, then differs only in when it makes
# the receive() under test:
#
#   after_close   sse.close, then receive()
#   while_parked  receive(), then sse.close from the same coroutine's next
#                 send -- the call is already outstanding when the stream ends
#   client_drop   no sse.close at all: the control, where the transport goes
#                 away under a parked receive()
sub sse_app {
    my ($case, $obs) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'sse';

        my $app_guard = RetentionGuard->new(\$obs->{app_freed}); # freed with the coroutine
        $obs->{scope_weak} = $scope;
        weaken($obs->{scope_weak});

        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { $obs->{on_complete}++ });
        $cs->on_disconnect(sub { $obs->{on_disconnect} = $_[0] // 'undef' });

        await $receive->();                                      # sse.request
        await $send->({ type => 'sse.start', status => 200, headers => [] });
        await $send->({ type => 'sse.send', data => 'hi' });

        my $under_test;
        if ($case eq 'while_parked') {
            $under_test = $receive->();                          # parked first
            my $rg = RetentionGuard->new(\$obs->{receive_freed});
            $under_test->on_ready(sub { my $keep = $rg });       # lives as long as it does
            await $send->({ type => 'sse.close' });
        }
        elsif ($case eq 'after_close') {
            await $send->({ type => 'sse.close' });
            $under_test = $receive->();
            my $rg = RetentionGuard->new(\$obs->{receive_freed});
            $under_test->on_ready(sub { my $keep = $rg });
        }
        else {
            $under_test = $receive->();
            my $rg = RetentionGuard->new(\$obs->{receive_freed});
            $under_test->on_ready(sub { my $keep = $rg });
        }

        $obs->{answer} = await bounded_wait($under_test);
        $obs->{pending} = $under_test->is_ready ? 0 : 1;
        $obs->{object} = {
            is_connected      => $cs->is_connected ? 1 : 0,
            response_complete => $cs->response_complete ? 1 : 0,
            disconnect_reason => $cs->disconnect_reason,
        };
        $obs->{returned} = 1;
        return;
    };
}

sub describe {
    my ($event) = @_;
    return 'no answer' unless ref $event eq 'HASH';
    my $type = $event->{type} // '';
    return $type . (defined $event->{reason} ? " reason=$event->{reason}" : '');
}

# ============================================================
# HTTP/1.1 harness
# ============================================================

sub h1_run {
    my ($case) = @_;
    my %obs;
    my $mark = scalar @LOG;

    my $server = PAGI::Server->new(
        app => sse_app($case, \%obs), host => '127.0.0.1', port => 0,
        log_level => 'debug', access_log => undef, shutdown_timeout => 1,
        logger => sub { push @LOG, $_[0] },
    );
    $loop->add($server);
    $server->listen->get;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 5)
        or die "connect: $!";
    print $sock "GET /events HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n";
    $sock->blocking(0);

    my $wire = '';
    my $read = sub {
        while (1) {
            my $buf;
            my $n = sysread($sock, $buf, 65536);
            last if !defined $n || $n == 0;
            $wire .= $buf;
        }
    };

    # The client drops as soon as the stream is running, for the control case.
    # The pump runs on past the application's return, so the wire holds
    # everything the server wrote rather than stopping at whatever the last
    # turn happened to flush.
    my $dropped = 0;
    my $after_return = 0;
    bounded(sub {
        for (1 .. 200) {
            $loop->loop_once(0.05);
            $read->() unless $dropped;
            if ($case eq 'client_drop' && !$dropped && $wire =~ /data: hi/) {
                close $sock;
                $dropped = 1;
            }
            last if $obs{returned} && ++$after_return > 20;
        }
    });

    my %before = %obs;
    close $sock unless $dropped;
    bounded(sub { $loop->loop_once(0.05) for 1 .. 40 });

    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
    return (\%before, \%obs, $wire, $mark);
}

# ============================================================
# HTTP/2 harness (shape borrowed from t/71 and t/73)
# ============================================================

sub h2_run {
    my ($case) = @_;
    my %obs;
    my $mark = scalar @LOG;

    my $app = sse_app($case, \%obs);
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, http2 => 1,
        log_level => 'debug', access_log => undef,
        logger => sub { push @LOG, $_[0] },
    );
    $loop->add($server);

    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $_->blocking(0) for $sock_a, $sock_b;
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;

    require Net::HTTP2::nghttp2::Session;
    my ($body, $close_code) = ('', undef);
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { 0 },
        on_frame_recv      => sub { 0 },
        on_data_chunk_recv => sub { my (undef, $d) = @_; $body .= $d; 0 },
        on_stream_close    => sub { my (undef, $c) = @_; $close_code = $c; 0 },
    });

    $loop->loop_once(0.1);
    my $settings = ''; $sock_b->sysread($settings, 4096);
    $client->send_connection_preface;
    $sock_b->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($settings);
    $loop->loop_once(0.1);
    my $ack = ''; $sock_b->sysread($ack, 4096);
    $client->mem_recv($ack) if length $ack;
    my $out = $client->mem_send; $sock_b->syswrite($out) if length $out;
    $loop->loop_once(0.1);
    my $extra = ''; $sock_b->sysread($extra, 4096);
    $client->mem_recv($extra) if length $extra;

    $client->submit_request(method => 'GET', path => '/events', scheme => 'http',
        authority => 'localhost', headers => [['accept', 'text/event-stream']]);
    $sock_b->syswrite($client->mem_send);

    my $dropped = 0;
    my $after_return = 0;
    bounded(sub {
        for (1 .. 200) {
            $loop->loop_once(0.05);
            my $buf = '';
            $sock_b->sysread($buf, 65536) unless $dropped;
            $client->mem_recv($buf) if length $buf;
            my $o = $client->mem_send;
            $sock_b->syswrite($o) if length $o && !$dropped;
            if ($case eq 'client_drop' && !$dropped && $body =~ /data: hi/) {
                close $sock_b;
                $dropped = 1;
            }
            last if $obs{returned} && ++$after_return > 20;
        }
    });

    my %before = %obs;
    close $sock_b unless $dropped;
    bounded(sub { $loop->loop_once(0.05) for 1 .. 40 });

    $stream->close_now;
    eval { $loop->remove($server) };
    undef $conn;
    bounded(sub { $loop->loop_once(0.05) for 1 .. 10 });

    return (\%before, \%obs, $body, $close_code, $mark);
}

sub freed { my ($obs, $key) = @_; return $obs->{$key} // 0 }
sub scope_alive { my ($obs) = @_; return defined $obs->{scope_weak} ? 1 : 0 }

# ============================================================
# 1. A receive() made after the application's own sse.close
# ============================================================

subtest 'h1: a receive after the app closed its own stream parks' => sub {
    my ($before, $after, $wire, $mark) = h1_run('after_close');

    is($before->{returned}, 1, 'the application ran to the end');
    is(describe($before->{answer}), 'PARKED', 'the receive was still pending when the bound expired');
    is($before->{pending}, 1, 'and its Future had not resolved');

    is($before->{object}, {
        is_connected => 0, response_complete => 1, disconnect_reason => undef,
    }, 'the object reads a clean end');
    is($before->{on_complete}, 1, 'on_complete fired exactly once');
    is($before->{on_disconnect}, undef, 'on_disconnect never fired');

    like($wire, qr/data: hi/, 'the client received the event');
    like($wire, qr/\r\n0\r\n\r\n\z/, 'and the chunked terminator ended the stream cleanly');
    is(logged_at_warn_or_above($mark), [], 'nothing was logged');

    is(freed($after, 'app_freed'), 1, 'the application coroutine was collected');
    is(freed($after, 'receive_freed'), 1, 'the parked receive Future was collected');
    is(scope_alive($after), 0, 'the scope hash is gone');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

subtest 'h2: a receive after the app closed its own stream parks' => sub {
    skip_all 'HTTP/2 not available' unless $have_h2;
    my ($before, $after, $body, $close_code, $mark) = h2_run('after_close');

    is($before->{returned}, 1, 'the application ran to the end');
    is(describe($before->{answer}), 'PARKED', 'the receive was still pending when the bound expired');
    is($before->{pending}, 1, 'and its Future had not resolved');

    is($before->{object}, {
        is_connected => 0, response_complete => 1, disconnect_reason => undef,
    }, 'the object reads a clean end');
    is($before->{on_complete}, 1, 'on_complete fired exactly once');
    is($before->{on_disconnect}, undef, 'on_disconnect never fired');

    like($body, qr/data: hi/, 'the client received the event');
    is($close_code, 0, 'and the stream ended with END_STREAM, no error code');
    is(logged_at_warn_or_above($mark), [], 'nothing was logged');

    is(freed($after, 'app_freed'), 1, 'the application coroutine was collected');
    is(freed($after, 'receive_freed'), 1, 'the parked receive Future was collected');
    is(scope_alive($after), 0, 'the scope hash is gone');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

# ============================================================
# 2. A receive() already outstanding when sse.close is sent
# ============================================================

subtest 'h1: a receive outstanding when the app sends sse.close stays parked' => sub {
    my ($before, $after, $wire, $mark) = h1_run('while_parked');

    is($before->{returned}, 1, 'the application ran to the end');
    is(describe($before->{answer}), 'PARKED', 'the outstanding receive never answered');
    is($before->{pending}, 1, 'and its Future had not resolved');

    is($before->{object}, {
        is_connected => 0, response_complete => 1, disconnect_reason => undef,
    }, 'the object reads a clean end');
    is($before->{on_complete}, 1, 'on_complete fired exactly once');
    is($before->{on_disconnect}, undef, 'on_disconnect never fired');

    like($wire, qr/data: hi/, 'the client received the event');
    like($wire, qr/\r\n0\r\n\r\n\z/, 'and the chunked terminator ended the stream cleanly');
    is(logged_at_warn_or_above($mark), [], 'nothing was logged');

    is(freed($after, 'app_freed'), 1, 'the application coroutine was collected');
    is(freed($after, 'receive_freed'), 1, 'the parked receive Future was collected');
    is(scope_alive($after), 0, 'the scope hash is gone');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

subtest 'h2: a receive outstanding when the app sends sse.close stays parked' => sub {
    skip_all 'HTTP/2 not available' unless $have_h2;
    my ($before, $after, $body, $close_code, $mark) = h2_run('while_parked');

    is($before->{returned}, 1, 'the application ran to the end');
    is(describe($before->{answer}), 'PARKED', 'the outstanding receive never answered');
    is($before->{pending}, 1, 'and its Future had not resolved');

    is($before->{object}, {
        is_connected => 0, response_complete => 1, disconnect_reason => undef,
    }, 'the object reads a clean end');
    is($before->{on_complete}, 1, 'on_complete fired exactly once');
    is($before->{on_disconnect}, undef, 'on_disconnect never fired');

    like($body, qr/data: hi/, 'the client received the event');
    is($close_code, 0, 'and the stream ended with END_STREAM, no error code');
    is(logged_at_warn_or_above($mark), [], 'nothing was logged');

    is(freed($after, 'app_freed'), 1, 'the application coroutine was collected');
    is(freed($after, 'receive_freed'), 1, 'the parked receive Future was collected');
    is(scope_alive($after), 0, 'the scope hash is gone');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

# ============================================================
# 3. Control: the transport-closed path still delivers the event
# ============================================================
# The reason is whatever the scope recorded for the end, which is what t/61
# accepts for a client that drops mid-stream.

subtest 'h1: a client that drops mid-stream still delivers sse.disconnect' => sub {
    my ($before) = h1_run('client_drop');

    is($before->{returned}, 1, 'the application ran to the end');
    is($before->{answer}{type}, 'sse.disconnect', 'the parked receive answered with the event');
    like($before->{answer}{reason}, qr/^(?:client_closed|read_error)$/,
        'carrying the reason the scope recorded');
    is($before->{object}{is_connected}, 0, 'the object reports the disconnect');
    is($before->{object}{disconnect_reason}, $before->{answer}{reason},
        'and agrees with the event on the reason');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

subtest 'h2: a client that drops mid-stream still delivers sse.disconnect' => sub {
    skip_all 'HTTP/2 not available' unless $have_h2;
    my ($before) = h2_run('client_drop');

    is($before->{returned}, 1, 'the application ran to the end');
    is($before->{answer}{type}, 'sse.disconnect', 'the parked receive answered with the event');
    like($before->{answer}{reason}, qr/^(?:client_closed|read_error)$/,
        'carrying the reason the scope recorded');
    is($before->{object}{is_connected}, 0, 'the object reports the disconnect');
    is($before->{object}{disconnect_reason}, $before->{answer}{reason},
        'and agrees with the event on the reason');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

done_testing;
