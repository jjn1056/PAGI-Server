use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use Future;
use MIME::Base64 ();
use FindBin;
use lib "$FindBin::Bin/../lib";

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Test: an HTTP/1.1 receive() made after the scope's disconnect
#       event was delivered answers with that event again
# ============================================================
# L<PAGI::Spec::Www> ("Disconnect - receive event"): "Once this event has been
# delivered the scope is over, and a further receive() resolves with the same
# websocket.disconnect again. The event reports the scope's terminal state
# rather than delivering a message, so it is never consumed by one reader and
# lost to another."
#
# On HTTP/1.1 the scope being over and the transport being gone are two
# different moments. A completed closing handshake is a clean end: the server
# answers the peer's Close, queues the scope's one websocket.disconnect, and
# leaves the TCP socket open while the application runs -- the request tail
# closes it when the application returns. A further receive() made in that
# window has no transport ending to key on, so the scope's own delivered
# disconnect is what must answer it.
#
# What is re-delivered is the event itself, not a fresh reading of the
# connection's ending record: a peer's Close frame names its own RFC code and
# its own reason TEXT, while the record's vocabulary is the standard reason
# tokens, so a rebuild would answer Close(4321, "bye") with a token instead.
#
# The HTTP/2 twin of both halves is t/http2/43; the cap that bounds
# re-delivery is t/75.

use PAGI::Server;
use Protocol::WebSocket::Frame;

my $loop = IO::Async::Loop->new;

# Every server built here logs into this collector rather than STDERR, so a
# case can assert the exact set of lines its scope produced.
my @LOG;

sub create_server {
    my (%opts) = @_;
    my $server = PAGI::Server->new(
        host => '127.0.0.1', port => 0,
        log_level => 'debug', access_log => undef, shutdown_timeout => 1,
        logger => sub { push @LOG, $_[0] },
        %opts,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub shutdown_server {
    my ($server) = @_;
    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
}

# Every log line the case's scope produced, counted by level. A case takes its
# mark once its server is up, so the count covers the scope alone and not the
# startup lines the server writes when it begins listening.
sub log_levels_since {
    my ($mark) = @_;
    my %by_level;
    $by_level{ $_->{level} }++ for @LOG[$mark .. $#LOG];
    return \%by_level;
}

sub ws_request {
    return "GET /socket HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
         . "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
         . "Sec-WebSocket-Key: "
         . MIME::Base64::encode_base64('0123456789abcdef', '') . "\r\n\r\n";
}

sub ws_connect {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5)
        or die "connect: $!";
    print $sock ws_request();
    $sock->blocking(0);
    return $sock;
}

# A masked client Close frame, as RFC 6455 requires of a client.
sub close_frame {
    my ($code, $reason) = @_;
    my $payload = defined $code ? pack('n', $code) . ($reason // '') : '';
    return Protocol::WebSocket::Frame->new(
        type => 'close', buffer => $payload, masked => 1)->to_bytes;
}

# Whether the client still has a connection, after draining whatever the
# server has written so far: a non-blocking sysread answers undef (EAGAIN) on
# a live socket and 0 at EOF.
sub client_socket_state {
    my ($sock) = @_;
    while (1) {
        my $buf;
        my $n = sysread($sock, $buf, 65536);
        return 'eof'  if defined $n && $n == 0;
        return 'open' if !defined $n;
    }
}

# A regression here is a receive() that never answers, so every pump carries an
# alarm: the file must fail by assertion, never hang.
my $ALARM_FIRED = 0;

sub bounded {
    my ($body, $seconds) = @_;
    local $SIG{ALRM} = sub { $ALARM_FIRED = 1; die "pump alarm\n" };
    alarm($seconds // 20);
    my $ok  = eval { $body->(); 1 };
    my $err = $@;
    alarm(0);
    die $err if !$ok && $err ne "pump alarm\n";
    return $ALARM_FIRED;
}

sub pump_until {
    my ($cond, $rounds) = @_;
    bounded(sub {
        for (1 .. ($rounds // 200)) {
            last if $cond->();
            $loop->loop_once(0.05);
        }
    });
    return $cond->();
}

# The bound on one receive() call. A call that has not answered within it is
# parked, and reports itself as such rather than stalling the file.
my $PARK_BOUND = 1;

sub bounded_receive {
    my ($receive) = @_;
    return Future->wait_any(
        $receive->()->without_cancel,
        $loop->delay_future(after => $PARK_BOUND)
             ->then(sub { Future->done({ type => 'PARKED' }) }),
    );
}

sub reset_case {
    @LOG         = ();
    $ALARM_FIRED = 0;
}

# ============================================================
# The application shape cases 1-3 share
# ============================================================
# Accept the handshake, then make three receives: one that is already parked
# when the scope ends, one made in the same turn as that answer, and one made
# after a real suspension -- the gate the case completes from outside, which
# is also the window the case reads the client socket in.
sub three_receives_app {
    my ($obs, $gate) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;

        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { $obs->{complete}++ });

        await $receive->();                        # websocket.connect
        await $send->({ type => 'websocket.accept' });
        $obs->{accepted} = 1;

        $obs->{first}  = await bounded_receive($receive);
        $obs->{second} = await bounded_receive($receive);

        $obs->{gated} = 1;
        await $gate;

        $obs->{third} = await bounded_receive($receive);

        $obs->{disconnect_reason} = $cs->disconnect_reason;
        $obs->{returned} = 1;
        return;
    };
}

# ============================================================
# 1. A peer Close with a code and reason text of its own
# ============================================================

subtest 'peer Close(4321, "bye"): every further receive answers the same event' => sub {
    reset_case();
    my %obs;
    my $gate   = $loop->new_future;
    my $server = create_server(app => three_receives_app(\%obs, $gate));
    my $mark   = scalar @LOG;

    my $sock = ws_connect($server->port);
    ok(pump_until(sub { $obs{accepted} }), 'the handshake was accepted');

    print $sock close_frame(4321, 'bye');
    ok(pump_until(sub { $obs{gated} }), 'two receives answered without parking');

    is($obs{first}, { type => 'websocket.disconnect', code => 4321, reason => 'bye' },
        'the parked receive got the peer\'s own close code and reason text');
    is($obs{second}, $obs{first},
        'a receive made in the same turn answered with an equal event');

    # The scope is over, but the transport is not: the request tail closes the
    # socket when the application returns, which has not happened yet.
    is(client_socket_state($sock), 'open',
        'the client socket is still open while the application runs on');

    $gate->done;
    ok(pump_until(sub { $obs{returned} }), 'the application returned');

    is($obs{third}, $obs{first},
        'a receive made after a yielded tick answered with an equal event');
    is($obs{complete}, 1, 'on_complete fired exactly once');
    is($obs{disconnect_reason}, undef,
        'a completed closing handshake is a clean end, not a disconnect');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    close $sock;
    shutdown_server($server);
};

# ============================================================
# 2. A peer Close with no payload at all
# ============================================================
# RFC 6455: an empty Close payload carries no status, which the application
# sees as 1005 with an empty reason. The record cannot produce that pair
# either -- its no-status default is the abnormal 1006 -- so this is the
# second reading the kept event has to preserve.

subtest 'peer Close with no payload: 1005 and an empty reason, re-delivered' => sub {
    reset_case();
    my %obs;
    my $gate   = $loop->new_future;
    my $server = create_server(app => three_receives_app(\%obs, $gate));
    my $mark   = scalar @LOG;

    my $sock = ws_connect($server->port);
    ok(pump_until(sub { $obs{accepted} }), 'the handshake was accepted');

    print $sock close_frame(undef, undef);
    ok(pump_until(sub { $obs{gated} }), 'two receives answered without parking');

    is($obs{first}, { type => 'websocket.disconnect', code => 1005, reason => '' },
        'the parked receive got the no-status close');
    is($obs{second}, $obs{first},
        'a receive made in the same turn answered with an equal event');
    is(client_socket_state($sock), 'open',
        'the client socket is still open while the application runs on');

    $gate->done;
    ok(pump_until(sub { $obs{returned} }), 'the application returned');

    is($obs{third}, $obs{first},
        'a receive made after a yielded tick answered with an equal event');
    is($obs{complete}, 1, 'on_complete fired exactly once');
    is($obs{disconnect_reason}, undef,
        'a completed closing handshake is a clean end, not a disconnect');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    close $sock;
    shutdown_server($server);
};

# ============================================================
# 3. An abrupt drop, with no Close frame at all
# ============================================================
# The other ending: the transport goes away, so the event is the server's own
# reading of an abnormal close. The scope must still answer a further receive
# with the same event, and the reason must still agree with the token the
# connection object reports (t/61 accepts the same alternatives -- an RST
# surfaces as read_error rather than the plain client_closed).

subtest 'abrupt drop: the further receive answers the same abnormal event' => sub {
    reset_case();
    my %obs;
    my $gate   = $loop->new_future;
    my $server = create_server(app => three_receives_app(\%obs, $gate));
    my $mark   = scalar @LOG;

    my $sock = ws_connect($server->port);
    ok(pump_until(sub { $obs{accepted} }), 'the handshake was accepted');

    close $sock;
    ok(pump_until(sub { $obs{gated} }), 'two receives answered without parking');

    is($obs{first}{type}, 'websocket.disconnect', 'the parked receive got the disconnect');
    is($obs{first}{code}, 1006, 'an abnormal close with no status received');
    like($obs{first}{reason}, qr/^(client_closed|read_error)$/,
        'the reason names the abnormal end');
    is($obs{second}, $obs{first},
        'a receive made in the same turn answered with an equal event');

    $gate->done;
    ok(pump_until(sub { $obs{returned} }), 'the application returned');

    is($obs{third}, $obs{first},
        'a receive made after a yielded tick answered with an equal event');
    is($obs{complete}, undef, 'an abnormal end is not a completion');
    is($obs{disconnect_reason}, $obs{first}{reason},
        'the event and the connection object name the same end');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
    is(log_levels_since($mark), {}, 'the scope logged nothing at any level');

    shutdown_server($server);
};

# ============================================================
# 4. The scope's event is queued exactly once
# ============================================================
# The same property t/http2/31 pins on HTTP/2. An answer shifted off the
# receive queue is an ordinary delivery and is uncounted, while a synthesized
# one counts against max_disconnect_receives. With the cap at 1, a second
# queued copy of the event would make the second further receive the first
# counted one, and it would resolve instead of failing -- so the cap is what
# proves the queue holds one copy and one only.

subtest 'the delivered event is queued once: the cap counts the re-deliveries' => sub {
    reset_case();
    my %obs;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;

        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { $obs{complete}++ });

        await $receive->();                        # websocket.connect
        await $send->({ type => 'websocket.accept' });
        $obs{accepted} = 1;

        $obs{first}  = await bounded_receive($receive);
        $obs{second} = await bounded_receive($receive);

        my $ok = eval { $obs{third} = await bounded_receive($receive); 1 };
        $obs{failure} = $ok ? '' : "$@";

        $obs{returned} = 1;
        return;
    };
    my $server = create_server(app => $app, max_disconnect_receives => 1);
    my $mark   = scalar @LOG;

    my $sock = ws_connect($server->port);
    ok(pump_until(sub { $obs{accepted} }), 'the handshake was accepted');

    print $sock close_frame(4321, 'bye');
    ok(pump_until(sub { $obs{returned} }), 'the application returned');

    is($obs{first}, { type => 'websocket.disconnect', code => 4321, reason => 'bye' },
        'the parked receive got the queued event, uncounted');
    is($obs{second}, $obs{first},
        'the first further receive answered with an equal event, the cap\'s one');
    is($obs{third}, undef, 'the second further receive did not resolve');

    my $message = "receive() called 2 times after the scope's disconnect event; "
                . "the application is not checking for it "
                . "(PAGI::Server max_disconnect_receives=1)";
    is($obs{failure}, "$message\n", 'it failed with the cap message');

    is([map { $_->{message} } grep { $_->{level} eq 'error' } @LOG[$mark .. $#LOG]],
        [ "websocket scope on HTTP/1.1: $message" ],
        'the cap logged one error line');
    is(log_levels_since($mark), { error => 1 },
        'that line is everything the scope logged, at any level');
    is($obs{complete}, 1, 'on_complete fired exactly once');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');

    close $sock;
    shutdown_server($server);
};

done_testing;
