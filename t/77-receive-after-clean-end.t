use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Future::AsyncAwait;
use Future;
use Socket qw(AF_UNIX SOCK_STREAM);
use MIME::Base64 ();
use Scalar::Util qw(weaken);
use FindBin;
use lib "$FindBin::Bin/../lib";

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Test: a receive after a clean end the application produced
# ============================================================
# L<PAGI::Spec::Www>, "Meaning per scope", "Receiving after the scope's end":
#
#   A $receive call made after the scope has ended reports the end; it never
#   invents data and never invents a failure. After an abnormal end, pending
#   and later receives resolve with the scope's disconnect event. After a
#   clean end the application itself produced with nothing left to deliver --
#   a completed refusal of a WebSocket handshake or an SSE stream, or
#   sse.close -- they resolve with the scope's end as well: sse.disconnect
#   with no reason on an sse scope, and http.disconnect after a WebSocket
#   refusal, which was an HTTP exchange on that scope.
#
# Three clean ends -- a refused WebSocket handshake, a refused SSE stream, and
# an SSE stream the application closed itself -- on both transports, at both
# moments the spec names: a call made after the end, and a call already
# outstanding when it happens. The reason key must be ABSENT from the event:
# the object was marked complete with no reason, and "Agreement with
# disconnect events" binds the event's reason to the object's.
#
# The sse scope carries a request body, so the pending call can be one
# reading that body on a request half the client never finished; two cases
# drive exactly that, and on HTTP/2 they also pin how the finished stream is
# ended. Two more pin where the answer arrives: after the application's own
# terminal send returns, never inside it.
#
# The spec also lets a server bound how many receives it answers this way.
# PAGI::Server's bound is max_disconnect_receives, and three subtests pin that
# these answers go through it like every other synthesized answer, and that a
# call already waiting when the scope ended is a delivery rather than a repeat
# request for the end, so it is not counted.
#
# The control is the other half of the rule: an end the transport imposed
# still delivers sse.disconnect carrying the reason the scope recorded.
#
# The release assertions (the answered Future, the application coroutine and
# the scope are all freed when the application returns) use t/73's technique;
# the alarmed pump and the per-call bound are t/76's.

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
# case can assert exactly what its scope complained about.
my @LOG;
my %RANK = (warn => 1, error => 1, fatal => 1);

sub logged_since {
    my ($mark) = @_;
    return [map { "$_->{level}: $_->{message}" }
            grep { $RANK{ $_->{level} // '' } } @LOG[$mark .. $#LOG]];
}

# The cap's failure message and error line, verbatim (PAGI::Server
# "max_disconnect_receives"); the same spelling t/75 and t/http2/31 use.
sub cap_message {
    my ($n, $max) = @_;
    return "receive() called $n times after the scope ended; "
         . "the application is not checking for it "
         . "(PAGI::Server max_disconnect_receives=$max)";
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
# reported as parked rather than stalling the file.
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
# The four scope endings this file drives
# ============================================================
#   ws_refusal    a refused WebSocket handshake -- an HTTP response on a
#                 websocket scope
#   sse_refusal   a refused SSE stream -- the same response on an sse scope
#   sse_close     a started stream the application ended with sse.close
#   body_open     the same sse.close, on a scope whose request body the
#                 client never finished, with the receive under test parked
#                 on that body
#   client_drop   the control: no terminal event at all, the transport goes
#                 away under an outstanding receive
my %SCOPE_KIND = (
    ws_refusal  => 'websocket',
    sse_refusal => 'sse',
    sse_close   => 'sse',
    body_open   => 'sse',
    client_drop => 'sse',
);

# What the scope's end resolves a receive with, per the spec paragraph above.
my %END_EVENT = (
    ws_refusal  => { type => 'http.disconnect' },
    sse_refusal => { type => 'sse.disconnect' },
    sse_close   => { type => 'sse.disconnect' },
);

my %REFUSAL_STATUS = (ws_refusal => 403, sse_refusal => 404);

# Sends made before the receive under test is armed.
sub setup_events {
    my ($case) = @_;
    return () unless $SCOPE_KIND{$case} eq 'sse' && $case ne 'sse_refusal';
    return ({ type => 'sse.start', status => 200, headers => [] },
            { type => 'sse.send', data => 'hi' });
}

# The sends that end the scope cleanly. The control has none.
sub terminal_events {
    my ($case) = @_;
    return ({ type => 'sse.close' }) if $case eq 'sse_close' || $case eq 'body_open';
    return () if $case eq 'client_drop';
    return ({ type => 'http.response.start', status => $REFUSAL_STATUS{$case},
              headers => [['content-type', 'text/plain'], ['content-length', 4]] },
            { type => 'http.response.body', body => 'nope' });
}

# ============================================================
# The application every case runs
# ============================================================
# $timing says when the receive under test is made: 'after' the scope's
# terminal sends, or 'pending' -- armed first, so it is already outstanding
# when the terminal event is accepted.
sub clean_end_app {
    my ($case, $timing, $obs) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq $SCOPE_KIND{$case};

        my $app_guard = RetentionGuard->new(\$obs->{app_freed}); # freed with the coroutine
        $obs->{scope_weak} = $scope;
        weaken($obs->{scope_weak});

        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { $obs->{on_complete}++ });
        $cs->on_disconnect(sub { $obs->{on_disconnect} = $_[0] // 'undef' });

        await $receive->();                           # websocket.connect / sse.request
        for my $event (setup_events($case)) { await $send->($event) }

        my $under_test;
        my $arm = sub {
            $under_test = $receive->();
            my $rg = RetentionGuard->new(\$obs->{receive_freed});
            $under_test->on_ready(sub { my $keep = $rg });   # lives as long as it does
        };

        if ($timing eq 'pending') {
            $arm->();
            for my $event (terminal_events($case)) { await $send->($event) }
        }
        else {
            for my $event (terminal_events($case)) { await $send->($event) }
            $arm->();
        }

        $obs->{answer}  = await bounded_wait($under_test);
        $obs->{pending} = $under_test->is_ready ? 0 : 1;
        $obs->{object}  = {
            is_connected      => $cs->is_connected ? 1 : 0,
            response_complete => $cs->response_complete ? 1 : 0,
            disconnect_reason => $cs->disconnect_reason,
        };
        $obs->{returned} = 1;
        return;
    };
}

# The cap case: one clean end, then three more receives on a scope whose
# server allows exactly one answer.
sub capped_app {
    my ($obs) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'sse';

        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { $obs->{on_complete}++ });
        $cs->on_disconnect(sub { $obs->{on_disconnect} = $_[0] // 'undef' });

        await $receive->();
        await $send->({ type => 'sse.start', status => 200, headers => [] });
        await $send->({ type => 'sse.send', data => 'hi' });
        await $send->({ type => 'sse.close' });

        for my $n (1 .. 3) {
            my $answer;
            my $ok = eval { $answer = await bounded_wait($receive->()); 1 };
            push @{ $obs->{answers} }, $ok ? $answer : { FAILED => "$@" };
        }
        $obs->{returned} = 1;
        return;
    };
}

# The cap case the ruling on pending answers needs: a receive already
# outstanding when the scope ends is a delivery, not a repeat request for the
# end, so it is answered without being counted. The two calls after it are
# counted, and the server here allows exactly one.
sub capped_pending_app {
    my ($obs) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'sse';

        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { $obs->{on_complete}++ });
        $cs->on_disconnect(sub { $obs->{on_disconnect} = $_[0] // 'undef' });

        await $receive->();
        await $send->({ type => 'sse.start', status => 200, headers => [] });
        await $send->({ type => 'sse.send', data => 'hi' });

        my $pending = $receive->();
        await $send->({ type => 'sse.close' });
        push @{ $obs->{answers} }, await bounded_wait($pending);

        for my $n (1 .. 2) {
            my $answer;
            my $ok = eval { $answer = await bounded_wait($receive->()); 1 };
            push @{ $obs->{answers} }, $ok ? $answer : { FAILED => "$@" };
        }
        $obs->{returned} = 1;
        return;
    };
}

# An sse scope carrying a request body the client never finished, with the
# receive under test parked on that body when sse.close lands. The two
# transports present the shape differently -- HTTP/1.1 delivers each chunk as
# it arrives, HTTP/2 holds sse.request back until the whole body is in -- so
# the HTTP/1.1 application reads its first chunk and arms the call under test
# after it, while on HTTP/2 the call under test is itself the one waiting for
# a body that never completes.
sub body_open_app {
    my ($transport, $obs) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'sse';

        my $cs = $scope->{'pagi.connection'};
        $cs->on_complete(sub { $obs->{on_complete}++ });
        $cs->on_disconnect(sub { $obs->{on_disconnect} = $_[0] // 'undef' });

        my $under_test;
        if ($transport eq 'h1') {
            $obs->{first} = await bounded_wait($receive->());
            for my $event (setup_events('sse_close')) { await $send->($event) }
            $under_test = $receive->();
        }
        else {
            $under_test = $receive->();
            for my $event (setup_events('sse_close')) { await $send->($event) }
        }
        $obs->{armed_ready} = $under_test->is_ready ? 1 : 0;

        await $send->({ type => 'sse.close' });

        $obs->{answer}  = await bounded_wait($under_test);
        $obs->{pending} = $under_test->is_ready ? 0 : 1;
        $obs->{object}  = {
            is_connected      => $cs->is_connected ? 1 : 0,
            response_complete => $cs->response_complete ? 1 : 0,
            disconnect_reason => $cs->disconnect_reason,
        };
        # Held so the file can ask, once the connection is gone, whether the
        # call was still waiting: a park that outlives teardown is the defect
        # this case exists for.
        $obs->{outstanding} = $under_test;
        $obs->{returned} = 1;
        return;
    };
}

# Where the answer to a receive resolved by the application's own terminal
# send arrives: a real awaiting coroutine records the moment it resumes,
# against the send that produced the answer. An answer delivered inline off
# ->done lands between BEFORE and AFTER, inside the application's own
# half-finished $send.
sub order_app {
    my ($case, $obs) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq $SCOPE_KIND{$case};

        await $receive->();
        for my $event (setup_events($case)) { await $send->($event) }

        my $under_test = $receive->();
        my $watcher = (async sub {
            $obs->{answer} = await $under_test;
            push @{ $obs->{order} }, 'resumed';
        })->();

        # Only the last of a case's terminal sends ends the scope; a refusal's
        # http.response.start is still setup.
        my @terminal = terminal_events($case);
        while (@terminal > 1) { await $send->(shift @terminal) }

        push @{ $obs->{order} }, 'BEFORE send';
        await $send->($terminal[0]);
        push @{ $obs->{order} }, 'AFTER send';

        await Future->wait_any($watcher->without_cancel,
                               $loop->delay_future(after => $PARK_BOUND));
        $obs->{returned} = 1;
        return;
    };
}

sub describe {
    my ($event) = @_;
    return 'no answer' unless ref $event eq 'HASH';
    return 'FAILED: ' . $event->{FAILED} if exists $event->{FAILED};
    my $type = $event->{type} // '';
    return $type . (defined $event->{reason} ? " reason=$event->{reason}" : '');
}

# ============================================================
# HTTP/1.1 harness
# ============================================================

sub h1_request {
    my ($case) = @_;
    return "GET /socket HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
         . "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: "
         . MIME::Base64::encode_base64('0123456789abcdef', '') . "\r\n\r\n"
        if $SCOPE_KIND{$case} eq 'websocket';
    # One chunk and no terminator: the request half stays open for the life of
    # the scope, so a receive reading the body has nothing left to wake it.
    return "POST /events HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n"
         . "Transfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n"
        if $case eq 'body_open';
    return "GET /events HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n";
}

sub h1_run {
    my ($case, $timing, %opt) = @_;
    my %obs;

    my $server = PAGI::Server->new(
        app => $opt{app} // clean_end_app($case, $timing, \%obs),
        host => '127.0.0.1', port => 0,
        log_level => 'debug', access_log => undef, shutdown_timeout => 1,
        logger => sub { push @LOG, $_[0] },
        %{ $opt{server_opts} // {} },
    );
    $loop->add($server);
    $server->listen->get;

    # Taken once the server is up, so a case's assertions cover its own scope
    # and not the lines the server writes as it begins listening.
    my $mark = scalar @LOG;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 5)
        or die "connect: $!";
    print $sock h1_request($case);
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

sub h2_submit {
    my ($client, $case) = @_;
    return $client->submit_request(
        method => 'CONNECT', path => '/socket', scheme => 'https',
        authority => 'localhost',
        headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
        body => sub { undef })
        if $SCOPE_KIND{$case} eq 'websocket';
    # One chunk and then a source that yields nothing and never signals
    # END_STREAM: the request half stays open, so this stream does not close
    # of its own accord once the response is finished.
    if ($case eq 'body_open') {
        my $sent = 0;
        return $client->submit_request(
            method => 'POST', path => '/events', scheme => 'http',
            authority => 'localhost',
            headers => [['accept', 'text/event-stream'], ['content-type', 'text/plain']],
            body => sub { return 'hello' unless $sent++; return undef });
    }
    return $client->submit_request(
        method => 'GET', path => '/events', scheme => 'http',
        authority => 'localhost', headers => [['accept', 'text/event-stream']]);
}

# RST_STREAM's frame type (RFC 9113 section 6.4, 0x03). Net::HTTP2::nghttp2
# exports NGHTTP2_DATA and NGHTTP2_FLAG_END_STREAM but no frame-type constant
# for this one.
use constant RST_STREAM_FRAME => 3;

sub h2_run {
    my ($case, $timing, %opt) = @_;
    my %obs;

    my $app = $opt{app} // clean_end_app($case, $timing, \%obs);
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, http2 => 1,
        log_level => 'debug', access_log => undef,
        logger => sub { push @LOG, $_[0] },
        %{ $opt{server_opts} // {} },
    );
    $loop->add($server);

    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $_->blocking(0) for $sock_a, $sock_b;
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        max_disconnect_receives => $server->{max_disconnect_receives},
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;
    my $mark = scalar @LOG;

    require Net::HTTP2::nghttp2::Session;
    my ($body, $close_code, %headers, @frames) = ('', undef);
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { my (undef, $n, $v) = @_; $headers{lc $n} = $v; 0 },
        on_frame_recv      => sub { my ($f) = @_; push @frames, $f; 0 },
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

    h2_submit($client, $case);
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

    return (\%before, \%obs, $body, \%headers, $close_code, $mark, \@frames);
}

sub freed { my ($obs, $key) = @_; return $obs->{$key} // 0 }
sub scope_alive { my ($obs) = @_; return defined $obs->{scope_weak} ? 1 : 0 }

# What each clean end leaves on the wire, so a case proves the end was the
# clean one it claims rather than a truncation that happened to look like it.
sub check_h1_wire {
    my ($case, $wire) = @_;
    if ($case eq 'sse_close') {
        like($wire, qr/data: hi/, 'the client received the event');
        like($wire, qr/\r\n0\r\n\r\n\z/, 'and the chunked terminator ended the stream cleanly');
        return;
    }
    like($wire, qr{^HTTP/1\.1 $REFUSAL_STATUS{$case}\b}, 'the refusal response reached the client');
    like($wire, qr/nope\z/, 'with its whole body');
}

sub check_h2_wire {
    my ($case, $body, $headers, $close_code) = @_;
    if ($case eq 'sse_close') {
        like($body, qr/data: hi/, 'the client received the event');
        is($close_code, 0, 'and the stream ended with END_STREAM, no error code');
        return;
    }
    is($headers->{':status'}, "$REFUSAL_STATUS{$case}",
        'the refusal response reached the client');
    like($body, qr/nope\z/, 'with its whole body');
    # Only the sse refusal's stream is reported closed here. The websocket
    # refusal rides a CONNECT whose request half this client leaves open, and
    # nghttp2 reports a close only once both halves are done; the refusal
    # response in full is the clean end on that scope.
    is($close_code, 0, 'and the stream ended with END_STREAM, no error code')
        if $case eq 'sse_refusal';
}

# ============================================================
# 1 & 2. The receive reports the scope's end, made after it or pending at it
# ============================================================

my %TITLE = (
    ws_refusal  => 'a refused WebSocket handshake',
    sse_refusal => 'a refused SSE stream',
    sse_close   => "the application's own sse.close",
);

my %WHEN = (
    after   => 'a receive made after',
    pending => 'a receive already pending at',
);

for my $case (qw(ws_refusal sse_refusal sse_close)) {
    for my $timing (qw(after pending)) {

        subtest "h1: $WHEN{$timing} $TITLE{$case} reports the scope's end" => sub {
            my ($before, $after, $wire, $mark) = h1_run($case, $timing);

            is($before->{returned}, 1, 'the application ran to the end');
            is($before->{pending}, 0, 'the receive answered');
            is($before->{answer}, $END_EVENT{$case},
                'with exactly the scope end, and no reason key')
                or diag('answer: ' . describe($before->{answer}));

            is($before->{object}, {
                is_connected => 0, response_complete => 1, disconnect_reason => undef,
            }, 'the object reads a clean end');
            is($before->{on_complete}, 1, 'on_complete fired exactly once');
            is($before->{on_disconnect}, undef, 'on_disconnect never fired');

            check_h1_wire($case, $wire);
            is(logged_since($mark), [], 'nothing was logged');

            is(freed($after, 'app_freed'), 1, 'the application coroutine was collected');
            is(freed($after, 'receive_freed'), 1, 'the receive Future was collected');
            is(scope_alive($after), 0, 'the scope hash is gone');
            ok(!$ALARM_FIRED, 'no pump needed its alarm');
        };

        subtest "h2: $WHEN{$timing} $TITLE{$case} reports the scope's end" => sub {
            skip_all 'HTTP/2 not available' unless $have_h2;
            my ($before, $after, $body, $headers, $close_code, $mark) = h2_run($case, $timing);

            is($before->{returned}, 1, 'the application ran to the end');
            is($before->{pending}, 0, 'the receive answered');
            is($before->{answer}, $END_EVENT{$case},
                'with exactly the scope end, and no reason key')
                or diag('answer: ' . describe($before->{answer}));

            is($before->{object}, {
                is_connected => 0, response_complete => 1, disconnect_reason => undef,
            }, 'the object reads a clean end');
            is($before->{on_complete}, 1, 'on_complete fired exactly once');
            is($before->{on_disconnect}, undef, 'on_disconnect never fired');

            check_h2_wire($case, $body, $headers, $close_code);
            is(logged_since($mark), [], 'nothing was logged');

            is(freed($after, 'app_freed'), 1, 'the application coroutine was collected');
            is(freed($after, 'receive_freed'), 1, 'the receive Future was collected');
            is(scope_alive($after), 0, 'the scope hash is gone');
            ok(!$ALARM_FIRED, 'no pump needed its alarm');
        };
    }
}

# ============================================================
# 3. The same clean end with the request body still open
# ============================================================
# Www.pod "Close SSE - send event" is unconditional: "A $receive call, pending
# or later, resolves with sse.disconnect carrying no reason." An sse scope
# carries a request body (sse.request: POST, htmx, datastar), so the call that
# is pending when sse.close lands can be one reading that body, on a request
# half the client has not finished. Nothing else will ever wake it, so this is
# the shape where a park lasts for the life of the process.
#
# On HTTP/2 the scope's end is also the moment the server has finished its
# output, so the object is complete there and then, and the stream -- which
# the open request half would otherwise hold open forever -- is ended the way
# RFC 9113 section 8.1 prescribes for a complete response sent before the
# request finished: RST_STREAM with error code NO_ERROR.

# The clean end on the HTTP/2 wire for such a stream, in order.
sub h2_end_frames {
    my ($frames) = @_;
    require Net::HTTP2::nghttp2;
    my $data       = Net::HTTP2::nghttp2::NGHTTP2_DATA();
    my $end_stream = Net::HTTP2::nghttp2::NGHTTP2_FLAG_END_STREAM();
    my @seen;
    for my $frame (@$frames) {
        push @seen, 'END_STREAM'
            if $frame->{type} == $data && ($frame->{flags} & $end_stream);
        push @seen, 'RST_STREAM' if $frame->{type} == RST_STREAM_FRAME;
    }
    return \@seen;
}

subtest 'h1: a receive reading a chunked request body reports the scope end' => sub {
    my %obs;
    my (undef, undef, $wire, $mark) =
        h1_run('body_open', 'pending', app => body_open_app('h1', \%obs));

    is($obs{returned}, 1, 'the application ran to the end');
    is($obs{first}{type}, 'sse.request', 'the first chunk arrived as sse.request');
    is($obs{first}{body}, 'hello', 'with its body');
    is($obs{first}{more}, 1, 'and more still to come');

    is($obs{armed_ready}, 0, 'the call under test was outstanding when sse.close was sent');
    is($obs{pending}, 0, 'and it answered');
    is($obs{answer}, { type => 'sse.disconnect' },
        'with exactly the scope end, and no reason key')
        or diag('answer: ' . describe($obs{answer}));

    is($obs{object}, {
        is_connected => 0, response_complete => 1, disconnect_reason => undef,
    }, 'the object reads a clean end');
    is($obs{on_complete}, 1, 'on_complete fired exactly once');
    is($obs{on_disconnect}, undef, 'on_disconnect never fired');

    like($wire, qr/data: hi/, 'the client received the event');
    like($wire, qr/\r\n0\r\n\r\n\z/, 'and the chunked terminator ended the stream cleanly');
    is(logged_since($mark), [], 'nothing was logged');

    is($obs{outstanding}->is_ready, 1, 'the call was not still waiting after teardown');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

subtest 'h2: a receive reading an unfinished request body reports the scope end' => sub {
    skip_all 'HTTP/2 not available' unless $have_h2;
    my %obs;
    my (undef, undef, $body, $headers, $close_code, $mark, $frames) =
        h2_run('body_open', 'pending', app => body_open_app('h2', \%obs));

    is($obs{returned}, 1, 'the application ran to the end');
    is($obs{armed_ready}, 0, 'the call under test was outstanding when sse.close was sent');
    is($obs{pending}, 0, 'and it answered');
    is($obs{answer}, { type => 'sse.disconnect' },
        'with exactly the scope end, and no reason key')
        or diag('answer: ' . describe($obs{answer}));

    is($obs{object}, {
        is_connected => 0, response_complete => 1, disconnect_reason => undef,
    }, 'the object reads a clean end, from inside the application');
    is($obs{on_complete}, 1, 'on_complete fired exactly once');
    is($obs{on_disconnect}, undef, 'on_disconnect never fired');

    like($body, qr/data: hi/, 'the client received the event');
    is(h2_end_frames($frames), ['END_STREAM', 'RST_STREAM'],
        'the response ended with END_STREAM, then the stream was reset');
    is($close_code, 0, 'and the reset carried error code NO_ERROR');
    is(logged_since($mark), [], 'nothing was logged');

    is($obs{outstanding}->is_ready, 1, 'the call was not still waiting after teardown');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

# ============================================================
# 4. The answer arrives after the application's own send returns
# ============================================================
# Www.pod "Callback invocation context" keeps a callback out of the
# application's own half-finished $send, so a framework never has to defend
# every send against re-entrancy. An answer the application's own terminal
# send produced is the same hazard: Future::AsyncAwait resumes an awaiting
# coroutine inline off ->done, so without a deferral the resumed code would
# run inside that send. The order below is the whole assertion.

for my $transport (qw(h1 h2)) {
    subtest "$transport: the scope end resolves a receive after the send returns" => sub {
        skip_all 'HTTP/2 not available' if $transport eq 'h2' && !$have_h2;

        for my $case (qw(ws_refusal sse_refusal sse_close)) {
            my %obs = (order => []);
            my $run = $transport eq 'h1' ? \&h1_run : \&h2_run;
            $run->($case, 'pending', app => order_app($case, \%obs));

            is($obs{returned}, 1, "$case: the application ran to the end");
            is($obs{order}, ['BEFORE send', 'AFTER send', 'resumed'],
                "$case: the awaiting coroutine resumed after the send returned");
            is($obs{answer}, $END_EVENT{$case},
                "$case: with the scope's end")
                or diag('answer: ' . describe($obs{answer}));
        }
        ok(!$ALARM_FIRED, 'no pump needed its alarm');
    };
}

# ============================================================
# 5. The bound the spec allows counts these answers
# ============================================================
# Www.pod: "A server MAY bound how many receives it answers this way on one
# scope ... failing the receive once its documented bound is exceeded."
# PAGI::Server's bound is max_disconnect_receives, so a clean end's answers
# go through the same gate as every other synthesized answer and are counted
# the same way, with the same message and the same single error line
# t/75 and t/http2/31 pin for an abnormal end.

subtest 'h1 sse.close: the answers count against max_disconnect_receives' => sub {
    my %obs;
    my (undef, undef, $wire, $mark) =
        h1_run('sse_close', 'after',
               app => capped_app(\%obs), server_opts => { max_disconnect_receives => 1 });

    is($obs{returned}, 1, 'the application ran to the end');
    is($obs{answers}[0], { type => 'sse.disconnect' },
        'the first receive is answered with the scope end')
        or diag('answer: ' . describe($obs{answers}[0]));
    like($obs{answers}[1]{FAILED}, qr/\Q@{[ cap_message(2, 1) ]}\E/,
        'the call past the cap fails, naming the cap');
    like($obs{answers}[2]{FAILED}, qr/\Q@{[ cap_message(3, 1) ]}\E/,
        'and so does the one after it');

    is(logged_since($mark), [ 'error: sse scope on HTTP/1.1: ' . cap_message(2, 1) ],
        'one error line for the scope, and nothing else');
    is($obs{on_complete}, 1, 'on_complete still fired exactly once');
    is($obs{on_disconnect}, undef, 'on_disconnect never fired');
    like($wire, qr/\r\n0\r\n\r\n\z/, 'the stream still ended cleanly on the wire');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

# ============================================================
# 6. A receive already pending when the scope ended is not counted
# ============================================================
# The same ruling's other half: an answer to a call that was already waiting
# is a delivery, not a repeat request for the end, so it does not count
# against the bound. At max_disconnect_receives => 1 that is the difference
# between two answers and one.

my %CAP_TRANSPORT = (h1 => 'HTTP/1.1', h2 => 'HTTP/2 stream 1');

for my $transport (qw(h1 h2)) {
    subtest "$transport sse.close: the pending answer is not counted against the bound" => sub {
        skip_all 'HTTP/2 not available' if $transport eq 'h2' && !$have_h2;

        my %obs;
        my $run = $transport eq 'h1' ? \&h1_run : \&h2_run;
        my @out = $run->('sse_close', 'after',
                         app => capped_pending_app(\%obs),
                         server_opts => { max_disconnect_receives => 1 });
        # The log mark each harness returns: h1_run's fourth value, h2_run's
        # sixth.
        my $mark = $transport eq 'h1' ? $out[3] : $out[5];

        is($obs{returned}, 1, 'the application ran to the end');
        is($obs{answers}[0], { type => 'sse.disconnect' },
            'the receive already pending at sse.close is answered')
            or diag('answer: ' . describe($obs{answers}[0]));
        is($obs{answers}[1], { type => 'sse.disconnect' },
            'and so is the next one, which is the one the bound allows')
            or diag('answer: ' . describe($obs{answers}[1]));
        like($obs{answers}[2]{FAILED}, qr/\Q@{[ cap_message(2, 1) ]}\E/,
            'the call past the bound fails, naming the cap');

        is(logged_since($mark),
            [ "error: sse scope on $CAP_TRANSPORT{$transport}: " . cap_message(2, 1) ],
            'one error line for the scope, and nothing else');
        is($obs{on_complete}, 1, 'on_complete still fired exactly once');
        is($obs{on_disconnect}, undef, 'on_disconnect never fired');
        ok(!$ALARM_FIRED, 'no pump needed its alarm');
    };
}

# ============================================================
# 7. Control: an end the transport imposed still carries its reason
# ============================================================

subtest 'h1: a client that drops mid-stream still delivers sse.disconnect' => sub {
    my ($before) = h1_run('client_drop', 'pending');

    is($before->{returned}, 1, 'the application ran to the end');
    is($before->{answer}{type}, 'sse.disconnect', 'the pending receive answered with the event');
    like($before->{answer}{reason}, qr/^(?:client_closed|read_error)$/,
        'carrying the reason the scope recorded');
    is($before->{object}{is_connected}, 0, 'the object reports the disconnect');
    is($before->{object}{disconnect_reason}, $before->{answer}{reason},
        'and agrees with the event on the reason');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

subtest 'h2: a client that drops mid-stream still delivers sse.disconnect' => sub {
    skip_all 'HTTP/2 not available' unless $have_h2;
    my ($before) = h2_run('client_drop', 'pending');

    is($before->{returned}, 1, 'the application ran to the end');
    is($before->{answer}{type}, 'sse.disconnect', 'the pending receive answered with the event');
    like($before->{answer}{reason}, qr/^(?:client_closed|read_error)$/,
        'carrying the reason the scope recorded');
    is($before->{object}{is_connected}, 0, 'the object reports the disconnect');
    is($before->{object}{disconnect_reason}, $before->{answer}{reason},
        'and agrees with the event on the reason');
    ok(!$ALARM_FIRED, 'no pump needed its alarm');
};

done_testing;
