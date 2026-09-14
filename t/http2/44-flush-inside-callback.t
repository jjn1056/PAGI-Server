use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use Future;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

# ============================================================
# Test: nothing reached from inside an nghttp2 session call flushes
# ============================================================
# L<Net::HTTP2::nghttp2::Session> "Reentrancy": a callback may queue frames
# with the submit_* methods, but "mem_send and mem_recv drive the flush, so
# calling either from inside a callback croaks". The server reaches both from
# inside a callback by an ordinary route: nghttp2 delivers a frame, the server
# wakes a receive() parked on that scope, Future::AsyncAwait resumes the
# application inline off ->done, and the application's next send() flushes.
#
# The two entry points are both covered here, because each is a different
# nghttp2 call on the stack:
#
#   (a) the read side -- a client RST_STREAM arrives, _h2_on_close wakes the
#       parked application, and the flush would land inside mem_recv.
#   (b) the write side -- a response completes with the request half still
#       open, the END_STREAM the server serialised triggers its own NO_ERROR
#       reset, the stream closes, the parked application is woken, and the
#       flush would land inside mem_send.
#
# In both the woken application sends on a SECOND stream it also owns, so the
# send is a live one that must reach the wire rather than a no-op on a stream
# that has just closed. One application instance cannot hold two scopes, so
# the two scopes share their send() closures through a file-scoped hash: the
# stream that stays open publishes its send() and waits on a gate, and the
# woken one reaches it through %HOLD. That hash is also how case (b) completes
# the parked scope's response while that scope is parked -- the sibling issues
# the terminal event on its behalf.
#
# What parks the application is the client's unfinished request body: an h2
# http scope delivers http.request only once the body is complete, so a client
# that sends HEADERS and no END_STREAM leaves the application's very first
# receive() parked -- and leaves the request half open, which is what makes
# the server send case (b)'s NO_ERROR reset in the first place.
#
# Case (c) is the same rule seen from the other side: the 431, 501 and 413
# answers the server synthesizes from inside mem_recv used to be deferred a
# loop turn each to keep their flush out of the session call. They submit
# synchronously now and are carried out by the flush the session call makes on
# its way out; the statuses must still arrive. t/http2/39, t/http2/12 and
# t/http2/21 pin the bodies and headers of those three answers.
#
# Case (d) is the whole file's guard: no log line anywhere names the croak.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# Every server built here logs into this collector rather than STDERR. It is
# never cleared: the last subtest asserts over the whole file's output.
my @LOG;

my $CROAK_RE = qr/inside a session callback/;

# An application resumed from an already-resolved Future never returns control
# to the event loop, so an alarm is the only bound a pump can impose.
my $ALARM_FIRED = 0;

sub bounded {
    my ($body, $seconds) = @_;
    local $SIG{ALRM} = sub { $ALARM_FIRED = 1; die "pump alarm\n" };
    alarm($seconds // 10);
    my $ok  = eval { $body->(); 1 };
    my $err = $@;
    alarm(0);
    die $err if !$ok && $err ne "pump alarm\n";
    return $ALARM_FIRED;
}

sub shutdown_server {
    my ($server) = @_;
    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
}

# ============================================================
# HTTP/2 harness (shape borrowed from t/http2/43, t/71 and t/75)
# ============================================================

sub create_h2_connection {
    my (%o) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app    = $o{app} // sub { };
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, http2 => 1,
        log_level => 'debug', access_log => undef,
        logger => sub { push @LOG, $_[0] },
        %{ $o{server_opts} // {} },
    );
    $loop->add($server);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        max_disconnect_receives => $server->{max_disconnect_receives},
        max_body_size           => $server->{max_body_size},
        sse_idle_timeout        => $server->{sse_idle_timeout},
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;
    return ($conn, $stream, $sock_b, $server);
}

sub create_client {
    my (%o) = @_;
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => $o{on_header}          // sub { 0 },
        on_frame_recv      => $o{on_frame_recv}      // sub { 0 },
        on_data_chunk_recv => $o{on_data_chunk_recv} // sub { 0 },
        on_stream_close    => $o{on_stream_close}    // sub { 0 },
    });
}

sub complete_h2_handshake {
    my ($client, $client_sock, %settings) = @_;
    return bounded(sub {
        $loop->loop_once(0.1);
        my $settings = '';
        $client_sock->sysread($settings, 4096);
        $client->send_connection_preface(%settings);
        $client_sock->syswrite($client->mem_send);
        $loop->loop_once(0.1);
        $client->mem_recv($settings);
        $loop->loop_once(0.1);
        my $ack = '';
        $client_sock->sysread($ack, 4096);
        $client->mem_recv($ack) if length($ack);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
        $loop->loop_once(0.1);
        my $extra = '';
        $client_sock->sysread($extra, 4096);
        $client->mem_recv($extra) if length($extra);
    });
}

sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    return bounded(sub {
        for (1 .. ($rounds // 20)) {
            $loop->loop_once(0.05);
            next unless defined fileno($client_sock);   # the client may have left
            my $buf = '';
            $client_sock->sysread($buf, 16384);
            $client->mem_recv($buf) if length($buf);
            my $out = $client->mem_send;
            $client_sock->syswrite($out) if length($out);
        }
    });
}

# A GET whose request half the client never finishes, so the server's response
# outruns it: the shape RFC 9113 section 8.1's NO_ERROR reset exists for, and
# the shape that keeps a receive() genuinely parked.
sub submit_open_request {
    my ($client, $client_sock, $path) = @_;
    my $sid = $client->submit_request(
        method => 'POST', path => $path, scheme => 'http', authority => 'localhost',
        headers => [], body => sub { return undef },
    );
    my $out = $client->mem_send;
    $client_sock->syswrite($out) if length($out);
    return $sid;
}

sub submit_closed_request {
    my ($client, $client_sock, $path) = @_;
    my $sid = $client->submit_request(
        method => 'GET', path => $path, scheme => 'http', authority => 'localhost',
        headers => [],
    );
    my $out = $client->mem_send;
    $client_sock->syswrite($out) if length($out);
    return $sid;
}

# ============================================================
# The two applications the woken-receive cases share
# ============================================================

my %HOLD;      # path => that scope's send() closure
my %REACHED;   # path => how far that scope's application got
my %WOKE;      # path => the event that resumed that scope
my %FAILED;    # path => why that scope's send failed, if it did
my $GATE;      # released once the woken application has had its turn

# The stream that stays open for the whole case. It starts a streaming
# response (so its stream stays writable), publishes its send(), and waits.
my $sibling_app = async sub {
    my ($receive, $send, $path) = @_;
    await $receive->();
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'open;', more => 1 });
    $HOLD{$path} = $send;
    $REACHED{$path} = 'published';
    return;
};

# The application that gets woken. It publishes its send() before parking, so
# the sibling can answer this scope on its behalf while it waits.
my $parked_app = async sub {
    my ($receive, $send, $path, $sibling, $bytes) = @_;
    $HOLD{$path}    = $send;
    $REACHED{$path} = 'parked';
    my $ev = await $receive->();
    $WOKE{$path}    = $ev->{type};
    $REACHED{$path} = 'woken';
    # The send that used to croak: a flush reached from inside a session call.
    # Its outcome is recorded rather than thrown, so a failure is asserted as
    # itself instead of reaching the assertions as a log line the dispatch
    # wrapper wrote about an application that died.
    my $f = $HOLD{$sibling}->({ type => 'http.response.body',
                                body => $bytes, more => 1 });
    $f->on_fail(sub { $FAILED{$path} = "$_[0]" });
    await $f->else(sub { Future->done });
    $REACHED{$path} = $FAILED{$path} ? 'send-failed' : 'sent';
    return;
};

# ============================================================
# (a) woken inside mem_recv by a client RST_STREAM
# ============================================================
subtest 'a receive woken inside the session read flushes after the read returns' => sub {
    %HOLD = (); %REACHED = (); %WOKE = (); %FAILED = (); $ALARM_FIRED = 0;
    my $mark = scalar @LOG;
    $GATE = $loop->new_future;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $path = $scope->{path};
        if ($path eq '/sibling') {
            await $sibling_app->($receive, $send, $path);
            await $GATE;
            await $send->({ type => 'http.response.body', body => 'end', more => 0 });
            $REACHED{$path} = 'finished';
            return;
        }
        await $parked_app->($receive, $send, $path, '/sibling', 'woken-a;');
        $GATE->done unless $GATE->is_ready;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my (%body, %closed);
    my $client = create_client(
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $body{$sid} .= $d; 0 },
        on_stream_close    => sub { my ($sid, $ec) = @_; $closed{$sid} = $ec; 0 },
    );
    complete_h2_handshake($client, $client_sock);

    my $sibling_id = submit_closed_request($client, $client_sock, '/sibling');
    exchange_frames($client, $client_sock, 10);
    my $parked_id = submit_open_request($client, $client_sock, '/parked');
    exchange_frames($client, $client_sock, 10);

    is($REACHED{'/sibling'}, 'published', 'the sibling stream published its send()');
    is($REACHED{'/parked'},  'parked',    'the other application is parked on receive()');

    # The wake: a client reset delivered inside mem_recv.
    $client->submit_rst_stream($parked_id, 8);   # CANCEL
    my $out = $client->mem_send;
    $client_sock->syswrite($out) if length($out);
    exchange_frames($client, $client_sock, 20);

    is($WOKE{'/parked'}, 'http.disconnect', 'the reset woke it with the scope\'s ending');
    is($FAILED{'/parked'}, undef, 'the send on the sibling stream did not fail')
        or diag($FAILED{'/parked'});
    is($REACHED{'/parked'}, 'sent',
        'the woken application completed its send on the sibling stream');
    like($body{$sibling_id} // '', qr/woken-a;/,
        'and the bytes it sent reached the wire');
    is($REACHED{'/sibling'}, 'finished', 'the sibling response then completed');

    my @lines = grep { ($_->{message} // q{}) =~ $CROAK_RE } @LOG[$mark .. $#LOG];
    is(scalar(@lines), 0, 'nothing croaked about a session callback')
        or diag(join qq{\n}, map { $_->{message} // q{} } @lines);
    my @warned = grep { ($_->{level} // '') =~ /^(warn|error)$/ } @LOG[$mark .. $#LOG];
    is(scalar(@warned), 0, 'the case logged nothing at warn or error')
        or diag(join qq{\n}, map { "$_->{level}: " . ($_->{message} // q{}) } @warned);
    is($ALARM_FIRED, 0, 'no pump alarm');

    $stream_io->close_now;
    shutdown_server($server);
};

# ============================================================
# (b) woken inside mem_send by the server's own NO_ERROR reset
# ============================================================
subtest 'a receive woken inside the session write flushes after the write returns' => sub {
    %HOLD = (); %REACHED = (); %WOKE = (); %FAILED = (); $ALARM_FIRED = 0;
    my $mark = scalar @LOG;
    $GATE = $loop->new_future;
    my $ANSWER = $loop->new_future;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $path = $scope->{path};
        if ($path eq '/sibling') {
            await $sibling_app->($receive, $send, $path);
            await $ANSWER;
            # Answer the parked scope on its behalf. Its request half is still
            # open, so the END_STREAM this serialises makes the server send its
            # own NO_ERROR reset from inside mem_send; the stream closes there,
            # and the parked application is woken inside that same mem_send.
            await $HOLD{'/parked'}->({ type => 'http.response.start', status => 200,
                                       headers => [['content-type', 'text/plain']] });
            await $HOLD{'/parked'}->({ type => 'http.response.body',
                                       body => 'final', more => 0 });
            await $GATE;
            await $send->({ type => 'http.response.body', body => 'end', more => 0 });
            $REACHED{$path} = 'finished';
            return;
        }
        await $parked_app->($receive, $send, $path, '/sibling', 'woken-b;');
        $GATE->done unless $GATE->is_ready;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my (%body, %closed);
    my $client = create_client(
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $body{$sid} .= $d; 0 },
        on_stream_close    => sub { my ($sid, $ec) = @_; $closed{$sid} = $ec; 0 },
    );
    complete_h2_handshake($client, $client_sock);

    my $sibling_id = submit_closed_request($client, $client_sock, '/sibling');
    exchange_frames($client, $client_sock, 10);
    my $parked_id = submit_open_request($client, $client_sock, '/parked');
    exchange_frames($client, $client_sock, 10);

    is($REACHED{'/parked'}, 'parked', 'the application is parked on receive()');
    $ANSWER->done;                      # the sibling now answers that scope
    exchange_frames($client, $client_sock, 20);

    is($WOKE{'/parked'}, 'http.disconnect', 'the stream closing woke it with the scope\'s ending');
    is($FAILED{'/parked'}, undef, 'the send on the sibling stream did not fail')
        or diag($FAILED{'/parked'});
    is($REACHED{'/parked'}, 'sent',
        'the woken application completed its send on the sibling stream');
    like($body{$parked_id} // '', qr/final/,
        'the parked scope\'s own response completed on the wire');
    is($closed{$parked_id}, 0,
        'and its stream closed with NO_ERROR: the reset went out from inside the write');
    like($body{$sibling_id} // '', qr/woken-b;/,
        'the bytes the woken application sent reached the wire');
    is($REACHED{'/sibling'}, 'finished', 'the sibling response then completed');

    my @lines = grep { ($_->{message} // q{}) =~ $CROAK_RE } @LOG[$mark .. $#LOG];
    is(scalar(@lines), 0, 'nothing croaked about a session callback')
        or diag(join qq{\n}, map { $_->{message} // q{} } @lines);
    my @warned = grep { ($_->{level} // '') =~ /^(warn|error)$/ } @LOG[$mark .. $#LOG];
    is(scalar(@warned), 0, 'the case logged nothing at warn or error')
        or diag(join qq{\n}, map { "$_->{level}: " . ($_->{message} // q{}) } @warned);
    is($ALARM_FIRED, 0, 'no pump alarm');

    $stream_io->close_now;
    shutdown_server($server);
};

# ============================================================
# (c) the answers the server synthesizes from inside mem_recv
# ============================================================
# Each of these used to submit its response a loop turn later, purely to keep
# the flush out of the session call. They submit in the callback now.
subtest 'the 431, 501 and 413 answers still reach the client' => sub {
    $ALARM_FIRED = 0;
    my $mark = scalar @LOG;

    my $dispatched = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        $dispatched++;
        await $receive->();
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    };

    my @cases = (
        {   name    => '431',
            opts    => { h2_max_header_list_size => 200 },
            submit  => sub {
                my ($client) = @_;
                return $client->submit_request(
                    method => 'GET', path => '/', scheme => 'http',
                    authority => 'localhost', headers => [['x-big', 'A' x 500]]);
            },
            status  => '431',
            dispatched => 0,
        },
        {   name    => '413',
            opts    => { max_body_size => 100 },
            submit  => sub {
                my ($client) = @_;
                return $client->submit_request(
                    method => 'POST', path => '/', scheme => 'http',
                    authority => 'localhost',
                    headers => [['content-length', '5000']],
                    body => sub { return undef });
            },
            status  => '413',
            dispatched => 0,
        },
    );

    for my $case (@cases) {
        $dispatched = 0;
        my ($conn, $stream_io, $client_sock, $server) =
            create_h2_connection(app => $app, server_opts => $case->{opts});
        my %headers;
        my $client = create_client(
            on_header => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; 0 },
        );
        complete_h2_handshake($client, $client_sock);
        $case->{submit}->($client);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
        exchange_frames($client, $client_sock, 20);

        is($headers{':status'}, $case->{status},
            "the $case->{name} answer reached the client");
        is($dispatched, $case->{dispatched},
            "and the application was dispatched $case->{dispatched} time(s) for it");

        $stream_io->close_now;
        shutdown_server($server);
    }

    # Plain CONNECT. nghttp2 rejects a malformed CONNECT frame before the
    # server's own code ever sees it, so this path is driven the way
    # t/http2/12-error-handling.t drives it: _h2_on_request directly, with
    # submit_response spied on. What is asserted is the part that changed --
    # the 501 is submitted inside the call rather than a loop turn later.
    {
        my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
        my $client = create_client();
        complete_h2_handshake($client, $client_sock);
        my $submitted;
        {
            no warnings 'redefine';
            local *PAGI::Server::Protocol::HTTP2::Session::submit_response = sub {
                my ($session, $sid, %args) = @_;
                $submitted = $args{status};
                return 0;
            };
            $conn->_h2_on_request(99, { ':method' => 'CONNECT', ':path' => '/',
                                        ':scheme' => 'http' }, [], 0);
        }
        is($submitted, 501,
            'the plain-CONNECT 501 is submitted inside the call, not a loop turn later');
        $stream_io->close_now;
        shutdown_server($server);
    }

    my @lines = grep { ($_->{message} // q{}) =~ $CROAK_RE } @LOG[$mark .. $#LOG];
    is(scalar(@lines), 0, 'nothing croaked about a session callback')
        or diag(join qq{\n}, map { $_->{message} // q{} } @lines);
    is($ALARM_FIRED, 0, 'no pump alarm');
};

# ============================================================
# (d) the whole file
# ============================================================
subtest 'no session-callback croak anywhere in this file' => sub {
    my @lines = grep { ($_->{message} // q{}) =~ $CROAK_RE } @LOG;
    is(scalar(@lines), 0, 'no collected log line names a flush from inside a session callback')
       
        or diag(join qq{\n}, map { $_->{message} // q{} } @lines);
};

done_testing;
