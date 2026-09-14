use strict;
use warnings;

# This file asserts a warning-free run around a dropped without_cancel
# observer, which Future::XS 0.15 warns about ("lost a sequence Future";
# reported to the Future-XS RT queue) and Future::PP does not. Future reads
# PERL_FUTURE_NO_XS when it is compiled, so it is set before anything below
# loads Future, and the file needs the pure-perl implementation to be there.
BEGIN { $ENV{PERL_FUTURE_NO_XS} = 1 }
use Test2::V0;
BEGIN { eval { require Future::PP; 1 } or plan skip_all => 'Future::PP required' }
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use Future;
use IO::Socket::INET;
use Socket qw(AF_UNIX SOCK_STREAM);
use MIME::Base64 ();
use Scalar::Util qw(weaken);
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Regression: what a refusal retains after the scope has ended
# ============================================================
# A port of the S6 retention probe used while auditing sub-spec 0.6 refusals
# (its refusal events, which were the 0.5 websocket.http.response.* names, are
# the 0.6 http.response.* events here -- see PAGI::Spec::Www, "Refusing the
# handshake").
#
# Two cases, on both transports, distinguished by how the application leaves
# the scope:
#
#   Case B  The refusal completes normally and the application returns. The
#           receive() it left outstanding while the refusal was still going
#           out is answered with the scope's end (Www.pod "Receiving after
#           the scope's end"), and nothing is retained: the application
#           coroutine is collected, that receive Future is collected, and the
#           scope hash is gone. Asserted so a future change cannot turn that
#           receive back into a leak.
#
#   Case A  The application is still suspended on a Future nobody will ever
#           resolve (a pool query, a lookup) when the client drops mid-refusal.
#           Its coroutine, and through it the scope, stay alive. This is the
#           application-side retention the spec's "The Problem" paragraph under
#           "Connection State" describes: an application that parks on
#           something external and never consults its connection object holds
#           the scope for as long as it parks. It is documented here, not
#           fixed: only the application can release that Future.

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

my $have_h2 = do {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available ? 1 : 0;
};

# Bumps a counter when it is collected, so a scalar can stand in for "this
# thing was freed".
package RetentionGuard {
    sub new     { my ($class, $flag) = @_; return bless { flag => $flag }, $class }
    sub DESTROY { ${ $_[0]{flag} }++ }
}

# Case B abandons its outstanding receive and returns in the same turn, so the
# connection tears down under a call the server has just answered. An answer
# that arrived late, or twice, does not fail an assertion on its own -- it
# complains on STDERR, as a dropped returning future or an already-done
# Future. Collected here so case B can assert that its window produced none.
my @WARNINGS;
$SIG{__WARN__} = sub { push @WARNINGS, $_[0] };

sub warnings_since {
    my ($mark) = @_;
    return [map { my $w = $_; $w =~ s/\s+\z//r } @WARNINGS[$mark .. $#WARNINGS]];
}

sub make_app {
    my ($case, $r) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $guard = RetentionGuard->new(\$r->{app_freed});  # freed with the coroutine
        $r->{scope_weak} = $scope;
        weaken($r->{scope_weak});
        await $receive->();                                 # websocket.connect
        await $send->({ type => 'http.response.start', status => 429,
                        headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'slow down: ', more => 1 });

        if ($case eq 'A') {
            $r->{lookup} = Future->new;                     # held externally, like a pool query
            my $detail = await $r->{lookup};                # never resolves
            await $send->({ type => 'http.response.body', body => $detail });
            return;
        }

        my $outstanding = $receive->();                     # outstanding across the refusal
        my $rg = RetentionGuard->new(\$r->{receive_freed});
        $outstanding->on_ready(sub { my $keep = $rg });      # the guard lives as long as it does
        my $detail = await Future->wait_any(Future->done('ok'), $outstanding->without_cancel);
        await $send->({ type => 'http.response.body', body => $detail });
        $r->{app_returned} = 1;
        return;
    };
}

sub ws_request {
    return "GET /socket HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
         . "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: "
         . MIME::Base64::encode_base64('0123456789abcdef', '') . "\r\n\r\n";
}

# Run one case over HTTP/1.1; returns the observations hash and the wire.
sub h1_case {
    my ($case) = @_;
    my %r;
    my $server = PAGI::Server->new(app => make_app($case, \%r), host => '127.0.0.1',
        port => 0, quiet => 1, shutdown_timeout => 1);
    $loop->add($server);
    $server->listen->get;

    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port,
        Proto => 'tcp', Timeout => 5) or die "connect: $!";
    print $sock ws_request();
    $sock->blocking(0);
    my $wire = '';
    for (1 .. 20) {
        my $b; my $n = sysread($sock, $b, 65536);
        $wire .= $b if $n;
        last if defined $n && $n == 0;
        $loop->loop_once(0.05);
    }
    close $sock;                       # client gone (mid-lookup for A, after the response for B)
    $loop->loop_once(0.05) for 1 .. 40;

    $server->shutdown->get;
    $loop->remove($server) if $server->loop;
    return (\%r, $wire);
}

# Run one case over HTTP/2; returns the observations hash, the response status,
# and a second observations snapshot taken after the Connection is dropped.
sub h2_case {
    my ($case) = @_;
    my %r;
    my $app = make_app($case, \%r);
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0,
        quiet => 1, http2 => 1);
    $loop->add($server);

    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $_->blocking(0) for $sock_a, $sock_b;
    my $stream = IO::Async::Stream->new(read_handle => $sock_a, write_handle => $sock_a,
        on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(stream => $stream, app => $app,
        protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2');
    $server->add_child($stream);
    $conn->start;

    require Net::HTTP2::nghttp2::Session;
    my %headers;
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_frame_recv      => sub { 0 },
        on_stream_close    => sub { 0 },
        on_data_chunk_recv => sub { 0 },
        on_header          => sub { my (undef, $n, $v) = @_; $headers{lc $n} = $v; 0 },
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

    $client->submit_request(method => 'CONNECT', path => '/socket', scheme => 'https',
        authority => 'localhost',
        headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
        body => sub { undef });
    $sock_b->syswrite($client->mem_send);
    for (1 .. 20) {
        $loop->loop_once(0.05);
        my $buf = ''; $sock_b->sysread($buf, 65536);
        $client->mem_recv($buf) if length $buf;
        my $o = $client->mem_send; $sock_b->syswrite($o) if length $o;
    }
    close $sock_b;
    $loop->loop_once(0.05) for 1 .. 40;

    my %before = %r;
    $stream->close_now;
    eval { $loop->remove($server) };
    undef $conn;
    $loop->loop_once(0.05) for 1 .. 10;

    return (\%before, $headers{':status'} // '', \%r);
}

sub freed { my ($r, $key) = @_; return $r->{$key} // 0 }
sub scope_alive { my ($r) = @_; return defined $r->{scope_weak} ? 1 : 0 }

# ============================================================
# Case B: a completed refusal retains nothing, its outstanding receive included
# ============================================================

subtest 'h1: a completed refusal frees the app, the answered receive and the scope' => sub {
    my $warn_mark = scalar @WARNINGS;
    my ($r, $wire) = h1_case('B');
    is(warnings_since($warn_mark), [], 'the run emitted no warnings');
    like($wire, qr{^HTTP/1\.1 429}, 'the refusal reached the client');
    # Two chunks, so two chunked frames, then the terminator.
    like($wire, qr/\r\nslow down: \r\n.*\r\nok\r\n0\r\n\r\n\z/s,
        'both body chunks and the terminator reached the client');
    is($r->{app_returned}, 1, 'the application returned');
    is(freed($r, 'app_freed'), 1, 'the application coroutine was collected');
    is(freed($r, 'receive_freed'), 1, 'the outstanding receive Future was collected');
    is(scope_alive($r), 0, 'the scope hash is gone');
};

subtest 'h2: a completed refusal frees the app, the answered receive and the scope' => sub {
    skip_all 'HTTP/2 not available' unless $have_h2;
    my $warn_mark = scalar @WARNINGS;
    my ($before, $status, $after) = h2_case('B');
    is(warnings_since($warn_mark), [], 'the run emitted no warnings');
    is($status, '429', 'the refusal reached the client');
    is($before->{app_returned}, 1, 'the application returned');
    is(freed($before, 'app_freed'), 1, 'the application coroutine was collected');
    is(freed($before, 'receive_freed'), 1, 'the outstanding receive Future was collected');
    is(scope_alive($before), 0, 'the scope hash is gone');
};

# ============================================================
# Case A: an application parked on an unresolvable Future keeps its own scope
# ============================================================

subtest 'h1: an app suspended on a Future nobody resolves retains its own scope' => sub {
    my ($r, $wire) = h1_case('A');
    is($r->{app_returned}, undef, 'the application never returned');
    is(freed($r, 'app_freed'), 0, 'its coroutine is still alive');
    is(scope_alive($r), 1, 'and through it, so is the scope');
    ok($r->{lookup} && !$r->{lookup}->is_ready, 'the Future it parked on is still pending');
};

subtest 'h2: an app suspended on a Future nobody resolves retains its own scope' => sub {
    skip_all 'HTTP/2 not available' unless $have_h2;
    my ($before, $status, $after) = h2_case('A');
    is($before->{app_returned}, undef, 'the application never returned');
    is(freed($before, 'app_freed'), 0, 'its coroutine is still alive');
    is(scope_alive($before), 1, 'and through it, so is the scope');
    is(freed($after, 'app_freed'), 0, 'dropping the Connection does not free it either');
};

done_testing;
