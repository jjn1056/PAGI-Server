use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use IO::Socket::INET;
use Socket qw(SO_RCVBUF);
use MIME::Base64 ();
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

sub create_server {
    my ($app, %opts) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, shutdown_timeout => 1,
        %opts,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub ws_request {
    my ($port) = @_;
    return "GET /socket HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
         . "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: "
         . MIME::Base64::encode_base64('0123456789abcdef', '') . "\r\n\r\n";
}
sub sse_request { my ($port) = @_; return "GET /events HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n" }

# Drive a raw client: send $request, pump until $until->() or $rounds, optionally close.
sub drive {
    my (%a) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $a{port}, Proto => 'tcp', Timeout => 5) or die $!;
    print $sock $a{request};
    $sock->blocking(0);
    my $wire = '';
    for (1 .. ($a{rounds} // 60)) {
        last if $a{until} && $a{until}->();
        my $b; my $n = sysread($sock, $b, 65536); $wire .= $b if $n;
        last if defined $n && $n == 0;
        $loop->loop_once(0.05);
    }
    close $sock if $a{close};
    # Keep reading during the settle pump: `until` can flip true on the same
    # tick the server flushes its final bytes (e.g. the chunked terminator),
    # and the loop above already moved on to `last` without a final sysread
    # for that tick -- without this, $wire would be missing exactly the
    # bytes written in that last round.
    for (1 .. 20) {
        $loop->loop_once(0.05);
        last if $a{close};   # nothing more to read from an already-closed socket
        my $b; my $n = sysread($sock, $b, 65536); $wire .= $b if $n;
    }
    return ($wire, $sock);
}

for my $type (qw(websocket sse)) {
    subtest "h1 $type scope carries a pagi.connection with the full interface" => sub {
        my %seen;
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            return unless $scope->{type} eq $type;
            my $c = $scope->{'pagi.connection'};
            $seen{present} = defined $c ? 1 : 0;
            $seen{methods} = [grep { !$c->can($_) } qw(is_connected disconnect_reason disconnect_detail on_disconnect on_complete disconnect_future response_started response_complete abort)];
            $seen{started_before} = $c->response_started;
            await $receive->();
            $seen{done} = 1;
            return;   # no response: exercised elsewhere
        };
        my $server = create_server($app);
        drive(port => $server->port, request => ($type eq 'websocket' ? ws_request($server->port) : sse_request($server->port)),
              until => sub { $seen{done} }, close => 1);
        $server->shutdown->get;
        is($seen{present}, 1, 'object present');
        is($seen{methods}, [], 'all nine methods present');
        is($seen{started_before}, 0, 'response_started false before any response');
    };
}

subtest 'h1 websocket: client drop before accept marks client_closed on the object' => sub {
    my %r;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $c = $scope->{'pagi.connection'};
        $c->on_disconnect(sub { $r{cb} = [@_] });
        $c->on_complete(sub { $r{complete}++ });
        await $receive->();                       # websocket.connect
        $r{parked} = 1;
        my $ev = await $receive->();              # parks until the drop
        $r{event} = $ev;
        $r{reason} = $c->disconnect_reason;
        $r{connected} = $c->is_connected;
        $r{done} = 1;
    };
    my $server = create_server($app);
    drive(port => $server->port, request => ws_request($server->port), until => sub { $r{parked} }, close => 1);
    $loop->loop_once(0.05) for 1 .. 20;
    $server->shutdown->get;
    is($r{done}, 1, 'app finished');
    is($r{event}{type}, 'websocket.disconnect', 'websocket.disconnect on a websocket scope (S1)');
    is($r{event}{reason}, 'client_closed', 'event reason');
    is($r{reason}, 'client_closed', 'object reason agrees');
    is($r{connected}, 0, 'not connected');
    is($r{cb}[0], 'client_closed', 'on_disconnect fired with the token');
    ok(!$r{complete}, 'on_complete did not fire');
};

subtest 'h1 websocket: accepted socket, peer Close frame, is a clean end' => sub {
    my %r;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $c = $scope->{'pagi.connection'};
        $c->on_complete(sub { $r{complete}++ });
        $c->on_disconnect(sub { $r{disc} = [@_] });
        await $receive->();
        await $send->({ type => 'websocket.accept' });
        $r{accepted} = 1;
        my $ev = await $receive->();
        $r{event} = $ev;
        $r{done} = 1;
    };
    my $server = create_server($app);
    my $close = chr(0x88) . chr(0x82) . pack('N', 0x11223344) . (pack('n', 1000) ^ pack('a2', substr(pack('N', 0x11223344), 0, 2)));
    my ($wire, $sock) = drive(port => $server->port, request => ws_request($server->port), until => sub { $r{accepted} });
    print $sock $close;   # masked Close 1000
    $loop->loop_once(0.05) for 1 .. 30;
    close $sock;
    $loop->loop_once(0.05) for 1 .. 20;
    $server->shutdown->get;
    is($r{event}{type}, 'websocket.disconnect', 'disconnect event delivered');
    is($r{event}{code}, 1000, 'peer code is protocol data');
    is($r{complete}, 1, 'on_complete fired once');
    ok(!$r{disc}, 'on_disconnect did not fire');
};

subtest 'h1 sse: client drop before start marks client_closed; sse.close is a clean end; return without it is incomplete (D12)' => sub {
    my %r;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'sse';
        my $c = $scope->{'pagi.connection'};
        $c->on_disconnect(sub { $r{disc} = [@_] });
        $c->on_complete(sub { $r{complete}++ });
        await $receive->();                       # sse.request
        if ($scope->{path} eq '/events') {
            $r{parked} = 1;
            my $ev = await $receive->();
            $r{event} = $ev; $r{reason} = $c->disconnect_reason; $r{done} = 1;
            return;
        }
        await $send->({ type => 'sse.start', status => 200, headers => [['content-type', 'text/event-stream']] });
        await $send->({ type => 'sse.send', data => 'x' });
        await $send->({ type => 'sse.close' }) if $scope->{path} eq '/stream';
        $r{done} = 1;
        return;                                   # /stream: clean end via sse.close; /abandon: incomplete
    };
    my $server = create_server($app);
    drive(port => $server->port, request => sse_request($server->port), until => sub { $r{parked} }, close => 1);
    $loop->loop_once(0.05) for 1 .. 20;
    is($r{event}{type}, 'sse.disconnect', 'sse.disconnect before start');
    is($r{reason}, 'client_closed', 'object agrees');
    is($r{disc}[0], 'client_closed', 'callback');

    %r = ();
    my $req = "GET /stream HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n";
    my ($wire) = drive(port => $server->port, request => $req, until => sub { $r{done} }, rounds => 80);
    $loop->loop_once(0.05) for 1 .. 20;
    is($r{complete}, 1, 'on_complete fired once after sse.close');
    ok(!$r{disc}, 'no on_disconnect');
    like($wire, qr/\r\n0\r\n\r\n\z/, 'chunked terminator on the wire');

    %r = ();
    $req = "GET /abandon HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n";
    ($wire) = drive(port => $server->port, request => $req, until => sub { $r{done} }, rounds => 80);
    $loop->loop_once(0.05) for 1 .. 20;
    $server->shutdown->get;
    ok(!$r{complete}, 'on_complete did not fire for a return without sse.close');
    is($r{disc}[0], 'server_error', 'on_disconnect server_error (incomplete response)');
    unlike($wire, qr/\r\n0\r\n\r\n\z/, 'no terminator synthesized; connection closed without it');
};

subtest 'h1 sse: abort() after sse.start reports app_abort on both the event and the object' => sub {
    my %r;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'sse';
        my $c = $scope->{'pagi.connection'};
        $c->on_disconnect(sub { $r{cb} = [@_] });
        await $receive->();                       # sse.request
        await $send->({ type => 'sse.start', status => 200 });
        $c->abort('quota exceeded');
        my $ev = await $receive->();               # settled by abort's own teardown
        $r{event} = $ev;
        $r{reason} = $c->disconnect_reason;
        $r{detail} = $c->disconnect_detail;
        $r{done} = 1;
    };
    my $server = create_server($app);
    drive(port => $server->port, request => sse_request($server->port), until => sub { $r{done} }, rounds => 80);
    $loop->loop_once(0.05) for 1 .. 10;
    $server->shutdown->get;
    is($r{event}{type}, 'sse.disconnect', 'sse.disconnect delivered');
    is($r{event}{reason}, 'app_abort',
        'event reason agrees with the object (Www.pod "Agreement with disconnect events")');
    is($r{reason}, 'app_abort', 'object disconnect_reason is app_abort');
    is($r{detail}, 'quota exceeded', 'object disconnect_detail carries the app string');
    is($r{cb}[0], 'app_abort', 'on_disconnect token is app_abort');
    is($r{cb}[1], 'quota exceeded', 'on_disconnect detail is the app string');
};

# I3: abort() MUST NOT wait for an in-flight write to drain (Www.pod
# "Connection Object Interface"). Pattern (small watermarks + a shrunk client
# receive buffer to park a send quickly) lifted from
# t/61-pending-io-at-disconnect.t's "websocket: parked send resolves at
# abrupt disconnect" subtest; here the parked send is resolved by abort()
# instead of by the client disconnecting, and B6 will extend this subtest.
subtest 'h1 websocket: abort() while a send is parked on backpressure resolves it and closes now (I3)' => sub {
    my %obs = (started => 0, completed => 0);
    my $payload = 'w' x 16384;
    my $cs;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        $cs = $scope->{'pagi.connection'};
        await $receive->();
        await $send->({ type => 'websocket.accept' });
        for my $n (1 .. 400) {
            $obs{started}++;
            my $ok = eval { await $send->({ type => 'websocket.send', bytes => $payload }); 1 };
            unless ($ok) { $obs{send_failed} = "$@"; last }
            $obs{completed}++;
        }
        $obs{app_completed} = 1;
        return;
    };

    my $server = create_server($app, write_high_watermark => 8192, write_low_watermark => 2048);
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 5,
    ) or die $!;
    $sock->sockopt(SO_RCVBUF, 4096);   # shrink so an unread response backs up quickly
    $sock->blocking(0);
    print $sock ws_request($server->port);

    my $got = '';
    for (1 .. 100) {
        my $buf; my $n = sysread($sock, $buf, 4096);
        $got .= $buf if defined $n && $n > 0;
        last if $got =~ /HTTP\/1\.1 101/;
        $loop->loop_once(0.05);
    }
    like($got, qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');

    # Stop reading; wait for the flood to park (started outruns completed and
    # stays that way across a settle tick).
    my $parked = 0;
    for (1 .. 80) {
        if ($obs{started} == $obs{completed} + 1) {
            my ($s, $c) = ($obs{started}, $obs{completed});
            $loop->loop_once(0.1);
            $parked = ($obs{started} == $s && $obs{completed} == $c);
            last if $parked;
        }
        $loop->loop_once(0.1);
    }
    ok($parked, 'a websocket send is parked on backpressure')
        or diag("started=$obs{started} completed=$obs{completed}");
    ok($cs, 'app captured a connection_state');

    $cs->abort('overloaded');   # MUST NOT wait for the parked write to drain

    my $app_completed = 0;
    for (1 .. 20) {
        $app_completed = 1, last if $obs{app_completed};
        $loop->loop_once(0.05);
    }
    ok($app_completed, 'application completed after abort (parked send resolved, not left hanging)');
    ok(!$obs{send_failed}, 'the parked send resolved successfully, not failed')
        or diag("send failed with: $obs{send_failed}");

    # The socket should close within a few more loop turns: close_now, not
    # close_when_empty waiting on a peer (this one) that never reads.
    my $closed = 0;
    for (1 .. 20) {
        my $buf; my $n = sysread($sock, $buf, 65536);
        if (defined $n && $n == 0) { $closed = 1; last }
        $loop->loop_once(0.05);
    }
    ok($closed, 'the socket closed within a few loop turns after abort');

    close $sock;
    $server->shutdown->get;
};

# D13 (Www.pod "Application Left a Response Incomplete"): an accepted socket
# that returns without sending websocket.close and without having received
# websocket.disconnect is incomplete, not a clean end. The brief's Step 1
# suite had no h1 case for this (only the h2 dispatch wrapper's own probe in
# the review); this fills that gap.
subtest 'h1 websocket: accepted socket returns without a closing handshake is an incomplete response (D13)' => sub {
    my %r;
    my @log_events;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $c = $scope->{'pagi.connection'};
        $r{cs} = $c;
        $c->on_disconnect(sub { $r{disc} = [@_] });
        $c->on_complete(sub { $r{complete}++ });
        await $receive->();                        # websocket.connect
        await $send->({ type => 'websocket.accept' });
        $r{accepted} = 1;
        my $msg = await $receive->();               # one inbound frame
        await $send->({ type => 'websocket.send', text => $msg->{text} });   # echo it
        $r{done} = 1;
        return;   # no websocket.close sent, no websocket.disconnect received
    };
    my $server = create_server($app, logger => sub { push @log_events, $_[0] });

    my ($wire, $sock) = drive(port => $server->port, request => ws_request($server->port),
        until => sub { $r{accepted} });

    # One masked client->server text frame ("hi"), per RFC 6455 5.3.
    my $mask = pack('N', 0x11223344);
    my $payload = 'hi';
    my $masked = $payload ^ substr($mask x 4, 0, length($payload));
    print $sock chr(0x81) . chr(0x80 | length($payload)) . $mask . $masked;

    for (1 .. 40) {
        last if $r{done};
        my $buf; my $n = sysread($sock, $buf, 65536); $wire .= $buf if $n;
        $loop->loop_once(0.05);
    }
    for (1 .. 20) {
        $loop->loop_once(0.05);
        my $buf; my $n = sysread($sock, $buf, 65536); $wire .= $buf if $n;
    }
    close $sock;
    $server->shutdown->get;

    ok($r{done}, 'app echoed the frame and returned');
    like($wire, qr/\x88\x02\x03\xf3/,
        'Close frame (opcode 0x88) with code 1011 on the wire')
        or diag('wire: ' . unpack('H*', $wire));
    is($r{cs}->is_connected, 0, 'is_connected false');
    is($r{disc}[0], 'server_error', 'on_disconnect fired with server_error');
    ok(!$r{complete}, 'on_complete did not fire');

    my @errors = grep { ($_->{level} // '') eq 'error' } @log_events;
    is(scalar(@errors), 1, 'exactly one error log line');
    like($errors[0]{message}, qr/returned from an accepted WebSocket without a closing handshake/,
        'the log line names the incomplete-response condition')
        if @errors;
};

done_testing;
