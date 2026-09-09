use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use IO::Socket::INET;
use MIME::Base64 ();
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

sub create_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1, shutdown_timeout => 1);
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
    {
        # S1, fixed in B3: pre-accept drop still delivers http.disconnect
        # (the scope-kind-aware event type is Task B3's fix), not
        # websocket.disconnect.
        my $todo = todo('S1, fixed in B3');
        is($r{event}{type}, 'websocket.disconnect', 'websocket.disconnect on a websocket scope (S1)');
        is($r{event}{reason}, 'client_closed', 'event reason');
    }
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

done_testing;
