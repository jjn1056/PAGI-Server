use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# A server shutting down ends each accepted WebSocket with a Close 1012
# ("Service Restart", IANA WebSocket close code registry) rather than dropping
# the connection, so the client sees a planned restart, not a 1006 abnormal
# closure. The application's websocket.disconnect reports the same code, with
# reason server_shutdown. HTTP/2: t/http2/ws-shutdown-close-h2.t.

my $loop = IO::Async::Loop->new;

sub pump_until {
    my ($cond, $timeout) = @_;
    my $deadline = time + ($timeout // 10);
    while (time < $deadline) {
        return 1 if $cond->();
        $loop->loop_once(0.02);
    }
    return $cond->() ? 1 : 0;
}

sub start_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1,
        access_log => undef, shutdown_timeout => 2,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub ws_connect {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp')
        or die "connect: $!";
    $sock->blocking(0);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n");
    my $got = '';
    pump_until(sub { my $n = sysread($sock, my $buf, 4096); $got .= $buf if $n; $got =~ /\r\n\r\n/ }, 5);
    return ($sock, $got);
}

# Server frames are unmasked: [opcode, payload] for each complete frame.
sub frames {
    my ($bytes) = @_;
    my @frames;
    while (length $bytes >= 2) {
        my ($b0, $b1) = unpack 'CC', $bytes;
        my ($len, $off) = ($b1 & 0x7f, 2);
        if    ($len == 126) { $len = unpack 'n', substr($bytes, 2, 2); $off = 4 }
        elsif ($len == 127) { $len = unpack 'Q>', substr($bytes, 2, 8); $off = 10 }
        last if length $bytes < $off + $len;
        push @frames, [$b0 & 0x0f, substr($bytes, $off, $len)];
        substr($bytes, 0, $off + $len, '');
    }
    return @frames;
}

# Shut the server down while reading what the client receives, to EOF.
sub shutdown_and_read {
    my ($server, $sock) = @_;
    my $rx = '';
    my $done = $server->shutdown;
    pump_until(sub {
        my $n = sysread($sock, my $buf, 65536);
        $rx .= $buf if $n;
        $done->is_ready && defined $n && $n == 0;
    }, 10);
    eval { $loop->remove($server) };
    return $rx;
}

# Accepts, then records the disconnect event and the connection's reason.
# When told 'close yourself', sends its own Close first.
sub recording_app {
    my ($obs) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $conn = $scope->{'pagi.connection'};
        await $receive->();
        await $send->({ type => 'websocket.accept' });
        $obs->{accepted} = 1;
        while (1) {
            my $event = await $receive->();
            if ($event->{type} eq 'websocket.receive' && ($event->{text} // '') eq 'close yourself') {
                await $send->({ type => 'websocket.close', code => 1000, reason => 'app' });
                $obs->{app_closed} = 1;
                next;
            }
            next unless $event->{type} eq 'websocket.disconnect';
            $obs->{code}    = $event->{code};
            $obs->{reason}  = $event->{reason};
            $obs->{dreason} = $conn->disconnect_reason;
            return;
        }
    };
}

subtest 'an open WebSocket is closed with 1012 at shutdown' => sub {
    my %obs;
    my $server = start_server(recording_app(\%obs));
    my ($sock, $head) = ws_connect($server->port);
    like($head, qr{^HTTP/1\.1 101}, 'upgraded');
    ok(pump_until(sub { $obs{accepted} }, 5), 'the app accepted');

    my @frames = frames(shutdown_and_read($server, $sock));
    is(scalar @frames, 1, 'one frame before the connection ended');
    is($frames[0][0], 8, 'a Close frame');
    is(unpack('n', $frames[0][1] // "\0\0"), 1012, 'with code 1012 (Service Restart)');

    is($obs{code},    1012,              'the app event reports 1012');
    is($obs{reason},  'server_shutdown', 'and reason server_shutdown');
    is($obs{dreason}, 'server_shutdown', 'disconnect_reason is server_shutdown');
    close $sock;
};

subtest 'an app that already sent its Close gets no second one' => sub {
    my %obs;
    my $server = start_server(recording_app(\%obs));
    my ($sock) = ws_connect($server->port);
    ok(pump_until(sub { $obs{accepted} }, 5), 'the app accepted');
    # Masked client text frame: 'close yourself'.
    my $text = 'close yourself';
    my $mask = pack 'N', 0x01020304;
    my $masked = join '', map { chr(ord(substr($text, $_, 1)) ^ ord(substr($mask, $_ % 4, 1))) } 0 .. length($text) - 1;
    syswrite($sock, chr(0x81) . chr(0x80 | length $text) . $mask . $masked);
    ok(pump_until(sub { $obs{app_closed} }, 5), 'the app sent its own Close');

    my @closes = grep { $_->[0] == 8 } frames(shutdown_and_read($server, $sock));
    is(scalar @closes, 1, 'exactly one Close on the wire');
    is(unpack('n', $closes[0][1] // "\0\0"), 1000, "and it is the app's own (1000)");
    close $sock;
};

done_testing;
