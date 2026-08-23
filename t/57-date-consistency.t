use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

eval { require Future::IO::Impl::IOAsync; 1 }
    or plan skip_all => 'Future::IO::Impl::IOAsync required for these tests';

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Design section 13.1: the server supplies a Date header only when the
# application did not provide one, on EVERY response path (HTTP/1.1
# normal responses, SSE decline, WebSocket denial -- HTTP/2 equivalents
# are covered by t/http2/21-http-date-header.t). An app-supplied Date
# must be preserved verbatim and must never be duplicated on the wire.
# ============================================================

my $loop = IO::Async::Loop->new;

sub create_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, shutdown_timeout => 1,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

# Reads raw bytes off a socket until $done->($wire_so_far) is true or the
# deadline elapses. Returns the accumulated wire bytes.
sub read_until {
    my ($sock, $done) = @_;
    $sock->blocking(0);
    my $wire = '';
    my $deadline = time + 5;
    while (time < $deadline) {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        if (defined $n && $n > 0) {
            $wire .= $buf;
        }
        elsif (defined $n && $n == 0) {
            last;   # connection closed
        }
        $loop->loop_once(0.05);
        last if $done->($wire);
    }
    return $wire;
}

sub date_lines_in {
    my ($wire) = @_;
    my ($header_block) = $wire =~ /\A(.*?)\r\n\r\n/s;
    $header_block //= '';
    return [ $header_block =~ /^date:.*$/mig ];
}

my $APP_DATE = 'Tue, 01 Jan 2030 00:00:00 GMT';

# ---------------------------------------------------------------------------
# (a) h1 normal http.response.start: app supplies Date -> exactly one on wire
# ---------------------------------------------------------------------------
subtest 'h1 normal response: app-supplied Date is not duplicated' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [
                [ 'content-type', 'text/plain' ],
                [ 'Date',         $APP_DATE ],
            ],
        });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    };

    my $server = create_server($app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 2 unless $sock;

        print $sock "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nConnection: close\r\n\r\n";
        my $wire = read_until($sock, sub { $_[0] =~ /\bok\z/ });
        close $sock;

        my $date_lines = date_lines_in($wire);
        is(scalar(@$date_lines), 1, 'exactly one Date header on the wire');
        like($date_lines->[0] // '', qr/\Q$APP_DATE\E/,
            'app-supplied Date is preserved, not overridden with the server clock');
    }

    $server->shutdown->get;
};

# ---------------------------------------------------------------------------
# (c1) h1 SSE decline: app supplies Date -> exactly one on wire
# ---------------------------------------------------------------------------
subtest 'h1 SSE decline: app-supplied Date is not duplicated' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({
            type    => 'sse.http.response.start',
            status  => 404,
            headers => [
                [ 'content-type', 'text/plain' ],
                [ 'Date',         $APP_DATE ],
            ],
        });
        await $send->({ type => 'sse.http.response.body', body => 'No such stream', more => 0 });
    };

    my $server = create_server($app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 2 unless $sock;

        print $sock "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nAccept: text/event-stream\r\n\r\n";
        my $wire = read_until($sock, sub { $_[0] =~ /No such stream/ });
        close $sock;

        my $date_lines = date_lines_in($wire);
        is(scalar(@$date_lines), 1, 'exactly one Date header on the wire');
        like($date_lines->[0] // '', qr/\Q$APP_DATE\E/,
            'app-supplied Date is preserved, not overridden with the server clock');
    }

    $server->shutdown->get;
};

# ---------------------------------------------------------------------------
# (c2) h1 WebSocket denial: app supplies Date -> exactly one on wire
# ---------------------------------------------------------------------------
subtest 'h1 WebSocket denial: app-supplied Date is not duplicated' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();   # websocket.connect
        await $send->({
            type    => 'websocket.http.response.start',
            status  => 401,
            headers => [
                [ 'content-type', 'application/json' ],
                [ 'Date',         $APP_DATE ],
            ],
        });
        await $send->({ type => 'websocket.http.response.body', body => '{"error":"unauthorized"}' });
        return;
    };

    my $server = create_server($app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 2 unless $sock;

        my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
        print $sock "GET / HTTP/1.1\r\n";
        print $sock "Host: 127.0.0.1:$port\r\n";
        print $sock "Upgrade: websocket\r\n";
        print $sock "Connection: Upgrade\r\n";
        print $sock "Sec-WebSocket-Key: $key\r\n";
        print $sock "Sec-WebSocket-Version: 13\r\n";
        print $sock "\r\n";

        my $wire = read_until($sock, sub { $_[0] =~ /"error":"unauthorized"/ });
        close $sock;

        my $date_lines = date_lines_in($wire);
        is(scalar(@$date_lines), 1, 'exactly one Date header on the wire');
        like($date_lines->[0] // '', qr/\Q$APP_DATE\E/,
            'app-supplied Date is preserved, not overridden with the server clock');
    }

    $server->shutdown->get;
};

done_testing;
