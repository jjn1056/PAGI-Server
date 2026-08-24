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
# PAGI spec (main @ 50d8a4c): "Over HTTP/1.1 the server must ignore or strip
# application-supplied Transfer-Encoding and Connection -- it supplies its
# own -- and SHOULD log when it does." Unlike the HTTP/2 six-name strip
# (design 13.3 / RFC 9113 8.2.2), HTTP/1.1 only strips these two names;
# keep-alive, proxy-connection, upgrade, and te are HTTP/1.1-legal
# application headers and are left alone.
#
# Before this fix, an app-supplied Transfer-Encoding or Connection response
# header on h1 reached the wire verbatim, alongside whatever framing header
# the server itself decided to emit -- producing a duplicate/conflicting
# Transfer-Encoding line, or a Connection line that contradicts what the
# server actually does with the socket.
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

sub header_block_of {
    my ($wire) = @_;
    my ($block) = $wire =~ /\A(.*?)\r\n\r\n/s;
    return $block // '';
}

sub header_lines_matching {
    my ($wire, $name) = @_;
    my $block = header_block_of($wire);
    return [ $block =~ /^\Q$name\E:.*$/mig ];
}

sub strip_warnings_matching {
    my ($warnings, $name) = @_;
    return [ grep { /connection-specific header '\Q$name\E' stripped from HTTP\/1\.1 response/ } @$warnings ];
}

# ---------------------------------------------------------------------------
# (a) h1 http.response.start, Content-Length framing
# ---------------------------------------------------------------------------
subtest 'h1 http.response.start (Content-Length): app TE/Connection stripped, not duplicated' => sub {
    my $body = 'Hello World';
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [
                [ 'transfer-encoding', 'chunked' ],
                [ 'connection',        'close' ],
                [ 'content-type',      'text/plain' ],
                [ 'content-length',    length($body) ],
            ],
        });
        await $send->({ type => 'http.response.body', body => $body, more => 0 });
    };

    my $server = create_server($app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 6 unless $sock;

        my @warnings;
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };

        print $sock "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nConnection: close\r\n\r\n";
        my $wire = read_until($sock, sub { $_[0] =~ /\Q$body\E\z/ });
        close $sock;

        is(header_lines_matching($wire, 'transfer-encoding'), [],
            "app-supplied Transfer-Encoding does not reach the wire alongside Content-Length");
        is(header_lines_matching($wire, 'connection'), [],
            "app-supplied Connection is stripped (server adds none of its own here)");
        is(scalar(@{header_lines_matching($wire, 'content-length')}), 1,
            'exactly one Content-Length header');
        like($wire, qr/content-type:\s*text\/plain/i, 'ordinary app header preserved');
        like($wire, qr/\Q$body\E\z/, 'body arrives intact');

        is(scalar(@{strip_warnings_matching(\@warnings, 'transfer-encoding')}), 1,
            "warns once for stripped 'transfer-encoding'");
        is(scalar(@{strip_warnings_matching(\@warnings, 'connection')}), 1,
            "warns once for stripped 'connection'");
    }

    $server->shutdown->get;
};

# ---------------------------------------------------------------------------
# (b) h1 http.response.start, chunked framing (no Content-Length)
# ---------------------------------------------------------------------------
subtest 'h1 http.response.start (chunked): app TE/Connection stripped, single Transfer-Encoding line' => sub {
    my $body = 'Hello World';
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [
                [ 'transfer-encoding', 'chunked' ],
                [ 'connection',        'close' ],
                [ 'content-type',      'text/plain' ],
            ],
        });
        await $send->({ type => 'http.response.body', body => $body, more => 0 });
    };

    my $server = create_server($app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 5 unless $sock;

        my @warnings;
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };

        print $sock "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nConnection: close\r\n\r\n";
        my $wire = read_until($sock, sub { $_[0] =~ /\Q$body\E/ && $_[0] =~ /0\r\n\r\n\z/ });
        close $sock;

        is(scalar(@{header_lines_matching($wire, 'transfer-encoding')}), 1,
            'exactly one Transfer-Encoding header (server-owned, app duplicate stripped)');
        like(header_lines_matching($wire, 'transfer-encoding')->[0] // '', qr/chunked/i,
            'the surviving Transfer-Encoding header says chunked');
        is(header_lines_matching($wire, 'connection'), [],
            'app-supplied Connection is stripped');
        like($wire, qr/\Q$body\E/, 'chunked body arrives intact');

        is(scalar(@{strip_warnings_matching(\@warnings, 'transfer-encoding')}), 1,
            "warns once for stripped 'transfer-encoding'");
        is(scalar(@{strip_warnings_matching(\@warnings, 'connection')}), 1,
            "warns once for stripped 'connection'");
    }

    $server->shutdown->get;
};

# ---------------------------------------------------------------------------
# (c) h1 sse.start
# ---------------------------------------------------------------------------
subtest 'h1 sse.start: app TE/Connection stripped, server-owned values survive undoubled' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({
            type    => 'sse.start',
            status  => 200,
            headers => [
                [ 'connection',        'close' ],
                [ 'transfer-encoding', 'gzip' ],
                [ 'x-marker',          'ok' ],
            ],
        });
        await $send->({ type => 'sse.send', data => 'hello' });
    };

    my $server = create_server($app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 6 unless $sock;

        my @warnings;
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };

        print $sock "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nAccept: text/event-stream\r\n\r\n";
        my $wire = read_until($sock, sub { $_[0] =~ /data: hello/ });
        close $sock;

        is(scalar(@{header_lines_matching($wire, 'connection')}), 1,
            'exactly one Connection header (server-owned, app duplicate stripped)');
        like(header_lines_matching($wire, 'connection')->[0] // '', qr/keep-alive/i,
            "the surviving Connection header is the server's own 'keep-alive'");
        is(scalar(@{header_lines_matching($wire, 'transfer-encoding')}), 1,
            'exactly one Transfer-Encoding header (server-owned, app duplicate stripped)');
        like(header_lines_matching($wire, 'transfer-encoding')->[0] // '', qr/chunked/i,
            "the surviving Transfer-Encoding header is the server's own 'chunked'");
        like($wire, qr/x-marker:\s*ok/i, 'ordinary app header preserved');

        is(scalar(@{strip_warnings_matching(\@warnings, 'transfer-encoding')}), 1,
            "warns once for stripped 'transfer-encoding'");
        is(scalar(@{strip_warnings_matching(\@warnings, 'connection')}), 1,
            "warns once for stripped 'connection'");
    }

    $server->shutdown->get;
};

# ---------------------------------------------------------------------------
# (d) h1 SSE decline (sse.http.response.start / .body)
# ---------------------------------------------------------------------------
subtest 'h1 SSE decline: app TE/Connection stripped, server-owned Connection: close survives' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({
            type    => 'sse.http.response.start',
            status  => 404,
            headers => [
                [ 'connection',        'keep-alive' ],
                [ 'transfer-encoding', 'chunked' ],
                [ 'content-type',      'text/plain' ],
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
        skip "Cannot connect", 5 unless $sock;

        my @warnings;
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };

        print $sock "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nAccept: text/event-stream\r\n\r\n";
        my $wire = read_until($sock, sub { $_[0] =~ /No such stream/ });
        close $sock;

        is(scalar(@{header_lines_matching($wire, 'connection')}), 1,
            'exactly one Connection header (app duplicate stripped, server-owned close survives)');
        like(header_lines_matching($wire, 'connection')->[0] // '', qr/close/i,
            "the surviving Connection header is the server's own 'close' (a decline always disconnects)");
        is(header_lines_matching($wire, 'transfer-encoding'), [],
            'app-supplied Transfer-Encoding does not reach the wire alongside Content-Length');
        like($wire, qr/No such stream/, 'decline body arrives intact');

        is(scalar(@{strip_warnings_matching(\@warnings, 'transfer-encoding')}), 1,
            "warns once for stripped 'transfer-encoding'");
        is(scalar(@{strip_warnings_matching(\@warnings, 'connection')}), 1,
            "warns once for stripped 'connection'");
    }

    $server->shutdown->get;
};

# ---------------------------------------------------------------------------
# (e) h1 WebSocket denial (websocket.http.response.start / .body)
# ---------------------------------------------------------------------------
subtest 'h1 WebSocket denial: app TE/Connection stripped' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();   # websocket.connect
        await $send->({
            type    => 'websocket.http.response.start',
            status  => 401,
            headers => [
                [ 'connection',        'keep-alive' ],
                [ 'transfer-encoding', 'chunked' ],
                [ 'x-deny',            'auth' ],
            ],
        });
        await $send->({ type => 'websocket.http.response.body', body => 'nope' });
        return;
    };

    my $server = create_server($app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 5 unless $sock;

        my @warnings;
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };

        my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
        print $sock "GET / HTTP/1.1\r\n";
        print $sock "Host: 127.0.0.1:$port\r\n";
        print $sock "Upgrade: websocket\r\n";
        print $sock "Connection: Upgrade\r\n";
        print $sock "Sec-WebSocket-Key: $key\r\n";
        print $sock "Sec-WebSocket-Version: 13\r\n";
        print $sock "\r\n";

        my $wire = read_until($sock, sub { $_[0] =~ /nope/ });
        close $sock;

        is(header_lines_matching($wire, 'connection'), [],
            'app-supplied Connection is stripped (denial adds none of its own)');
        is(header_lines_matching($wire, 'transfer-encoding'), [],
            'app-supplied Transfer-Encoding does not reach the wire alongside Content-Length');
        like($wire, qr/x-deny:\s*auth/i, 'ordinary app header preserved');
        like($wire, qr/nope\z/, 'denial body arrives intact');

        is(scalar(@{strip_warnings_matching(\@warnings, 'transfer-encoding')}), 1,
            "warns once for stripped 'transfer-encoding'");
        is(scalar(@{strip_warnings_matching(\@warnings, 'connection')}), 1,
            "warns once for stripped 'connection'");
    }

    $server->shutdown->get;
};

done_testing;
