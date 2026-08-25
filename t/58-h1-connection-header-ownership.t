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
use Protocol::WebSocket::Frame;

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

# The bytes after the final chunk's "0\r\n" -- i.e. the trailer header lines
# (if any) plus the terminating blank line. Empty-trailers wire ("0\r\n\r\n")
# yields just "\r\n".
sub trailer_section_of {
    my ($wire) = @_;
    my ($section) = $wire =~ /\r\n0\r\n(.*)\z/s;
    return $section // '';
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

# ---------------------------------------------------------------------------
# (f) h1 WebSocket accept (websocket.accept extra headers)
# ---------------------------------------------------------------------------
# The 101 response starts with the server's own literal "Connection: Upgrade"
# line (RFC 6455 requires it -- untouched here), then appends the app's
# websocket.accept headers with only byte validation. Before this fix an
# app-supplied ['Connection', 'close'] reached the 101 verbatim, alongside
# the server's own Connection: Upgrade -- two Connection lines on one
# response. Strip the app's extra headers the same way every other h1
# response-header path already does.
subtest 'h1 websocket.accept: app TE/Connection stripped, server-owned Connection: Upgrade survives' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();   # websocket.connect
        await $send->({
            type    => 'websocket.accept',
            headers => [
                [ 'connection', 'close' ],
                [ 'x-legit',    '1' ],
            ],
        });
        while (1) {
            my $event = await $receive->();
            last if $event->{type} eq 'websocket.disconnect';
            if ($event->{type} eq 'websocket.receive' && defined $event->{text}) {
                await $send->({ type => 'websocket.send', text => "echo: $event->{text}" });
            }
        }
        return;
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

        my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
        print $sock "GET / HTTP/1.1\r\n";
        print $sock "Host: 127.0.0.1:$port\r\n";
        print $sock "Upgrade: websocket\r\n";
        print $sock "Connection: Upgrade\r\n";
        print $sock "Sec-WebSocket-Key: $key\r\n";
        print $sock "Sec-WebSocket-Version: 13\r\n";
        print $sock "\r\n";

        my $wire = read_until($sock, sub { $_[0] =~ /\r\n\r\n/ });

        is(scalar(@{header_lines_matching($wire, 'connection')}), 1,
            'exactly one Connection header on the 101 (app duplicate stripped)');
        like(header_lines_matching($wire, 'connection')->[0] // '', qr/Upgrade/i,
            "the surviving Connection header is the server's own 'Upgrade' (RFC 6455)");
        like($wire, qr/x-legit:\s*1/i, 'ordinary app header preserved');
        like($wire, qr/\A HTTP\/1\.1 \s+ 101/x, 'handshake completed with 101 Switching Protocols');

        # Handshake completed -- prove the connection is still a working
        # WebSocket, not just a header-shaped response: send a masked client
        # frame and read the server's echo (idiom: t/04-websocket.t).
        my $ws_frame = Protocol::WebSocket::Frame->new(
            buffer => 'still works',
            type   => 'text',
            masked => 1,
        );
        print $sock $ws_frame->to_bytes;

        my $response_frame = Protocol::WebSocket::Frame->new;
        my $echoed_text;
        my $deadline = time + 5;
        while (time < $deadline) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            if (defined $n && $n > 0) {
                $response_frame->append($buf);
                $echoed_text = $response_frame->next_bytes;
                last if defined $echoed_text;
            }
            elsif (defined $n && $n == 0) {
                last;
            }
            $loop->loop_once(0.05);
        }
        close $sock;

        is($echoed_text, 'echo: still works',
            'the WebSocket connection is still usable after the fix (client frame echoed)');

        is(scalar(@{strip_warnings_matching(\@warnings, 'connection')}), 1,
            "warns once for stripped 'connection'");
    }

    $server->shutdown->get;
};

# ---------------------------------------------------------------------------
# (g) h1 http.response.trailers
# ---------------------------------------------------------------------------
# RFC 9110 section 6.5.1 additionally forbids connection-specific/framing
# fields in trailers outright, on any HTTP version -- not just the h1
# response-header rule this file otherwise covers. An app-supplied
# Connection or Transfer-Encoding trailer tuple must never reach the wire,
# same as it never reaches the wire in a response header block.
subtest 'h1 http.response.trailers: app TE/Connection stripped from the trailer section' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type     => 'http.response.start',
            status   => 200,
            trailers => 1,
            headers  => [ [ 'content-type', 'text/plain' ] ],
        });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
        await $send->({
            type    => 'http.response.trailers',
            headers => [
                [ 'x-t',              '1' ],
                [ 'connection',        'close' ],
                [ 'transfer-encoding', 'chunked' ],
            ],
        });
    };

    my $server = create_server($app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 3 unless $sock;

        my @warnings;
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };

        print $sock "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nConnection: close\r\n\r\n";
        my $wire = read_until($sock, sub {
            $_[0] =~ /\r\nok\r\n0\r\n/ && $_[0] =~ /\r\n\r\n\z/;
        });
        close $sock;

        is(trailer_section_of($wire), "x-t: 1\r\n\r\n",
            'trailer section contains only x-t, terminated correctly (app Connection/Transfer-Encoding stripped)');

        is(scalar(@{strip_warnings_matching(\@warnings, 'transfer-encoding')}), 1,
            "warns once for stripped 'transfer-encoding'");
        is(scalar(@{strip_warnings_matching(\@warnings, 'connection')}), 1,
            "warns once for stripped 'connection'");
    }

    $server->shutdown->get;
};

# ---------------------------------------------------------------------------
# Upgrade companion rule (PAGI spec, main @ e6ba2bb): over HTTP/1.1, when an
# application response carries an Upgrade header (e.g. 426 Upgrade Required),
# the server must include 'upgrade' among the tokens of the Connection header
# it supplies -- RFC 9110 requires the pair from any Upgrade sender. The app
# never sends Connection: upgrade itself (it would be stripped like any other
# app-supplied Connection header).
# ---------------------------------------------------------------------------
subtest 'h1 426 with Upgrade: server supplies the Connection: upgrade companion' => sub {
    my $body = 'Upgrade Required';
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type    => 'http.response.start',
            status  => 426,
            headers => [
                [ 'upgrade',        'websocket' ],
                [ 'connection',     'upgrade' ],   # app-supplied: still stripped
                [ 'content-type',   'text/plain' ],
                [ 'content-length', length($body) ],
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

        my $conn_lines = header_lines_matching($wire, 'connection');
        is(scalar(@$conn_lines), 1,
            'exactly one Connection header on the wire');
        like($conn_lines->[0] // '', qr/^connection:\s*.*\bupgrade\b/i,
            "server-supplied Connection carries the 'upgrade' token");
        is(scalar(@{header_lines_matching($wire, 'upgrade')}), 1,
            'app-supplied Upgrade header preserved (h1 does not strip it)');
        like($wire, qr/\A\QHTTP\E\/1\.1 426/, 'status 426 on the wire');

        is(scalar(@{strip_warnings_matching(\@warnings, 'connection')}), 1,
            "app-supplied 'connection' still stripped with one warning");
        is(scalar(@{strip_warnings_matching(\@warnings, 'upgrade')}), 0,
            "no strip warning for 'upgrade' (legal h1 app header)");
    }

    $server->shutdown->get;
};

subtest 'h1 response without Upgrade: server still supplies no Connection header' => sub {
    my $body = 'plain';
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [
                [ 'content-type',   'text/plain' ],
                [ 'content-length', length($body) ],
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
        skip "Cannot connect", 1 unless $sock;

        print $sock "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nConnection: close\r\n\r\n";
        my $wire = read_until($sock, sub { $_[0] =~ /\Q$body\E\z/ });
        close $sock;

        is(header_lines_matching($wire, 'connection'), [],
            'no Connection header when the response carries no Upgrade (keep-alive stays implicit)');
    }

    $server->shutdown->get;
};

done_testing;
