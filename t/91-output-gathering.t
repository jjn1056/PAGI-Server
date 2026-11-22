use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;
use IO::Socket::INET;
use Time::HiRes qw(time);
use Protocol::WebSocket::Frame;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# A response's bytes reach the client whole and in order however the server
# gathers them on their way to the socket: many small sends, a close after
# the response (its Connection may be gone before the loop writes), a second
# response on a kept-alive connection, one very large send.

my $loop = IO::Async::Loop->new;

# Line $n of a body: 100 bytes, numbered, so order can be checked.
sub line { sprintf("%07d", $_[0]) . ('x' x 92) . "\n" }

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    if ($scope->{type} eq 'websocket') {
        # A large backlog, so the server's Close waits behind it; on
        # /ws-keepalive, a keepalive asked for after the peer's Close has
        # already arrived.
        await $receive->();    # websocket.connect
        await $send->({ type => 'websocket.accept' });
        for my $i (1 .. 70) {
            await $send->({ type => 'websocket.send', bytes => 'x' x 60_000 });
        }
        await $loop->delay_future(after => 0.5);
        await $send->({ type => 'websocket.keepalive', interval => 0.2 }) if $scope->{path} eq '/ws-keepalive';
        while (1) {
            my $event = await $receive->();
            last if $event->{type} eq 'websocket.disconnect';
        }
        return;
    }
    die "unsupported scope\n" unless $scope->{type} eq 'http';
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    if ($scope->{path} eq '/big') {
        await $send->({ type => 'http.response.body', body => 'x' x (1 << 20), more => 0 });
        return;
    }
    my ($n) = $scope->{path} =~ m{\A/lines/(\d+)\z} or die "unknown path\n";
    for my $i (1 .. $n) {
        await $send->({ type => 'http.response.body', body => line($i), more => 1 });
    }
    await $send->({ type => 'http.response.body', body => '', more => 0 });
    return;
};

sub start_server {
    my (%opts) = @_;
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0,
        quiet => 1, access_log => undef, shutdown_timeout => 1, %opts);
    $loop->add($server);
    $server->listen->get;
    return $server;
}

# Reads while running the loop until $done says so, EOF, or $secs pass.
sub read_until {
    my ($sock, $done, $secs) = @_;
    my ($buf, $eof, $t0) = ('', 0, time);
    while (time - $t0 < $secs) {
        my $n = sysread($sock, my $chunk, 1 << 16);
        if (defined $n && $n == 0) { $eof = 1; last }
        $buf .= $chunk if $n;
        last if $done && $done->($buf);
        $loop->loop_once(0.01);
    }
    return ($buf, $eof);
}

# The body of one chunked response at the start of $buf, and what follows it.
sub dechunk {
    my ($buf) = @_;
    my $at = index($buf, "\r\n\r\n");
    return unless $at >= 0;
    my ($body, $pos) = ('', $at + 4);
    while ((my $eol = index($buf, "\r\n", $pos)) >= 0) {
        my $len = hex substr($buf, $pos, $eol - $pos);
        return ($body, substr($buf, $eol + 4)) if $len == 0;
        return if length($buf) < $eol + 2 + $len + 2;
        $body .= substr($buf, $eol + 2, $len);
        $pos = $eol + 2 + $len + 2;
    }
    return;
}

my $response_done = sub { my ($body) = dechunk($_[0]); defined $body };

sub connect_to {
    my ($server) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp')
        or die "connect: $!";
    $sock->blocking(0);
    return $sock;
}

subtest 'a close-after-response body sent in many small sends arrives whole' => sub {
    my $server = start_server();
    my $sock = connect_to($server);
    syswrite $sock, "GET /lines/2000 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
    my ($buf, $eof) = read_until($sock, undef, 10);
    ok($eof, 'the server closed the connection after the response');
    my ($body) = dechunk($buf);
    is(length($body // ''), 200_000, 'every byte arrived');
    ok(($body // '') eq join('', map { line($_) } 1 .. 2000), 'lines 1..2000, in order');
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'an HTTP/1.0 body sent in many small sends arrives whole before the close' => sub {
    my $server = start_server();
    my $sock = connect_to($server);
    syswrite $sock, "GET /lines/2000 HTTP/1.0\r\nHost: x\r\n\r\n";
    my ($buf, $eof) = read_until($sock, undef, 10);
    ok($eof, 'the server closed the connection after the response');
    my $at = index($buf, "\r\n\r\n");
    my $body = $at >= 0 ? substr($buf, $at + 4) : '';
    is(length $body, 200_000, 'every byte arrived');
    ok($body eq join('', map { line($_) } 1 .. 2000), 'lines 1..2000, in order');
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'a keep-alive connection serves a second response after a burst' => sub {
    my $server = start_server();
    my $sock = connect_to($server);
    my $expect = join '', map { line($_) } 1 .. 500;
    for my $round (1, 2) {
        # A few idle passes after the first response, so its burst has ended
        # (the socket is writable, so its generator is asked again and
        # leaves): the second response is a new burst, not the first one's.
        $loop->loop_once(0.01) for 1 .. 5;
        syswrite $sock, "GET /lines/500 HTTP/1.1\r\nHost: x\r\n\r\n";
        my ($buf, $eof) = read_until($sock, $response_done, 10);
        my ($body) = dechunk($buf);
        ok(!$eof, "response $round: the connection stays open");
        ok(($body // '') eq $expect, "response $round: 500 lines, whole and in order");
    }
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'a 1 MiB body in one send arrives intact over HTTP/1.1' => sub {
    my $server = start_server();
    my $sock = connect_to($server);
    syswrite $sock, "GET /big HTTP/1.1\r\nHost: x\r\n\r\n";
    my ($buf) = read_until($sock, $response_done, 10);
    my ($body) = dechunk($buf);
    is(length($body // ''), 1 << 20, 'every byte arrived');
    ok(($body // '') eq 'x' x (1 << 20), 'unchanged');
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

# A WebSocket whose server Close waits behind a backlog the peer has stopped
# reading, after the peer's Close. Returns the server, the socket, the
# connection and the bytes read so far; $while_closing runs once the server's
# Close is queued.
sub closing_websocket {
    my ($path, $while_closing) = @_;
    my $server = start_server(write_high_watermark => 1 << 30);
    my $sock = connect_to($server);
    syswrite $sock, "GET $path HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    my ($buf) = read_until($sock, sub { $_[0] =~ /\r\n\r\n/ }, 5);
    my $accepted = $buf =~ m{\AHTTP/1\.1 101};
    $buf =~ s/\A.*?\r\n\r\n//s;
    my ($conn) = values %{ $server->{connections} };
    syswrite $sock, Protocol::WebSocket::Frame->new(type => 'close', masked => 1, buffer => pack('n', 1000))->to_bytes;
    my $t0 = time;
    my $ran = 0;
    while (time - $t0 < 1.5) {
        $loop->loop_once(0.01);
        $ran = 1, $while_closing->($conn) if $while_closing && !$ran && $conn->{close_sent};
    }
    my ($rest, $eof) = read_until($sock, undef, 15);
    my $parser = Protocol::WebSocket::Frame->new(max_payload_size => 0);
    $parser->append($buf . $rest);
    my @frames;
    while (defined(my $payload = $parser->next_bytes)) {
        push @frames, $parser->is_close ? 'close' : $parser->is_ping ? 'ping' : 'data';
    }
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
    return ($accepted, $eof, $ran, @frames);
}

subtest q{a keepalive asked for after the peer's Close sends nothing and warns nothing} => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my ($accepted, $eof, undef, @frames) = closing_websocket('/ws-keepalive');
    ok($accepted, 'the handshake was accepted');
    ok($eof, 'the server closed the connection');
    is($frames[-1], 'close', "the server's Close is the last frame it sent")
        or diag "last frames: @frames[-5 .. -1]";
    is([grep { $_ eq 'ping' } @frames], [], 'no ping was sent');
    is(\@warnings, [], 'no warnings');
};

# Nothing public writes after the close begins any more, so this stands in
# for any late writer by writing on the connection directly.
subtest q{output written after the close began is refused, not sent after the Close} => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $late = Protocol::WebSocket::Frame->new(type => 'ping', buffer => 'late')->to_bytes;
    my ($accepted, $eof, $ran, @frames) = closing_websocket('/ws',
        sub { $_[0]->_write_server_output($late) });
    ok($accepted && $ran, 'the late write was made while the close waited');
    ok($eof, 'the server closed the connection');
    is($frames[-1], 'close', "the server's Close is the last frame it sent")
        or diag "last frames: @frames[-5 .. -1]";
    is([grep { $_ eq 'ping' } @frames], [], 'the late bytes were not sent');
    is([grep { !/Cannot write data to a Stream that is closing/ } @warnings], [],
        'the only warning is IO::Async refusing the write');
};

done_testing;
