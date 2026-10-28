use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;
use IO::Socket::INET;
use Protocol::WebSocket::Frame;
use Time::HiRes qw(time);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# PAGI::Spec::Www 0.7, "Sends Are Sequential", over HTTP/1.1: an application
# may issue a send before an earlier one has resolved, and the server
# processes a scope's sends in the order they were called -- each whole, none
# overtaking or discarding an earlier one, the terminal event after them all.
# Every case checks, server side, that the earlier send was still pending when
# the later one was issued; without that it could not fail.

my $loop = IO::Async::Loop->new;
my @log;

sub start_server {
    my ($app, %opts) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
        shutdown_timeout => 1, logger => sub { push @log, $_[0] }, %opts,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub stop_server {
    my ($server) = @_;
    $server->shutdown->get;
    $loop->remove($server);
}

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 10);
    $loop->loop_once(0.005) until $cond->() || time > $deadline;
    return $cond->();
}

sub raw_connect {
    my ($server) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2,
    ) or die "connect: $!";
    $sock->blocking(0);
    return $sock;
}

# Reads what is there without blocking; returns the bytes and whether the
# server closed the connection.
sub read_now {
    my ($sock) = @_;
    my ($buf, $eof) = ('', 0);
    while (1) {
        my $n = sysread($sock, my $chunk, 65536);
        if (defined $n && $n > 0) { $buf .= $chunk; next }
        $eof = 1 if defined $n && $n == 0;
        last;
    }
    return ($buf, $eof);
}

# Reads, pumping the loop, until $done->($all) or the server closes.
sub read_until {
    my ($sock, $done, $secs) = @_;
    my ($all, $eof) = ('', 0);
    my $deadline = time + ($secs // 20);
    until ($eof || $done->($all) || time > $deadline) {
        my ($buf, $closed) = read_now($sock);
        $all .= $buf;
        $eof = $closed;
        $loop->loop_once(0.005);
    }
    return ($all, $eof);
}

sub http_scope { my ($scope) = @_; $scope->{type} eq 'http' }

subtest 'a send issued while an earlier one waits on backpressure goes out after it' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless http_scope($scope);
        my $transport = $scope->{'pagi.transport'};
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my $big = $send->({ type => 'http.response.body', body => '1' x 8_000_000, more => 1 });
        my $a = $send->({ type => 'http.response.body', body => "\nMARK-A\n", more => 1 });
        # Wait until the buffer is below the high watermark but above the low
        # one while A has not resolved: on a server that waits and then
        # writes, A is parked there and B is not.
        until ($transport->buffered_amount < $transport->high_water_mark) {
            await $loop->delay_future(after => 0.002);
        }
        $seen{b_at} = $transport->buffered_amount;
        $seen{a_pending} = !$a->is_ready;
        my $b = $send->({ type => 'http.response.body', body => "\nMARK-B\n", more => 1 });
        await Future->needs_all($big, $a, $b);
        await $send->({ type => 'http.response.body', body => "\nEND\n", more => 0 });
    };
    my $server = start_server($app, write_high_watermark => 4_000_000, write_low_watermark => 16_384);
    my $sock = raw_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my ($wire) = read_until($sock, sub { $_[0] =~ /\nEND\n/ });
    ok($seen{a_pending}, "A was still pending when B was issued (buffer $seen{b_at})");
    ok($seen{b_at} >= 16_384, 'B was issued above the low watermark');
    my ($pos_a, $pos_b) = (index($wire, 'MARK-A'), index($wire, 'MARK-B'));
    ok($pos_a > 0 && $pos_b > 0, 'both markers arrived');
    ok($pos_a < $pos_b, 'A before B, the order they were called');
    close $sock;
    stop_server($server);
};

subtest 'sse.close behind a waiting send ends the stream after it, and the socket stays usable' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'http') {
            await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
            await $send->({ type => 'http.response.body', body => 'SECOND', more => 0 });
            return;
        }
        die "unsupported\n" unless $scope->{type} eq 'sse';
        await $receive->();
        await $send->({ type => 'sse.start' });
        my $big = $send->({ type => 'sse.send', data => 'B' x 8_000_000 });
        my $a = $send->({ type => 'sse.send', event => 'marker', data => 'AFTER-PARK-MARKER' });
        $seen{a_pending} = !$a->is_ready;
        my $c = $send->({ type => 'sse.close' });
        await Future->needs_all($big, $a, $c);
    };
    my $server = start_server($app);
    my $sock = raw_connect($server);
    syswrite($sock, "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n");
    my ($wire) = read_until($sock, sub { $_[0] =~ /\r\n0\r\n\r\n/ });
    ok($seen{a_pending}, 'the marker was still pending when sse.close was issued');
    my $end = index($wire, "\r\n0\r\n\r\n");
    ok($end > 0, 'the stream ended with the chunked terminator');
    my $marker = index($wire, 'AFTER-PARK-MARKER');
    ok($marker > 0 && $marker < $end, 'the marker came before the terminator');

    # Nothing may follow the terminator but the next response.
    syswrite($sock, "GET /second HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my ($more) = read_until($sock, sub { $_[0] =~ /SECOND/ }, 5);
    my $after = substr($wire, $end + 7) . $more;
    like($after, qr{\AHTTP/1\.1 200}, 'the next bytes on the socket are the next response');
    close $sock;
    stop_server($server);
};

# A WebSocket client on a raw socket: the handshake, then frames.
sub ws_open {
    my ($server) = @_;
    my $sock = raw_connect($server);
    syswrite($sock, join "\r\n", 'GET /ws HTTP/1.1', 'Host: localhost', 'Upgrade: websocket',
        'Connection: Upgrade', 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==',
        'Sec-WebSocket-Version: 13', '', '');
    my ($head) = read_until($sock, sub { $_[0] =~ /\r\n\r\n/ }, 5);
    my ($status, $rest) = $head =~ /\A(.*?\r\n).*?\r\n\r\n(.*)\z/s;
    return ($sock, $status, $rest // '');
}

# Reads frames until a Close arrives or the server closes; returns them as
# [opcode, payload] pairs.
sub ws_frames {
    my ($sock, $pending) = @_;
    my $parser = Protocol::WebSocket::Frame->new(max_payload_size => 1 << 20);
    $parser->append($pending) if length $pending;
    my @frames;
    my $deadline = time + 20;
    while (time < $deadline) {
        while (defined(my $payload = $parser->next_bytes)) {
            push @frames, [$parser->opcode, $payload];
            return @frames if $parser->opcode == 8;
        }
        my ($buf, $eof) = read_now($sock);
        $parser->append($buf) if length $buf;
        last if $eof && !length $buf;
        $loop->loop_once(0.005);
    }
    return @frames;
}

subtest 'websocket.close behind 150 waiting frames: every frame, then the Close' => sub {
    my (%seen, $late);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'websocket';
        await $receive->();
        await $send->({ type => 'websocket.accept' });
        my @bigs = map { $send->({ type => 'websocket.send', bytes => 'W' x 60_000 }) } 1 .. 150;
        $seen{pending} = grep { !$_->is_ready } @bigs;
        my $a = $send->({ type => 'websocket.send', text => 'AFTER-PARK-MARKER' });
        my $c = $send->({ type => 'websocket.close', code => 1000, reason => 'bye' });
        $late = $send->({ type => 'websocket.send', text => 'TOO-LATE' });
        await Future->needs_all(@bigs, $a, $c);
        while (1) { last if (await $receive->())->{type} eq 'websocket.disconnect' }
    };
    my $server = start_server($app);
    my ($sock, $status, $rest) = ws_open($server);
    like($status, qr{\AHTTP/1\.1 101}, 'upgraded');
    my @frames = ws_frames($sock, $rest);
    ok($seen{pending} > 0, "$seen{pending} frames were still pending when the Close was issued");
    is(scalar(grep { $_->[0] == 2 } @frames), 150, 'all 150 binary frames arrived');
    is([map { $_->[1] } grep { $_->[0] == 1 } @frames], ['AFTER-PARK-MARKER'], 'then the marker');
    is($frames[-1][0], 8, 'and the Close last');
    ok($late && $late->is_failed, "a send issued after the application's own close fails");
    close $sock;
    stop_server($server);
};

subtest 'buffered_amount counts every send accepted and not yet written' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless http_scope($scope);
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my @sent = map { $send->({ type => 'http.response.body', body => 'x' x 10_000, more => 1 }) } 1 .. 50;
        $seen{buffered} = $scope->{'pagi.transport'}->buffered_amount;
        push @sent, $send->({ type => 'http.response.body', body => '', more => 0 });
        await Future->needs_all(@sent);
    };
    my $server = start_server($app);
    my $sock = raw_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    read_until($sock, sub { $_[0] =~ /\r\n0\r\n\r\n/ });
    ok($seen{buffered} >= 50 * 10_000, "right after 50 unawaited 10 KB sends, buffered_amount is $seen{buffered}");
    close $sock;
    stop_server($server);
};

subtest 'a cancelled send does not hold up the sends behind it' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless http_scope($scope);
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my $big = $send->({ type => 'http.response.body', body => '1' x 8_000_000, more => 1 });
        my $a = $send->({ type => 'http.response.body', body => "\nMARK-A\n", more => 1 });
        $seen{a_pending} = !$a->is_ready;
        $a->cancel;
        my $b = $send->({ type => 'http.response.body', body => "\nMARK-B\n", more => 1 });
        await Future->needs_all($big, $b);
        $seen{b_done} = $b->is_done;
        await $send->({ type => 'http.response.body', body => "\nEND\n", more => 0 });
    };
    my $server = start_server($app);
    my $sock = raw_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my ($wire) = read_until($sock, sub { $_[0] =~ /\nEND\n/ });
    ok($seen{a_pending}, 'A was pending when the application cancelled it');
    ok($seen{b_done}, 'B completed');
    like($wire, qr/MARK-B/, 'and arrived');
    close $sock;
    stop_server($server);
};

subtest 'a peer close while frames are queued: later sends are quiet no-ops' => sub {
    my (%seen, @after);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'websocket';
        await $receive->();
        await $send->({ type => 'websocket.accept' });
        my @bigs = map { $send->({ type => 'websocket.send', bytes => 'W' x 60_000 }) } 1 .. 100;
        $seen{pending} = grep { !$_->is_ready } @bigs;
        my $e = await $receive->();
        $seen{event} = $e->{type};
        push @after, $send->({ type => 'websocket.send', text => 'AFTER-PEER-CLOSE' }) for 1 .. 3;
        await Future->wait_all(@bigs, @after);
    };
    @log = ();
    my $server = start_server($app);
    my ($sock, $status) = ws_open($server);
    pump_until(sub { $seen{pending} }, 5);
    # A masked client Close (1000).
    syswrite($sock, Protocol::WebSocket::Frame->new(type => 'close', buffer => pack('n', 1000), masked => 1)->to_bytes);
    ok(pump_until(sub { @after == 3 && !grep { !$_->is_ready } @after }, 20), 'the later sends settled');
    ok($seen{pending} > 0, "$seen{pending} frames were queued when the peer closed");
    is($seen{event}, 'websocket.disconnect', 'the application saw the disconnect');
    ok(!grep({ $_->is_failed } @after), 'none of the later sends failed');
    is([grep { ($_->{level} // '') =~ /error|fatal/ } @log], [], 'nothing logged at error');
    close $sock;
    stop_server($server);
};

subtest 'a client gone with a large unwritten buffer: every pending send settles' => sub {
    my (@sent, $conn);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless http_scope($scope);
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        @sent = map { $send->({ type => 'http.response.body', body => 'x' x 100_000, more => 1 }) } 1 .. 80;
        await Future->wait_all(@sent);
    };
    my $server = start_server($app);
    my $sock = raw_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    pump_until(sub { ($conn) = values %{ $server->{connections} }; @sent == 80 }, 5);
    ok((grep { !$_->is_ready } @sent) > 0, 'sends were pending');
    close $sock;
    ok(pump_until(sub { !grep { !$_->is_ready } @sent }, 20), 'every pending send settled');
    ok(!grep({ $_->is_failed } @sent), 'none failed (they resolve at disconnect)');
    is($conn->_get_write_buffer_size, 0, 'and the count of queued bytes is 0');
    stop_server($server);
};

subtest 'SSE keepalives fall between queued events, never inside one' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'sse';
        await $receive->();
        await $send->({ type => 'sse.start' });
        await $send->({ type => 'sse.keepalive', interval => 0.01, comment => 'ka' });
        my @sent = map { $send->({ type => 'sse.send', data => "$_:" . ('e' x 200_000) }) } 1 .. 20;
        await $loop->delay_future(after => 0.3);
        push @sent, $send->({ type => 'sse.close' });
        await Future->needs_all(@sent);
    };
    my $server = start_server($app);
    my $sock = raw_connect($server);
    syswrite($sock, "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n");
    # Read slowly, so keepalives come due while events are queued.
    my ($wire, $eof) = ('', 0);
    my $deadline = time + 30;
    until ($wire =~ /\r\n0\r\n\r\n/ || $eof || time > $deadline) {
        my $n = sysread($sock, my $chunk, 16_384);
        if (defined $n && $n > 0) { $wire .= $chunk } elsif (defined $n) { $eof = 1 }
        $loop->loop_once(0.005);
    }
    (my $body = $wire) =~ s/\A.*?\r\n\r\n//s;
    my $text = '';
    while ($body =~ /\G([0-9a-fA-F]+)\r\n/gc) {
        my $len = hex $1;
        last unless $len;
        $text .= substr($body, pos($body), $len);
        pos($body) += $len + 2;
    }
    my @events = grep { length } split /\n\n/, $text;
    my @data = grep { /^data: / } map { split /\n/ } @events;
    is(scalar(@data), 20, 'all 20 events arrived');
    is(scalar(grep { /\Adata: \d+:e{200000}\z/ } @data), 20, 'each whole: no keepalive inside an event');
    ok(scalar(grep { /^: ?ka/ } map { split /\n/ } @events) >= 1, 'keepalives were written');
    close $sock;
    stop_server($server);
};

done_testing;
