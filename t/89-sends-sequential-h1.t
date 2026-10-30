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
        $seen{transport} = $scope->{'pagi.transport'};
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
    my ($sock, $status, $rest) = ws_open($server);
    pump_until(sub { $seen{pending} }, 5);
    # A masked client Close (1000).
    syswrite($sock, Protocol::WebSocket::Frame->new(type => 'close', buffer => pack('n', 1000), masked => 1)->to_bytes);
    ok(pump_until(sub { @after == 3 && !grep { !$_->is_ready } @after }, 20), 'the later sends settled');
    ok($seen{pending} > 0, "$seen{pending} frames were queued when the peer closed");
    is($seen{event}, 'websocket.disconnect', 'the application saw the disconnect');
    ok(!grep({ $_->is_failed } @after), 'none of the later sends failed');
    is([grep { ($_->{level} // '') =~ /error|fatal/ } @log], [], 'nothing logged at error');
    # Everything the server sent, to the end of the connection.
    my ($more) = read_until($sock, sub { 0 }, 10);
    my $parser = Protocol::WebSocket::Frame->new(max_payload_size => 1 << 20);
    $parser->append($rest . $more);
    my @opcodes;
    while (defined $parser->next_bytes) { push @opcodes, $parser->opcode }
    is(scalar(grep { $_ == 8 } @opcodes), 1, 'one Close reached the wire');
    is($opcodes[-1], 8, 'and nothing followed it') or diag "opcodes: @opcodes";
    is($seen{transport}->buffered_amount, 0, 'nothing is still counted as waiting');
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
    # A length check, not e{200000}: perl before 5.38 caps a quantifier (32766 on 5.24, 65534 on 5.36).
    is(scalar(grep { /\Adata: \d+:(e+)\z/ && length $1 == 200_000 } @data), 20,
        'each whole: no keepalive inside an event');
    ok(scalar(grep { /^: ?ka/ } map { split /\n/ } @events) >= 1, 'keepalives were written');
    close $sock;
    stop_server($server);
};

subtest 'trailers issued while a large file body streams come after the whole body' => sub {
    my %seen;
    my $file = do {
        require File::Temp;
        my ($fh, $path) = File::Temp::tempfile(UNLINK => 1);
        print {$fh} join '', map { sprintf "line %05d %s\n", $_, 'x' x 30 } 1 .. 5000;
        close $fh;
        $path;
    };
    my $size = -s $file;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless http_scope($scope);
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type', 'text/plain']], trailers => 1 });
        my $body = $send->({ type => 'http.response.body', file => $file });
        $seen{body_pending} = !$body->is_ready;
        my $trailers = $send->({ type => 'http.response.trailers', headers => [['x-check', 'done']] });
        await Future->wait_all($body, $trailers);
        $seen{body_ok} = $body->is_done;
        $seen{trailers_ok} = $trailers->is_done;
    };
    my $server = start_server($app);
    my $sock = raw_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\nTE: trailers\r\n\r\n");
    my ($wire) = read_until($sock, sub { $_[0] =~ /x-check: done\r\n\r\n/ });
    ok($seen{body_pending}, "the file body ($size bytes) was still streaming when the trailers were issued");
    (my $chunks = $wire) =~ s/\A.*?\r\n\r\n//s;
    my ($body, $trailer_section) = ('', '');
    while ($chunks =~ /\G([0-9a-fA-F]+)\r\n/gc) {
        my $len = hex $1;
        if (!$len) { $trailer_section = substr($chunks, pos($chunks)); last }
        $body .= substr($chunks, pos($chunks), $len);
        pos($chunks) += $len + 2;
    }
    is(length $body, $size, 'the whole file arrived');
    like($trailer_section, qr/\Ax-check: done\r\n/, 'then the trailers');
    ok($seen{body_ok} && $seen{trailers_ok}, 'and both sends completed');
    close $sock;
    stop_server($server);
};

subtest 'an SSE stream ends -- for pagi.connection too -- only after the sends before sse.close' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'sse';
        my $conn = $scope->{'pagi.connection'};
        await $receive->();
        await $send->({ type => 'sse.start' });
        my $big = $send->({ type => 'sse.send', data => 'B' x 8_000_000 });
        my $a = $send->({ type => 'sse.send', event => 'marker', data => 'MARKER' });
        my $close = $send->({ type => 'sse.close' });
        $seen{a_pending} = !$a->is_ready;
        $seen{complete_at_close} = $conn->response_complete ? 1 : 0;
        $seen{connected_at_close} = $conn->is_connected ? 1 : 0;
        await Future->needs_all($big, $a, $close);
        $seen{complete_after} = $conn->response_complete ? 1 : 0;
    };
    my $server = start_server($app);
    my $sock = raw_connect($server);
    syswrite($sock, "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n");
    read_until($sock, sub { $_[0] =~ /\r\n0\r\n\r\n/ });
    pump_until(sub { exists $seen{complete_after} }, 5);
    ok($seen{a_pending}, 'the marker was still pending when sse.close was issued');
    is($seen{complete_at_close}, 0, 'the stream was not yet complete when sse.close was called');
    is($seen{connected_at_close}, 1, 'nor was the connection reported closed');
    is($seen{complete_after}, 1, 'it was complete once the sends before it were written');
    close $sock;
    stop_server($server);
};

subtest 'keep-alive after a response whose last bytes were still queued' => sub {
    my ($n, $queued_at_return) = (0, 0);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless http_scope($scope);
        $n++;
        if ($n == 1) {
            # Every send awaited (PAGI::Spec's Application Completion
            # Contract); the last bytes are still in the write buffer when
            # the application returns.
            await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
            await $send->({ type => 'http.response.body', body => '1' x 2_000_000, more => 1 });
            await $send->({ type => 'http.response.body', body => "\nFIRST-END\n", more => 0 });
            $queued_at_return = $scope->{'pagi.transport'}->buffered_amount;
            return;
        }
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'SECOND', more => 0 });
    };
    my $server = start_server($app);
    my $sock = raw_connect($server);
    syswrite($sock, "GET /one HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my ($first) = read_until($sock, sub { $_[0] =~ /FIRST-END\n\r\n0\r\n\r\n/ });
    ok($queued_at_return > 0, "$queued_at_return bytes were still queued when the application returned");
    like($first, qr/FIRST-END\n\r\n0\r\n\r\n\z/, 'the first response ended cleanly');
    syswrite($sock, "GET /two HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my ($second) = read_until($sock, sub { $_[0] =~ /SECOND/ });
    like($second, qr{\AHTTP/1\.1 200}, 'the second response starts the next bytes');
    like($second, qr/SECOND/, 'and is complete');
    close $sock;
    stop_server($server);
};

# The two other body sources: an fh, and a file small enough to be read in one
# go (under sync_file_threshold). Each ends with the trailers, not with a
# chunked terminator of its own.
for my $source (qw(fh small-file)) {
    my $name = $source eq 'fh' ? 'an fh body' : 'a small file body';
    subtest "$name with trailers: the body, one terminator, then the trailers" => sub {
        my %seen;
        my $content = join '', map { sprintf "line %05d %s\n", $_, 'x' x 30 } 1 .. 500;
        my $path = do {
            require File::Temp;
            my ($fh, $path) = File::Temp::tempfile(UNLINK => 1);
            print {$fh} $content;
            close $fh;
            $path;
        };
        my $n = 0;
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            die "unsupported\n" unless http_scope($scope);
            if (++$n == 2) {
                await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
                await $send->({ type => 'http.response.body', body => 'SECOND', more => 0 });
                return;
            }
            await $send->({ type => 'http.response.start', status => 200,
                            headers => [['content-type', 'text/plain']], trailers => 1 });
            my $body = $source eq 'fh'
                ? do { open my $fh, '<', $path or die $!; $send->({ type => 'http.response.body', fh => $fh }) }
                : $send->({ type => 'http.response.body', file => $path });
            my $trailers = $send->({ type => 'http.response.trailers', headers => [['x-check', 'done']] });
            await Future->wait_all($body, $trailers);
            $seen{both_done} = $body->is_done && $trailers->is_done;
        };
        my $server = start_server($app);
        my $sock = raw_connect($server);
        syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\nTE: trailers\r\n\r\n");
        my ($wire) = read_until($sock, sub { $_[0] =~ /x-check: done\r\n\r\n/ });
        (my $chunks = $wire) =~ s/\A.*?\r\n\r\n//s;
        my ($body, $trailer_section, $terminators) = ('', '', 0);
        while ($chunks =~ /\G([0-9a-fA-F]+)\r\n/gc) {
            my $len = hex $1;
            if (!$len) { $terminators++; $trailer_section = substr($chunks, pos($chunks)); last }
            $body .= substr($chunks, pos($chunks), $len);
            pos($chunks) += $len + 2;
        }
        is($body, $content, 'the whole body arrived');
        is($terminators, 1, 'one chunked terminator');
        is($trailer_section, "x-check: done\r\n\r\n", 'then the trailers end the response');
        ok($seen{both_done}, 'both sends completed');
        syswrite($sock, "GET /second HTTP/1.1\r\nHost: localhost\r\n\r\n");
        my ($more) = read_until($sock, sub { $_[0] =~ /SECOND/ }, 5);
        like($more, qr{\AHTTP/1\.1 200.*SECOND}s, 'and the connection serves the next request');
        close $sock;
        stop_server($server);
    };
}

# PAGI::Spec::Www "Transport Flow Control": on_high_water fires when
# buffered_amount crosses the high mark, and on_drain when it falls below the
# low mark. buffered_amount counts the sends still waiting their turn, so a
# burst of them is one crossing each way, however often the written buffer
# itself empties between turns.
# Large sends refill the buffer as soon as a waiting one resumes; small ones
# leave it below the low mark while many more still wait their turn.
for my $case (['http', 20, 100_000], ['sse', 20, 100_000], ['http', 300, 1_000], ['sse', 300, 1_000]) {
    my ($kind, $count, $size) = @$case;
    subtest "$kind: a queued burst of $count x $size bytes crosses the watermarks once each way" => sub {
        my ($HIGH, $LOW) = (65_536, 16_384);
        my (@high, @drain, %seen);
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            my $t = $scope->{'pagi.transport'};
            $t->on_high_water(sub { push @high, $t->buffered_amount });
            $t->on_drain(sub { push @drain, $t->buffered_amount });
            my @sent;
            if ($scope->{type} eq 'http') {
                await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
                @sent = map { $send->({ type => 'http.response.body', body => 'x' x $size, more => 1 }) } 1 .. $count;
                $seen{pending} = grep { !$_->is_ready } @sent;
                push @sent, $send->({ type => 'http.response.body', body => "\nEND\n", more => 0 });
            }
            else {
                await $receive->();
                await $send->({ type => 'sse.start' });
                @sent = map { $send->({ type => 'sse.send', data => 'x' x $size }) } 1 .. $count;
                $seen{pending} = grep { !$_->is_ready } @sent;
                push @sent, $send->({ type => 'sse.close' });
            }
            await Future->needs_all(@sent);
        };
        my $server = start_server($app, write_high_watermark => $HIGH, write_low_watermark => $LOW);
        my $sock = raw_connect($server);
        syswrite($sock, $kind eq 'http'
            ? "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
            : "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n");
        # A slow reader: a little at a time, so the written buffer empties
        # again and again while sends still wait their turn.
        my $wire = '';
        my $deadline = time + 30;
        until ($wire =~ /\r\n0\r\n\r\n/ || time > $deadline) {
            my $n = sysread($sock, my $chunk, 40_000);
            $wire .= $chunk if $n;
            $loop->loop_once(0.002);
        }
        like($wire, qr/\r\n0\r\n\r\n/, 'the response completed');
        ok($seen{pending} >= $count * 0.75, "$seen{pending} sends were queued unawaited");
        is(scalar @high, 1, 'on_high_water fired once') or diag "at: @high";
        is(scalar @drain, 1, 'on_drain fired once') or diag "at: @drain";
        ok(!grep({ $_ >= $LOW } @drain), "on_drain fired below the low mark (@drain)");
        close $sock;
        stop_server($server);
    };
}

done_testing;
