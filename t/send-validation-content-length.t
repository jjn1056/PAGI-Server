use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Future::AsyncAwait;
use Socket qw(AF_UNIX SOCK_STREAM);
use Test::TempDir::Tiny qw(tempdir);
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::EventValidator;
use PAGI::Server::Protocol::HTTP1;

# A response declares its length in at most one content-length field line,
# whose value is one decimal number (RFC 9110 S8.6, S5.3). A list such as
# '5, 5' is one field line holding two values.

my $V = 'PAGI::Server::EventValidator';
my $start = sub { $V->can('validate_http_send')->({ type => 'http.response.start', status => 200, headers => $_[0] }) };

subtest 'one decimal content-length passes' => sub {
    ok(lives { $start->([['content-length', '5']]) }, 'content-length 5');
    ok(lives { $start->([['Content-Length', '5']]) }, 'Content-Length 5');
    ok(lives { $start->([['content-length', '0']]) }, 'content-length 0');
    ok(lives { $start->([['x-a', '1']]) },             'no content-length');
};

subtest 'anything else in a content-length fails' => sub {
    for my $v ('abc', '-1', '+5', '', '5, 5', '5, 3', '5.0', '0x5') {
        like(dies { $start->([['x-a', '1'], ['Content-Length', $v]]) },
            qr/\Ahttp\.response\.start header 1 \('Content-Length'\): content-length must be one decimal number\n\z/,
            "'$v' fails");
    }
};

subtest 'a second content-length line fails, equal or not' => sub {
    like(dies { $start->([['content-length', 5], ['content-length', 5]]) },
        qr/\Ahttp\.response\.start header 1 \('content-length'\): content-length appears more than once \(also header 0\)\n\z/,
        'two equal lines');
    like(dies { $start->([['content-length', 5], ['x-a', '1'], ['Content-Length', 3]]) },
        qr/\Ahttp\.response\.start header 2 \('Content-Length'\): content-length appears more than once \(also header 0\)\n\z/,
        'two different lines, either case');
};

subtest 'refusals check it too; sse.start does not' => sub {
    like(dies { $V->can('validate_websocket_send')->({ type => 'http.response.start', status => 403,
        headers => [['content-length', 'abc']] }, {}) },
        qr/content-length must be one decimal number/, 'websocket refusal');
    like(dies { $V->can('validate_sse_send')->({ type => 'http.response.start', status => 404,
        headers => [['content-length', '1'], ['content-length', '1']] }, {}) },
        qr/content-length appears more than once/, 'sse refusal');
    ok(lives { $V->can('validate_sse_send')->({ type => 'sse.start', headers => [['content-length', 'abc']] }, {}) },
        'sse.start is not checked');
};

subtest 'declared_content_length' => sub {
    is(PAGI::Server::EventValidator::declared_content_length([['Content-Length', '12']]), 12, 'reads the length');
    is(PAGI::Server::EventValidator::declared_content_length([['x-a', '1'], ['content-length', '007']]), 7,
        'leading zeros are a decimal number');
    is(PAGI::Server::EventValidator::declared_content_length([]), undef, 'undef without one');
    is(PAGI::Server::EventValidator::declared_content_length([['x-a', '1']]), undef, 'undef among other headers');
};

# --- Body bytes against the declared length (HTTP/1.x) -----------------------

subtest 'count_body_bytes' => sub {
    my $count = sub { PAGI::Server::EventValidator::count_body_bytes(@_) };
    is($count->({ declared => 5, remaining => 5 }, 5, 1), { declared => 5, remaining => 0 }, 'exact');
    is($count->({ declared => 10, remaining => 10 }, 5, 0), { declared => 10, remaining => 5 },
        'short but not terminal: the rest may follow');
    like(dies { $count->({ declared => 3, remaining => 3 }, 5, 1) },
        qr/\Ahttp\.response\.body would send 5 bytes, 2 more than the declared content-length \(3\)\n\z/, 'over');
    like(dies { $count->({ declared => 5, remaining => 2 }, 3, 0) },
        qr/\Ahttp\.response\.body would send 3 bytes, 1 more than the declared content-length \(5\)\n\z/,
        'over counts what is left, not the whole length');
    like(dies { $count->({ declared => 10, remaining => 10 }, 5, 1) },
        qr/\Ahttp\.response\.body ends the response 5 bytes short of the declared content-length \(10\)\n\z/,
        'short at the terminal event');
    my $record = { declared => 3, remaining => 3 };
    ok(dies { $count->($record, 5, 1) }, 'the count fails');
    is($record, { declared => 3, remaining => 3 }, 'a failed count leaves the record as it was');
};

my $loop = IO::Async::Loop->new;
my $dir  = tempdir();
my $file = "$dir/five.txt";
{ open my $out, '>:raw', $file or die "write $file: $!"; print $out 'hello'; close $out }
my $unreadable = "$dir/unreadable.txt";
{ open my $out, '>:raw', $unreadable or die "write $unreadable: $!"; print $out 'hello'; close $out }
chmod 0000, $unreadable;

# Each path sends one response shape and records what its body sends did.
my %result;
my $LARGE = 'x' x (1 << 20);
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    await $receive->();
    my $path = $scope->{path};
    my $try = async sub {
        my ($event) = @_;
        my $ok = eval { await $send->({ type => 'http.response.body', %$event }); 1 };
        return $ok ? 'NO-ERROR' : $@;
    };
    my ($length) = $path =~ m{/(\d+)\z};
    await $send->({ type => 'http.response.start', status => 200,
        headers => [['content-type', 'text/plain'], ['content-length', $length]] });

    if ($path =~ m{\A/body/}) {
        $result{$path} = await $try->({ body => 'hello', more => 0 });
    }
    elsif ($path =~ m{\A/empty/}) {
        $result{$path} = await $try->({ body => '', more => 0 });
    }
    elsif ($path =~ m{\A/recover/}) {
        $result{$path} = [ await $try->({ body => 'hello world', more => 0 }),
                           await $try->({ body => 'hello', more => 0 }) ];
    }
    elsif ($path =~ m{\A/overlap/}) {
        # The large body fills the write buffer past its high watermark, so
        # the next send waits for it to drain and is still pending when the
        # one after it is called.
        await $send->({ type => 'http.response.body', body => $LARGE, more => 1 });
        my $first = $send->({ type => 'http.response.body', body => 'abc', more => 1 });
        my $first_pending = !$first->is_ready;
        my $second = await $try->({ body => 'abc', more => 1 });
        await $first;
        $result{$path} = [ $first_pending, $second, await $try->({ body => 'de', more => 0 }) ];
    }
    elsif ($path =~ m{\A/file/}) {
        $result{$path} = await $try->({ file => $file });
    }
    elsif ($path =~ m{\A/file-unreadable/}) {
        $result{$path} = [ await $try->({ file => $unreadable }), await $try->({ body => 'hello', more => 0 }) ];
    }
    elsif ($path =~ m{\A/fh-closed/}) {
        open my $fh, '<:raw', $file or die "read $file: $!";
        close $fh;
        $result{$path} = [ await $try->({ fh => $fh, length => 5 }), await $try->({ body => 'hello', more => 0 }) ];
    }
    elsif ($path =~ m{\A/file-length/}) {
        $result{$path} = await $try->({ file => $file, length => 10 });
    }
    elsif ($path =~ m{\A/fh-length/}) {
        open my $fh, '<:raw', $file or die "read $file: $!";
        $result{$path} = [ await $try->({ fh => $fh, length => 10 }) ];
        push @{ $result{$path} }, await $try->({ body => 'world', more => 0 })
            if $result{$path}[0] =~ /short/;
    }
    elsif ($path =~ m{\A/fh/}) {
        open my $fh, '<:raw', $file or die "read $file: $!";
        $result{$path} = [ await $try->({ fh => $fh }) ];
        push @{ $result{$path} }, await $try->({ body => 'world', more => 0 })
            if $result{$path}[0] =~ /short/;
    }
};

my @logged;
my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, http2 => 1,
    log_level => 'info', logger => sub { push @logged, $_[0] }, access_log => undef);
$loop->add($server);
$server->listen->get;

# Sends one request, reads its response, then sends a second request on the
# same connection if it is still open. Returns the first response's head and
# body, whether the connection closed before the second request, and the
# second response.
sub exchange {
    my ($request, $done) = @_;
    @logged = ();
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port,
        Proto => 'tcp', Timeout => 2) or die "Cannot connect: $!";
    $sock->blocking(0);
    my ($wire, $eof) = ('', 0);
    my $pump = sub {
        my ($until) = @_;
        my $deadline = time + 3;
        while (!$eof && time < $deadline) {
            $loop->loop_once(0.02);
            my $n = sysread($sock, my $buf, 65536);
            if (defined $n) { $n ? ($wire .= $buf) : ($eof = 1) }
            last if $until && $until->($wire);
        }
    };
    syswrite $sock, $request;
    $pump->($done);
    my ($head, $body) = split /\r\n\r\n/, $wire, 2;
    my %r = (head => $head // '', body => $body // '', closed => $eof);
    unless ($eof) {
        $wire = '';
        syswrite $sock, "GET /body/5 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        $pump->();
        $r{second} = $wire;
    }
    close $sock;
    return \%r;
}

sub get_once {
    my ($path, %o) = @_;
    my $method  = $o{method}  // 'GET';
    my $version = $o{version} // '1.1';
    my $extra   = $version eq '1.0' ? "Connection: keep-alive\r\n" : '';
    return exchange("$method $path HTTP/$version\r\nHost: localhost\r\n$extra\r\n", $o{done});
}

my $ends_with = sub { my ($tail) = @_; sub { $_[0] =~ /\r\n\r\n\Q$tail\E\z/ } };

sub logged_errors { [ map { $_->{message} } grep { $_->{level} eq 'error' } @logged ] }

subtest 'a body longer than declared fails and the connection closes' => sub {
    my $r = get_once('/body/3');
    like($result{'/body/3'}, qr/would send 5 bytes, 2 more than the declared content-length \(3\)/, 'the body send fails');
    like($r->{head}, qr{\AHTTP/1\.1 200 }, 'the head went out');
    is($r->{body}, '', 'no body bytes follow it');
    ok($r->{closed}, 'the connection closes before a second request');
    like(logged_errors(), [qr/incomplete response/], 'the incomplete response is logged');
};

subtest 'a terminal body shorter than declared fails and the connection closes' => sub {
    my $r = get_once('/body/10');
    like($result{'/body/10'}, qr/ends the response 5 bytes short of the declared content-length \(10\)/, 'the body send fails');
    is($r->{body}, '', 'no body bytes follow the head');
    ok($r->{closed}, 'the connection closes before a second request');
    like(logged_errors(), [qr/incomplete response/], 'the incomplete response is logged');
};

subtest 'HTTP/1.0 is counted too' => sub {
    my $r = get_once('/body/3', version => '1.0');
    like($result{'/body/3'}, qr/2 more than the declared content-length/, 'the body send fails');
    is($r->{body}, '', 'no body bytes follow the head');
    ok($r->{closed}, 'the connection closes');
    like(logged_errors(), [qr/incomplete response/], 'the incomplete response is logged');
};

subtest 'control: a body of the declared length keeps the connection' => sub {
    my $r = get_once('/body/5', done => $ends_with->('hello'));
    is($result{'/body/5'}, 'NO-ERROR', 'the body send succeeds');
    is($r->{body}, 'hello', 'the body is sent');
    ok(!$r->{closed}, 'the connection stays open');
    like($r->{second} // '', qr{\AHTTP/1\.1 200 .*\r\n\r\nhello\z}s, 'a second request gets a clean response');
    is(logged_errors(), [], 'nothing is logged as an error');
};

subtest 'a second overlapping body send is counted at call time' => sub {
    my $declared = length($LARGE) + 5;
    my $r = get_once("/overlap/$declared", done => sub { $_[0] =~ /abcde\z/ });
    my ($first_pending, $second, $last) = @{ $result{"/overlap/$declared"} };
    ok($first_pending, 'the first small send was still pending when the second was called');
    like($second, qr/would send 3 bytes, 1 more than the declared content-length \($declared\)/, 'the second send fails');
    is($last, 'NO-ERROR', 'the right remainder still completes the response');
    is(length $r->{body}, $declared, 'only the declared bytes are written');
    is(substr($r->{body}, -6), 'xabcde', 'ending with the small sends that fit');
    ok(!$r->{closed}, 'the connection stays open');
    is(logged_errors(), [], 'nothing is logged as an error');
};

subtest 'HEAD carries a declared length and no body, and is not counted' => sub {
    my $r = get_once('/body/5', method => 'HEAD', done => sub { $_[0] =~ /\r\n\r\n\z/ });
    is($result{'/body/5'}, 'NO-ERROR', 'the body send succeeds');
    like($r->{head}, qr{\AHTTP/1\.1 200 .*\r\ncontent-length: 5\r\n}s, 'the head declares 5');
    is($r->{body}, '', 'no body is sent');
    ok(!$r->{closed}, 'the connection is reused');
    like($r->{second} // '', qr{\AHTTP/1\.1 200 }, 'a second request gets a response');

    # The usual HEAD handler declares the GET length and sends no body.
    $r = get_once('/empty/5', method => 'HEAD', done => sub { $_[0] =~ /\r\n\r\n\z/ });
    is($result{'/empty/5'}, 'NO-ERROR', 'an empty body against a declared 5 succeeds');
    ok(!$r->{closed}, 'the connection is reused');
    is(logged_errors(), [], 'nothing is logged as an error');
};

subtest 'an application that catches the failure may send the right body' => sub {
    my $r = get_once('/recover/5', done => $ends_with->('hello'));
    my ($first, $second) = @{ $result{'/recover/5'} };
    like($first, qr/6 more than the declared content-length \(5\)/, 'the long body fails');
    is($second, 'NO-ERROR', 'the right body succeeds');
    is($r->{body}, 'hello', 'the client reads exactly the declared body');
    like($r->{second} // '', qr{\AHTTP/1\.1 200 .*\r\n\r\nhello\z}s, 'the next request on the connection is clean');
    is(logged_errors(), [], 'nothing is logged as an error');
};

subtest 'a file body is counted by its size' => sub {
    my $r = get_once('/file/3');
    like($result{'/file/3'}, qr/would send 5 bytes, 2 more than the declared content-length \(3\)/, 'a 5-byte file fails against 3');
    is($r->{body}, '', 'none of the file is written');
    ok($r->{closed}, 'the connection closes');
    like(logged_errors(), [qr/incomplete response/], 'the incomplete response is logged');

    $r = get_once('/file/5', done => $ends_with->('hello'));
    is($result{'/file/5'}, 'NO-ERROR', 'a 5-byte file passes against 5');
    is($r->{body}, 'hello', 'the file is sent');
    ok(!$r->{closed}, 'the connection stays open');
};

subtest 'an fh without a length is counted while it streams' => sub {
    my $r = get_once('/fh/3');
    like($result{'/fh/3'}[0], qr/would send more than the declared content-length \(3\)/, 'a 5-byte handle fails against 3');
    is($r->{body}, 'hel', 'it stops at the declared length');
    ok($r->{closed}, 'the connection closes');
    like(logged_errors(), [qr/incomplete response/], 'the incomplete response is logged');

    $r = get_once('/fh/10', done => $ends_with->('helloworld'));
    my ($short, $rest) = @{ $result{'/fh/10'} };
    like($short, qr/ends the response 5 bytes short of the declared content-length \(10\)/, 'a 5-byte handle fails against 10');
    is($rest, 'NO-ERROR', 'the count kept the bytes already written, so the remainder completes it');
    is($r->{body}, 'helloworld', 'the client reads the declared length');
    ok(!$r->{closed}, 'the connection stays open');
    is(logged_errors(), [], 'nothing is logged as an error');
};

subtest 'a file or fh send that fails before writing leaves the count as it was' => sub {
    SKIP: {
        skip 'running as root: an unreadable file is readable', 1 if $> == 0;
        my $r = get_once('/file-unreadable/5', done => $ends_with->('hello'));
        my ($failed, $retry) = @{ $result{'/file-unreadable/5'} };
        like($failed, qr/\ACannot read file: /, 'the unreadable file fails');
        is($retry, 'NO-ERROR', 'the right body then fits the declared length');
        is($r->{body}, 'hello', 'and is what the client reads');
        like($r->{second} // '', qr{\AHTTP/1\.1 200 .*\r\n\r\nhello\z}s, 'the connection is reused cleanly');
    }

    my $r = get_once('/fh-closed/5', done => $ends_with->('hello'));
    my ($failed, $retry) = @{ $result{'/fh-closed/5'} };
    like($failed, qr/\AFailed to read filehandle: /, 'a closed handle fails');
    is($retry, 'NO-ERROR', 'the right body then fits the declared length');
    is($r->{body}, 'hello', 'and is what the client reads');
    ok(!$r->{closed}, 'the connection stays open');
    is(logged_errors(), [], 'nothing is logged as an error');
};

subtest 'a length longer than the file or handle holds is short' => sub {
    my $r = get_once('/file-length/10');
    like($result{'/file-length/10'}, qr/ends the response 5 bytes short of the declared content-length \(10\)/,
        'a file is counted by what it holds, not the length given');
    is($r->{body}, '', 'none of it is written');
    ok($r->{closed}, 'the connection closes');

    $r = get_once('/fh-length/10', done => $ends_with->('helloworld'));
    my ($short, $rest) = @{ $result{'/fh-length/10'} };
    like($short, qr/ends the response 5 bytes short of the declared content-length \(10\)/,
        'a handle that ends before its length fails');
    is($rest, 'NO-ERROR', 'the count kept the bytes already written, so the remainder completes it');
    is($r->{body}, 'helloworld', 'the client reads the declared length');
    ok(!$r->{closed}, 'the connection stays open');
    is(logged_errors(), [], 'nothing is logged as an error');
};

subtest 'h2c: the body is not counted' => sub {
    require PAGI::Server::Protocol::HTTP2;
    skip_all 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)'
        unless PAGI::Server::Protocol::HTTP2->available;
    delete $result{'/body/3'};

    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $_->blocking(0) for $sock_a, $sock_b;
    my $stream = IO::Async::Stream->new(read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => PAGI::Server::Protocol::HTTP1->new,
        server => $server, h2_protocol => $server->{http2_protocol}, h2c_enabled => $server->{h2c_enabled},
    );
    $server->add_child($stream);
    $conn->start;

    require Net::HTTP2::nghttp2::Session;
    my $closed;
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers => sub { 0 }, on_header => sub { 0 }, on_frame_recv => sub { 0 },
        on_data_chunk_recv => sub { 0 }, on_stream_close => sub { $closed = 1; 0 },
    });
    my $pump = sub {
        for (1 .. $_[0]) {
            $loop->loop_once(0.05);
            my $buf = '';
            $sock_b->sysread($buf, 16384);
            $client->mem_recv($buf) if length $buf;
            my $out = $client->mem_send;
            $sock_b->syswrite($out) if length $out;
            last if $closed;
        }
    };
    $client->send_connection_preface;
    $sock_b->syswrite($client->mem_send);
    $pump->(5);
    $client->submit_request(method => 'GET', path => '/body/3', scheme => 'http', authority => 'localhost');
    $sock_b->syswrite($client->mem_send);
    $pump->(40);
    $stream->close_now;

    is($result{'/body/3'}, 'NO-ERROR', 'a 5-byte body against 3 is sent as given');
};

$server->shutdown->get;
$loop->remove($server);

done_testing;
