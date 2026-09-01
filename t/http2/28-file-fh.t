use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use File::Temp qw(tempfile);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# ============================================================
# Test: HTTP/2 `file` body streaming
# ============================================================
# http.response.body carrying 'file' streams through the per-stream send
# queue (PAGI::Server::AsyncFile->read_file_chunked + $emit_chunk), one
# chunk at a time under this stream's own backpressure -- never slurped as
# one scalar. The fixture is > 4x FILE_CHUNK_SIZE so a real multi-chunk
# pump is exercised, and assertions are byte-exact against a known pattern.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (lifted verbatim from t/http2/27-head.t)
# ============================================================

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app   => $args{app} // sub { },
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
        http2 => 1,
        validate_events => 0,
        %args,
    );
    $loop->add($server);
    return $server;
}

sub create_h2c_connection {
    my (%overrides) = @_;

    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0)
        or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);

    my $app = $overrides{app} // sub { };
    my $server = $overrides{server} // create_test_server(app => $app);

    my $stream = IO::Async::Stream->new(
        read_handle  => $sock_a,
        write_handle => $sock_a,
        on_read => sub { 0 },
    );

    my $conn = PAGI::Server::Connection->new(
        stream        => $stream,
        app           => $app,
        protocol      => $protocol,
        server        => $server,
        h2_protocol   => $server->{http2_protocol},
        h2c_enabled   => $server->{h2c_enabled},
    );

    $server->add_child($stream);
    $conn->start;

    return ($conn, $stream, $sock_b, $server);
}

sub create_client {
    my (%overrides) = @_;
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(
        callbacks => {
            on_begin_headers   => $overrides{on_begin_headers}   // sub { 0 },
            on_header          => $overrides{on_header}          // sub { 0 },
            on_frame_recv      => $overrides{on_frame_recv}      // sub { 0 },
            on_data_chunk_recv => $overrides{on_data_chunk_recv} // sub { 0 },
            on_stream_close    => $overrides{on_stream_close}    // sub { 0 },
        },
    );
}

sub h2c_handshake {
    my ($client, $client_sock) = @_;
    $client->send_connection_preface;
    my $data = $client->mem_send;
    $client_sock->syswrite($data);
    for (1..5) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    $rounds //= 10;
    for (1..$rounds) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

# ============================================================
# Request helper
# ============================================================

sub h2_request {
    my ($method, $path, %opts) = @_;
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $opts{app});

    my %headers;
    my $body = '';
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $d)    = @_; $body .= $d;         return 0 },
    );
    h2c_handshake($client, $client_sock);
    $client->submit_request(
        method    => $method,
        path      => $path,
        scheme    => 'http',
        authority => 'localhost',
        headers   => $opts{headers} // [],
    );
    $client_sock->syswrite($client->mem_send);
    # File streaming crosses IO::Async::Function worker-process round trips,
    # so give it more ticks than the plain in-memory h2 tests need.
    exchange_frames($client, $client_sock, $opts{rounds} // 100);

    $stream_io->close_now;
    $loop->remove($server);
    return { headers => \%headers, body => $body };
}

sub get_h2 {
    my ($path, %opts) = @_;
    return h2_request('GET', $path, app => $opts{app} // $File::app);
}

# ============================================================
# Fixture: 300_000 bytes (> 4x FILE_CHUNK_SIZE == 262_144) of a known,
# non-repeating-at-chunk-boundary pattern so truncation/misalignment bugs
# are visible in a byte-exact comparison.
# ============================================================

my $unit = join('', map { chr(32 + ($_ % 95)) } 0..96);   # 97 printable bytes
my $pattern = $unit x (int(300_000 / length($unit)) + 1);
$pattern = substr($pattern, 0, 300_000);

my ($big_fh, $big_file) = tempfile(UNLINK => 1);
binmode $big_fh;
print $big_fh $pattern;
close $big_fh;

# Small fixture (well under the default 64KB sync_file_threshold) so I3's
# h2 sync fast-path is the one actually exercised for /file-small below.
my $small_content = substr($pattern, 0, 500);
my ($small_fh, $small_file) = tempfile(UNLINK => 1);
binmode $small_fh;
print $small_fh $small_content;
close $small_fh;

# ============================================================
# App
# ============================================================

package File;
our $BIG   = $big_file;
our $SMALL = $small_file;
package main;

$File::app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    my $path = $scope->{path} // '/';

    if ($path eq '/file-full') {
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','application/octet-stream']] });
        await $send->({ type => 'http.response.body', file => $File::BIG });
        return;
    }
    if ($path eq '/file-range') {
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','application/octet-stream']] });
        await $send->({ type => 'http.response.body', file => $File::BIG,
                         offset => 1000, length => 1000 });
        return;
    }
    if ($path eq '/file-past-eof') {
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','application/octet-stream']] });
        await $send->({ type => 'http.response.body', file => $File::BIG,
                         offset => 999_999_999 });
        return;
    }
    if ($path eq '/file-small') {
        # I3: a file at or under sync_file_threshold (default 64KB) takes
        # the h2 sync fast path (one queue chunk, no worker-pool round
        # trip). We can't easily assert "no worker was used" from here, so
        # this asserts byte-exact correctness -- the threshold path is the
        # one actually exercised for a file this size.
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','application/octet-stream']] });
        await $send->({ type => 'http.response.body', file => $File::SMALL });
        return;
    }
    if ($path eq '/file-before-start') {
        # M2: a file event (bad file) arriving in an illegal sequence state
        # (before http.response.start) must report the SEQUENCE error, not
        # the misleading "File not found" -- h1 parity.
        my $err = do {
            local $@;
            eval { await $send->({ type => 'http.response.body', file => '/nonexistent' }) };
            $@;
        };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','text/plain']] });
        await $send->({ type => 'http.response.body', body => "err=$err", more => 0 });
        return;
    }
    if ($path eq '/file-missing') {
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','text/plain']] });
        my $err = do {
            local $@;
            eval { await $send->({ type => 'http.response.body', file => '/nonexistent' }) };
            $@;
        };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.body', body => "err=$err", more => 0 });
        return;
    }
    if ($path eq '/file-after-chunks') {
        # A response's body is either inline events or one opaque event, never
        # both (PAGI::Spec::Www, "Payload kinds do not mix within a response").
        # The file send is rejected; the failed send delivers no bytes and
        # leaves the sequence state untouched, so the application recovers by
        # finishing inline -- exactly as it may after any failed file send.
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','application/octet-stream']] });
        await $send->({ type => 'http.response.body', body => 'x', more => 1 });
        my $err = do {
            local $@;
            eval { await $send->({ type => 'http.response.body', file => $File::BIG }) };
            $@;
        };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.body', body => "err=$err", more => 0 });
        return;
    }
    if ($path eq '/fh-full') {
        open my $fh, '<:raw', $File::BIG or die "Cannot open: $!";
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','application/octet-stream']] });
        await $send->({ type => 'http.response.body', fh => $fh });
        $FhOwn::CLOSED_OK = (close $fh ? 1 : 0);
        return;
    }
    if ($path eq '/fh-range') {
        open my $fh, '<:raw', $File::BIG or die "Cannot open: $!";
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','application/octet-stream']] });
        await $send->({ type => 'http.response.body', fh => $fh,
                         offset => 1000, length => 1000 });
        close $fh;
        return;
    }
    if ($path eq '/fh-past-eof') {
        open my $fh, '<:raw', $File::BIG or die "Cannot open: $!";
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','application/octet-stream']] });
        await $send->({ type => 'http.response.body', fh => $fh,
                         offset => 999_999_999 });
        close $fh;
        return;
    }
    if ($path eq '/fh-unseekable') {
        # A pipe's read end is not seekable: a nonzero offset forces the
        # fh arm's seek() call, which fails with ESPIPE ("Illegal seek").
        pipe(my $read_fh, my $write_fh) or die "pipe: $!";
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','text/plain']] });
        my $err = do {
            local $@;
            eval { await $send->({ type => 'http.response.body', fh => $read_fh, offset => 1 }) };
            $@;
        };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.body', body => "err=$err", more => 0 });
        close $read_fh;
        close $write_fh;
        return;
    }
    if ($path eq '/fh-closed') {
        open my $fh, '<:raw', $File::BIG or die "Cannot open: $!";
        close $fh;
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','text/plain']] });
        my $err = do {
            local $@;
            eval { await $send->({ type => 'http.response.body', fh => $fh }) };
            $@;
        };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.body', body => "err=$err", more => 0 });
        return;
    }
    die "unknown path $path";
};

# ============================================================
# Assertions
# ============================================================

is( length get_h2('/file-full')->{body}, 300_000, 'full file streamed' );
is( get_h2('/file-full')->{body}, $pattern, 'byte-exact content' );
is( get_h2('/file-range')->{body}, substr($pattern,1000,1000), 'offset+length honored' );
is( get_h2('/file-past-eof')->{body}, '', 'offset past EOF sends zero bytes, stream ends cleanly' );
is( get_h2('/file-small')->{body}, $small_content,
    'small file (<= sync_file_threshold) reads correctly via the h2 sync fast path (I3)' );
like( get_h2('/file-before-start')->{body}, qr/err=.*before http\.response\.start/,
    'file event before start reports the SEQUENCE error, not File not found (M2)' );
like( get_h2('/file-missing')->{body}, qr/err=.*File not found/, 'open failure fails the Future; app recovered with a normal body' );
like( get_h2('/file-after-chunks')->{body},
    qr/\Ax\Qerr=\E.*after inline body bytes/,
    'a file body after inline chunks is rejected; the chunk already sent stands and the app recovers inline' );

is( get_h2('/fh-full')->{body}, $pattern, 'fh streamed byte-exact' );
ok( $FhOwn::CLOSED_OK, 'application still owns the handle after the send resolves' );
is( get_h2('/fh-range')->{body}, substr($pattern,1000,1000), 'fh offset+length honored' );
is( get_h2('/fh-past-eof')->{body}, '', 'fh offset past EOF sends zero bytes, stream ends cleanly' );
like( get_h2('/fh-unseekable')->{body}, qr/err=.*Cannot seek/,
    'seek failure on an unseekable (pipe) fh fails the Future loudly; app recovered' );
like( get_h2('/fh-closed')->{body}, qr/err=.*Failed to read filehandle/, 'closed fh fails the Future; app recovered' );

done_testing;
