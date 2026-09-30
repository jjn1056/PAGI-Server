use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::JSONLog;

# Every HTTP/2 stream is one request, and gets one access-log record like an
# HTTP/1.1 request does: written when the stream closes, with the status sent
# (null if none was), the body bytes sent, and protocol HTTP/2.

use constant H2_CANCEL => 8;   # RST_STREAM error code CANCEL (RFC 9113)

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;
my $json_ok  = PAGI::Server::JSONLog::available();
my $decoder  = $json_ok ? do { require JSON::MaybeXS; JSON::MaybeXS->new(utf8 => 1) } : undef;

# A connection wired as the server wires one, writing its access log to
# $$log_ref in $format.
sub h2_connection {
    my (%args) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $_->blocking(0) for $sock_a, $sock_b;
    my $server = PAGI::Server->new(
        app => $args{app}, host => '127.0.0.1', port => 0, quiet => 1, http2 => 1,
        ($args{max_body_size} ? (max_body_size => $args{max_body_size}) : ()),
    );
    $loop->add($server);
    open my $log_fh, '>', $args{log_ref} or die "in-memory log: $!";
    $log_fh->autoflush(1);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 },
    );
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $args{app}, protocol => $protocol, server => $server,
        h2_protocol   => $server->{http2_protocol},
        h2c_enabled   => 1,
        max_body_size => $server->{max_body_size},
        access_log    => $log_fh,
        _access_log_formatter => PAGI::Server->_compile_access_log_format($args{format}),
    );
    $server->add_child($stream);
    $conn->start;

    require Net::HTTP2::nghttp2::Session;
    my %closed;
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { 0 },
        on_frame_recv      => sub { 0 },
        on_data_chunk_recv => sub { 0 },
        on_stream_close    => sub { $closed{$_[0]} = 1; 0 },
    });
    my $pump = sub {
        my ($until) = @_;
        for (1 .. 200) {
            $loop->loop_once(0.02);
            my $buf = '';
            $sock_b->sysread($buf, 65536);
            $client->mem_recv($buf) if length $buf;
            my $out = $client->mem_send;
            $sock_b->syswrite($out) if length $out;
            last if $until && $until->();
        }
    };
    $client->send_connection_preface;
    $sock_b->syswrite($client->mem_send);
    $pump->();
    return {
        # The server keeps only connections it accepts; this one is built by
        # hand, so the test holds it or it is freed and never reads a frame.
        conn   => $conn,
        client => $client, sock => $sock_b, pump => $pump, closed => \%closed,
        done => sub { $stream->close_now; $loop->remove($server) },
    };
}

sub request {
    my ($h2, %request) = @_;
    my $sid = $h2->{client}->submit_request(
        scheme => 'http', authority => 'localhost', %request,
    );
    $h2->{sock}->syswrite($h2->{client}->mem_send);
    return $sid;
}

sub records {
    my ($log) = @_;
    return [ map { $decoder->decode($_) } grep { length } split /\n/, $log ];
}

my $hello = async sub {
    my ($scope, $receive, $send) = @_;
    return if $scope->{type} ne 'http';
    my $status = $scope->{path} eq '/missing' ? 404 : 200;
    my $body   = $scope->{path} eq '/missing' ? 'nope' : 'hello';
    await $send->({ type => 'http.response.start', status => $status,
                    headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => $body, more => 0 });
};

subtest 'an HTTP/2 request writes one JSON record' => sub {
    skip_all 'Cpanel::JSON::XS not installed' unless $json_ok;
    my $log = '';
    my $h2 = h2_connection(app => $hello, format => 'json', log_ref => \$log);
    my $sid = request($h2, method => 'GET', path => '/path?x=1',
        headers => [['user-agent', 'h2-test/1.0']]);
    $h2->{pump}->(sub { $h2->{closed}{$sid} && length $log });

    my $records = records($log);
    is(scalar @$records, 1, 'one record');
    is([@{ $records->[0] }{qw(method path query protocol status size user_agent)}],
        ['GET', '/path', 'x=1', 'HTTP/2', 200, 5, 'h2-test/1.0'],
        'method, path, query, protocol, status, body bytes and user agent');
    $h2->{done}->();
};

subtest 'clf reads the same for HTTP/2' => sub {
    my $log = '';
    my $h2 = h2_connection(app => $hello, format => 'clf', log_ref => \$log);
    my $sid = request($h2, method => 'GET', path => '/p?x=1');
    $h2->{pump}->(sub { $h2->{closed}{$sid} && length $log });
    like($log, qr{\A\S+ - - \[[^\]]+\] "GET /p\?x=1" 200 \d+\.\d+s\n\z}, 'one clf line');
    $h2->{done}->();
};

subtest 'concurrent streams each get their own record' => sub {
    skip_all 'Cpanel::JSON::XS not installed' unless $json_ok;
    my $log = '';
    my $h2 = h2_connection(app => $hello, format => 'json', log_ref => \$log);
    my @sids = (request($h2, method => 'GET', path => '/found'),
                request($h2, method => 'GET', path => '/missing'));
    $h2->{pump}->(sub { (grep { $h2->{closed}{$_} } @sids) == 2 && ($log =~ tr/\n//) == 2 });

    my %by_path = map { $_->{path} => $_ } @{ records($log) };
    is([@{ $by_path{'/found'} }{qw(status size)}],   [200, 5], '/found: 200, 5 bytes');
    is([@{ $by_path{'/missing'} }{qw(status size)}], [404, 4], '/missing: 404, 4 bytes');
    $h2->{done}->();
};

subtest 'a streamed body counts every chunk sent' => sub {
    skip_all 'Cpanel::JSON::XS not installed' unless $json_ok;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return if $scope->{type} ne 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        await $send->({ type => 'http.response.body', body => 'ab', more => 1 });
        await $send->({ type => 'http.response.body', body => 'cde', more => 1 });
        await $send->({ type => 'http.response.body', body => '', more => 0 });
    };
    my $log = '';
    my $h2 = h2_connection(app => $app, format => 'json', log_ref => \$log);
    my $sid = request($h2, method => 'GET', path => '/stream');
    $h2->{pump}->(sub { $h2->{closed}{$sid} && length $log });
    is(records($log)->[0]{size}, 5, 'two chunks, five bytes');
    $h2->{done}->();
};

subtest 'a stream reset before any response logs a null status' => sub {
    skip_all 'Cpanel::JSON::XS not installed' unless $json_ok;
    my $never = Future->new;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return if $scope->{type} ne 'http';
        await $never;
    };
    my $log = '';
    my $h2 = h2_connection(app => $app, format => 'json', log_ref => \$log);
    my $sid = request($h2, method => 'GET', path => '/slow');
    $h2->{pump}->(sub { 0 }) for 1;   # let the app start and park
    $h2->{client}->submit_rst_stream($sid, H2_CANCEL);
    $h2->{sock}->syswrite($h2->{client}->mem_send);
    $h2->{pump}->(sub { length $log });

    my $records = records($log);
    is(scalar @$records, 1, 'the reset stream is still logged');
    is([@{ $records->[0] }{qw(path status size)}], ['/slow', undef, 0],
        'with no status and no bytes');
    $never->done;
    $h2->{done}->();
};

subtest "the server's own 413 refusal is logged" => sub {
    skip_all 'Cpanel::JSON::XS not installed' unless $json_ok;
    my $log = '';
    my $h2 = h2_connection(app => $hello, format => 'json', log_ref => \$log,
                           max_body_size => 100);
    my $sid = request($h2, method => 'POST', path => '/upload',
        headers => [['content-length', '50000']], body => sub { return undef });
    $h2->{pump}->(sub { length $log });
    is([@{ records($log)->[0] // {} }{qw(method path status)}], ['POST', '/upload', 413],
        'status 413');
    $h2->{done}->();
};

subtest "the server's own 501 for a plain CONNECT is logged" => sub {
    skip_all 'Cpanel::JSON::XS not installed' unless $json_ok;
    my $log = '';
    my $h2 = h2_connection(app => $hello, format => 'json', log_ref => \$log);
    # nghttp2 refuses a plain CONNECT itself (GOAWAY) before the server sees
    # it, so the server's own 501 is defense in depth and unreachable from
    # the wire. Drive it the way t/http2/12-error-handling.t does: call the
    # request handler with plain-CONNECT pseudo-headers on an unused stream id.
    $h2->{conn}->_h2_on_request(
        99, { ':method' => 'CONNECT', ':authority' => 'proxy.example.com:443' }, [], 0,
    );
    $h2->{pump}->(sub { length $log });
    is([@{ records($log)->[0] // {} }{qw(method status)}], ['CONNECT', 501], 'status 501');
    $h2->{done}->();
};

subtest 'an SSE stream over HTTP/2 is logged when it ends' => sub {
    skip_all 'Cpanel::JSON::XS not installed' unless $json_ok;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return if $scope->{type} ne 'sse';
        await $send->({ type => 'sse.start', status => 200, headers => [] });
        await $send->({ type => 'sse.send', data => 'hi' });
        await $send->({ type => 'sse.close' });
    };
    my $log = '';
    my $h2 = h2_connection(app => $app, format => 'json', log_ref => \$log);
    my $sid = request($h2, method => 'GET', path => '/events',
        headers => [['accept', 'text/event-stream']]);
    $h2->{pump}->(sub { $h2->{closed}{$sid} && length $log });
    my $record = records($log)->[0] // {};
    is([@$record{qw(path protocol status)}], ['/events', 'HTTP/2', 200], 'status 200');
    ok($record->{size} > 0, 'with the event bytes counted');
    $h2->{done}->();
};

done_testing;
