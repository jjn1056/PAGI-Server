use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Future::AsyncAwait;
use Socket qw(AF_UNIX SOCK_STREAM);
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::EventValidator;
use PAGI::Server::Protocol::HTTP1;

# Header names are RFC 9110 tokens (S5.1) and values are field values (S5.5)
# (Www.pod "Header byte safety"). A failure names the event, the header's
# index and its name, and never echoes the value.

my $V = 'PAGI::Server::EventValidator';

# A test name with the input's non-printable characters escaped.
sub shown { (my $s = $_[0]) =~ s/([^\x20-\x7E])/sprintf '\x{%x}', ord $1/ge; $s }

subtest 'header names are tokens' => sub {
    for my $name ('x-ok', 'X-Ok', "!#\$%&'*+-.^_`|~09AZaz") {
        ok(lives { PAGI::Server::EventValidator::check_header_name($name) }, "name '$name' passes");
    }
    my %fails = (
        ''                  => qr/\AInvalid header name is empty\n\z/,
        'x a'               => qr/\AInvalid header name 'x a' contains SP \(\\x20\) at offset 1 /,
        'x-a:b'             => qr/\AInvalid header name 'x-a:b' contains ':' \(\\x3a\) at offset 3 /,
        'set-cookie:a=b; x' => qr/\AInvalid header name 'set-cookie:a=b; x' contains ':' \(\\x3a\) at offset 10 /,
        '(x)'               => qr/\AInvalid header name '\(x\)' contains '\(' \(\\x28\) at offset 0 /,
        "\x85x"             => qr/\AInvalid header name '\\x85x' contains a non-ASCII byte \(\\x85\) at offset 0 /,
        "x-\xC9tag"         => qr/\AInvalid header name 'x-\\xc9tag' contains a non-ASCII byte \(\\xc9\) at offset 2 /,
        ':status'           => qr/\AInvalid header name ':status' contains ':' \(\\x3a\) at offset 0 /,
        ':foo'              => qr/\AInvalid header name ':foo' contains ':' \(\\x3a\) at offset 0 /,
        "x\x01y"            => qr/\AInvalid header name 'x\\x01y' contains a control character \(\\x01\) at offset 1 /,
        "x\r\ny"            => qr/\AInvalid header name 'x\\x0d\\x0ay' contains CR \(\\x0d\) at offset 1 /,
        "x\x{263a}"         => qr/\AInvalid header name 'x\\x\{263a\}' contains a character above 0xFF \(\\x\{263a\}\) at offset 1 /,
    );
    for my $name (sort keys %fails) {
        like(dies { PAGI::Server::EventValidator::check_header_name($name) }, $fails{$name},
            "name '" . shown($name) . "' fails");
    }
    like(dies { PAGI::Server::EventValidator::check_header_name('x a') },
        qr/\(a header name must be an RFC 9110 token\)\n\z/, 'the name message says what a name must be');
};

subtest 'header values are RFC 9110 field values' => sub {
    for my $value ('', 'v', "a\tb", 'a b', "\xC9", "caf\xE9", "x\xFFy") {
        ok(lives { PAGI::Server::EventValidator::check_header_value($value) },
            "value '" . shown($value) . "' passes");
    }
    my %fails = (
        ' v'           => qr/\AInvalid header value starts with SP \(\\x20\); trim it\n\z/,
        'v '           => qr/\AInvalid header value ends with SP \(\\x20\); trim it\n\z/,
        "\tv"          => qr/\AInvalid header value starts with HTAB \(\\x09\); trim it\n\z/,
        ' '            => qr/\AInvalid header value starts with SP \(\\x20\); trim it\n\z/,
        "a\x01b"       => qr/\AInvalid header value contains a control character \(\\x01\) at offset 1\n\z/,
        "a\x7Fb"       => qr/\AInvalid header value contains DEL \(\\x7f\) at offset 1\n\z/,
        "a\r\nb"       => qr/\AInvalid header value contains CR \(\\x0d\) at offset 1\n\z/,
        "a\nb"         => qr/\AInvalid header value contains LF \(\\x0a\) at offset 1\n\z/,
        "a\0b"         => qr/\AInvalid header value contains NUL \(\\x00\) at offset 1\n\z/,
        "ok\x{263a}"   => qr/\AInvalid header value contains a character above 0xFF \(\\x\{263a\}\) at offset 2 \(encode it to bytes\)\n\z/,
    );
    for my $value (sort keys %fails) {
        like(dies { PAGI::Server::EventValidator::check_header_value($value) }, $fails{$value},
            "value '" . shown($value) . "' fails");
    }
};

subtest 'a failure names the event and the header, never the value' => sub {
    my @cases = (
        [ 'http.response.start',    sub { $V->can('validate_http_send')->({ type => 'http.response.start', status => 200, headers => $_[0] }) } ],
        [ 'http.response.trailers', sub { $V->can('validate_http_send')->({ type => 'http.response.trailers', headers => $_[0] }) } ],
        [ 'websocket.accept',       sub { $V->can('validate_websocket_send')->({ type => 'websocket.accept', headers => $_[0] }, {}) } ],
        [ 'sse.start',              sub { $V->can('validate_sse_send')->({ type => 'sse.start', headers => $_[0] }, {}) } ],
        [ 'http.response.start',    sub { $V->can('validate_websocket_send')->({ type => 'http.response.start', status => 401, headers => $_[0] }, {}) }, 'websocket refusal' ],
        [ 'http.response.start',    sub { $V->can('validate_sse_send')->({ type => 'http.response.start', status => 404, headers => $_[0] }, {}) }, 'sse refusal' ],
    );
    for my $case (@cases) {
        my ($type, $send, $label) = @$case;
        $label //= $type;
        like(dies { $send->([['a', 'ok'], ['x-v', "a\rb"]]) },
            qr/\A\Q$type\E header 1 \('x-v'\): value contains CR \(\\x0d\) at offset 1\n\z/,
            "$label: a value failure names the event, the index and the name");
        like(dies { $send->([['x a', 'v']]) },
            qr/\A\Q$type\E header 0: name 'x a' contains SP \(\\x20\) at offset 1 \(a header name must be an RFC 9110 token\)\n\z/,
            "$label: a name failure names the event and the index");
        ok(lives { $send->([['x-ok', 'v'], ['X-Ok', ''], ['x-b', "caf\xE9 \tau lait"]]) },
            "$label: valid headers pass");
    }

    my $err = dies { $V->can('validate_http_send')->({ type => 'http.response.start', status => 200,
        headers => [['x-v', "secret\nmore"]] }) };
    like($err, qr/header 0 \('x-v'\): value contains LF/, 'the secret value fails');
    unlike($err, qr/secret|more/, 'the message does not echo the value');

    $err = dies { $V->can('validate_http_send')->({ type => 'http.response.start', status => 200,
        headers => [["x\r\nInjected: yes", 'v']] }) };
    unlike($err, qr/[\r\x01]|\n./s, 'an escaped name puts no control bytes in the message');
};

# --- Real server ---------------------------------------------------------

my $loop = IO::Async::Loop->new;

# The app sends the given header and lets a failed send die, so the server
# answers 500 and logs the failure; the log is captured and checked.
sub header_app {
    my ($name) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope" if $scope->{type} ne 'http';
        await $receive->();
        await $send->({ type => 'http.response.start', status => 200,
            headers => [['content-type', 'text/plain'], [$name, 'v']] });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    };
}

sub h1_get {
    my ($name) = @_;
    my @logged;
    my $server = PAGI::Server->new(
        app => header_app($name), host => '127.0.0.1', port => 0,
        log_level => 'info', logger => sub { push @logged, $_[0] }, access_log => undef,
    );
    $loop->add($server);
    $server->listen->get;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port,
        Proto => 'tcp', Timeout => 2) or die "Cannot connect: $!";
    print $sock "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    $sock->blocking(0);
    my ($wire, $eof) = ('', 0);
    my $deadline = time + 3;
    while (!$eof && time < $deadline) {
        $loop->loop_once(0.05);
        my $n = sysread($sock, my $buf, 4096);
        if (defined $n) { $n ? ($wire .= $buf) : ($eof = 1) }
    }
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
    return ($wire, \@logged);
}

subtest 'HTTP/1.1: a non-token name never reaches the wire' => sub {
    my ($wire, $logged) = h1_get('set-cookie:a=b; x');
    like($wire, qr{\AHTTP/1\.1 500 }, 'the response is a 500');
    unlike($wire, qr/set-cookie:a=b/i, 'the bad name is not on the wire');
    my @errors = grep { $_->{level} eq 'error' } @$logged;
    is(scalar @errors, 1, 'one error is logged');
    like($errors[0]{message} // '', qr/http\.response\.start header 1: name 'set-cookie:a=b; x' contains ':'/,
        'the log names the header');

    ($wire, $logged) = h1_get('x-ok');
    like($wire, qr{\AHTTP/1\.1 200 }, 'control: a token name gives a 200');
    like($wire, qr/\r\nx-ok: v\r\n/, 'control: the header is sent');
    is([grep { $_->{level} eq 'error' } @$logged], [], 'control: nothing is logged as an error');
};

subtest 'h2c: a non-token name never reaches the wire' => sub {
    require PAGI::Server::Protocol::HTTP2;
    skip_all 'HTTP/2 not available (Net::HTTP2::nghttp2 0.013+ required)'
        unless PAGI::Server::Protocol::HTTP2->available;

    my $h2c_get = sub {
        my ($name) = @_;
        my @logged;
        my $app = header_app($name);
        my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, http2 => 1,
            log_level => 'info', logger => sub { push @logged, $_[0] }, access_log => undef);
        $loop->add($server);

        socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
        $_->blocking(0) for $sock_a, $sock_b;
        my $stream = IO::Async::Stream->new(read_handle => $sock_a, write_handle => $sock_a,
            on_read => sub { 0 });
        my $conn = PAGI::Server::Connection->new(
            stream => $stream, app => $app, protocol => PAGI::Server::Protocol::HTTP1->new,
            server => $server, h2_protocol => $server->{http2_protocol},
            h2c_enabled => $server->{h2c_enabled},
        );
        $server->add_child($stream);
        $conn->start;

        require Net::HTTP2::nghttp2::Session;
        my (%headers, $closed);
        my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
            on_begin_headers   => sub { 0 },
            on_header          => sub { my (undef, $n, $v) = @_; $headers{$n} = $v; 0 },
            on_frame_recv      => sub { 0 },
            on_data_chunk_recv => sub { 0 },
            on_stream_close    => sub { $closed = 1; 0 },
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
        $client->submit_request(method => 'GET', path => '/', scheme => 'http', authority => 'localhost');
        $sock_b->syswrite($client->mem_send);
        $pump->(40);

        $stream->close_now;
        $loop->remove($server);
        return (\%headers, \@logged);
    };

    my ($headers, $logged) = $h2c_get->('x-a:b');
    is($headers->{':status'}, '500', 'the response is a 500');
    ok(!grep({ /x-a/ } keys %$headers), 'the bad name is not sent');
    my @errors = grep { $_->{level} eq 'error' } @$logged;
    is(scalar @errors, 1, 'one error is logged');
    like($errors[0]{message} // '', qr/http\.response\.start header 1: name 'x-a:b' contains ':'/,
        'the log names the header');

    ($headers, $logged) = $h2c_get->('x-ok');
    is($headers->{':status'}, '200', 'control: a token name gives a 200');
    is($headers->{'x-ok'}, 'v', 'control: the header is sent');
    is([grep { $_->{level} eq 'error' } @$logged], [], 'control: nothing is logged as an error');
};

# A failed send's message ends where the application's send failed. The
# server's own call into the validator is no help to the application.
subtest 'a failed send does not point into the server' => sub {
    my %err;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope" if $scope->{type} ne 'http';
        await $receive->();
        $err{sequence} = do { local $@; eval { await $send->({ type => 'http.response.body', body => 'x' }) }; $@ };
        $err{field}    = do { local $@; eval { await $send->({ type => 'http.response.start', status => 200, trailers => 2 }) }; $@ };
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    };
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0,
        log_level => 'info', logger => sub { }, access_log => undef);
    $loop->add($server);
    $server->listen->get;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port,
        Proto => 'tcp', Timeout => 2) or die "Cannot connect: $!";
    print $sock "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    $sock->blocking(0);
    my $eof = 0;
    my $deadline = time + 3;
    while (!$eof && time < $deadline) {
        $loop->loop_once(0.05);
        my $n = sysread($sock, my $buf, 4096);
        $eof = 1 if defined $n && !$n;
    }
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);

    is($err{sequence}, "cannot send 'http.response.body' before http.response.start\n",
        'a sequence error is the message alone');
    is($err{field}, "http.response.start 'trailers' must be 0 or 1\n", 'a field error is the message alone');
};

done_testing;
