use strict;
use warnings;
use Test2::V0;

use PAGI::Server::Protocol::HTTP1;

# The request head becomes the scope's headers as sent: names lowercased but
# otherwise untouched, in order, duplicates kept apart; a malformed head is
# refused rather than repaired.
my $proto = PAGI::Server::Protocol::HTTP1->new;

sub parse {
    my ($head) = @_;
    my $buffer = $head . "\r\n";
    my ($request, $consumed) = $proto->parse_request(\$buffer);
    return $request;
}
sub head { my (@lines) = @_; return join('', map { "$_\r\n" } @lines) }
sub error_of { my ($r) = @_; return $r && $r->{error} }

subtest 'headers keep their order and their names' => sub {
    my $r = parse(head('GET / HTTP/1.1', 'Host: x', 'X-B: 2', 'X-A: 1', 'X-Forwarded-For: 10.0.0.1', 'X_Forwarded_For: 6.6.6.6'));
    is($r->{headers}, [
        ['host', 'x'], ['x-b', '2'], ['x-a', '1'],
        ['x-forwarded-for', '10.0.0.1'], ['x_forwarded_for', '6.6.6.6'],
    ], 'in order; an underscore name stays its own header');
};

subtest 'a repeated header stays repeated' => sub {
    my $r = parse(head('GET / HTTP/1.1', 'Host: x', 'Accept: a/b', 'Accept: c/d'));
    is([grep { $_->[0] eq 'accept' } @{ $r->{headers} }], [['accept', 'a/b'], ['accept', 'c/d']],
        'two entries');
};

subtest 'whitespace around the value is not part of it' => sub {
    my $r = parse(head('GET / HTTP/1.1', 'Host: x', "X-A: \t one two \t "));
    is([grep { $_->[0] eq 'x-a' } @{ $r->{headers} }], [['x-a', 'one two']], 'trimmed');
};

subtest 'a field name that is not a token is refused' => sub {
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: x', 'X-A : one'))), 400, 'space before the colon');
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: x', "X-A\t: one"))), 400, 'tab before the colon');
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: x', 'X A: one'))), 400, 'space inside the name');
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: x', ': one'))), 400, 'empty name');
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: x', 'no colon here'))), 400, 'no colon');
};

subtest 'a control character in a value is refused' => sub {
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: x', "X-A: a\0b"))), 400, 'NUL');
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: x', "X-A: a\x07b"))), 400, 'BEL');
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: x', "X-A: a\tb"))), undef, 'a tab inside is fine');
};

subtest 'Host' => sub {
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: a', 'Host: b'))), 400, 'two Host headers');
    is(error_of(parse(head('GET / HTTP/1.1'))), 400, 'none on HTTP/1.1');
    is(error_of(parse(head('GET / HTTP/1.0'))), undef, 'none on HTTP/1.0 is fine');
};

subtest 'too many headers is 431' => sub {
    my @many = map { "X-$_: v" } 1 .. 500;
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: x', @many))), 431, '500 headers');
    my @limit = map { "X-$_: v" } 1 .. 99;
    is(error_of(parse(head('GET / HTTP/1.1', 'Host: x', @limit))), undef, '100 headers is the limit');
};

subtest 'an obs-fold continuation joins the previous value with a space' => sub {
    my $r = parse(head('GET / HTTP/1.1', 'Host: x', 'X-A: one', '  two'));
    is([grep { $_->[0] eq 'x-a' } @{ $r->{headers} }], [['x-a', 'one two']], 'unfolded');
    is(error_of(parse(head('GET / HTTP/1.1', ' Host: x'))), 400, 'a fold with nothing before it');
};

subtest 'cookies' => sub {
    my $r = parse(head('GET / HTTP/1.1', 'Host: x', 'Cookie: a=1', 'X-A: 1', 'Cookie: b=2'));
    is($r->{headers}, [['host', 'x'], ['cookie', 'a=1; b=2'], ['x-a', '1']],
        'one cookie header, where the first one was');
};

subtest 'body framing' => sub {
    my $r = parse(head('POST / HTTP/1.1', 'Host: x', 'Content-Length: 12'));
    is([$r->{content_length}, $r->{chunked}], [12, 0], 'Content-Length');
    is(error_of(parse(head('POST / HTTP/1.1', 'Host: x', 'Content-Length: 5', 'Content-Length: 5'))), 400,
        'two Content-Length headers');
    is(error_of(parse(head('POST / HTTP/1.1', 'Host: x', 'Content-Length: +5'))), 400, 'a signed length');
    $r = parse(head('POST / HTTP/1.1', 'Host: x', 'Transfer-Encoding: chunked'));
    is($r->{chunked}, 1, 'chunked');
    is(error_of(parse(head('POST / HTTP/1.1', 'Host: x', 'Transfer-Encoding: chunked', 'Content-Length: 5'))), 400,
        'Transfer-Encoding with Content-Length');
    is(error_of(parse(head('POST / HTTP/1.1', 'Host: x', 'Transfer-Encoding: chunked', 'Transfer-Encoding: gzip'))), 400,
        'chunked not last across two headers');
    $r = parse(head('POST / HTTP/1.1', 'Host: x', 'Expect: 100-continue', 'Content-Length: 1'));
    is($r->{expect_continue}, 1, 'Expect: 100-continue');
};

subtest 'the request line' => sub {
    my $r = parse(head('GET /a%20b?q=1 HTTP/1.1', 'Host: x'));
    is([@$r{qw(method raw_path path query_string http_version)}], ['GET', '/a%20b', '/a b', 'q=1', '1.1'],
        'method, path, query, version');
    is(parse(head('get / HTTP/1.1', 'Host: x'))->{method}, 'get', 'methods are case-sensitive tokens');
    is(error_of(parse(head('HELLO'))), 400, 'not a request line');
    is(error_of(parse(head('GET /'))), 400, 'HTTP/0.9');
    is(error_of(parse(head('GET / HTTP/2.0', 'Host: x'))), 400, 'HTTP/2.0 on HTTP/1');
    is(error_of(parse(head('GET  / HTTP/1.1', 'Host: x'))), 400, 'two spaces');
    is(error_of(parse(head('G(T / HTTP/1.1', 'Host: x'))), 400, 'method not a token');
};

# A head still arriving is held to the same limits as a complete one, and a
# bare LF line ending is refused at once rather than waited on forever.
sub parse_raw {
    my ($buffer, %limits) = @_;
    my ($request, $consumed) = PAGI::Server::Protocol::HTTP1->new(%limits)->parse_request(\$buffer);
    return $request;
}

subtest 'a bare LF line ending is refused' => sub {
    is(error_of(parse_raw("GET / HTTP/1.1\nHost: x\n\n")), 400, 'all bare LF');
    is(error_of(parse_raw("GET / HTTP/1.1\r\nHost: x\n\n")), 400, 'bare LF after a CRLF line');
    is(error_of(parse_raw("GET / HTTP/1.1\nHost: x")), 400, 'before the head is complete');
};

subtest 'an incomplete head within the limits is waited for' => sub {
    is(parse_raw("GET / HTTP/1.1\r\nHost: x\r\nX-A: 1"), undef, 'mid-header');
    is(parse_raw("GET / HTTP/1.1\r"), undef, 'a CR waiting for its LF');
    is(parse_raw("GET / HTTP/1.1\r\nHost: x\r\n\r"), undef, 'the last CR of the head');
};

subtest 'an incomplete head past the limits is refused' => sub {
    is(error_of(parse_raw('GET /' . ('a' x 100), max_request_line_size => 50)), 414,
        'a request line already past max_request_line_size');
    is(error_of(parse_raw("GET / HTTP/1.1\r\nHost: x\r\nX-Big: " . ('a' x 200), max_header_size => 100)), 431,
        'headers already past max_header_size');
    is(parse_raw("GET / HTTP/1.1\r\nHost: x\r\nX-A: " . ('a' x 40), max_header_size => 100), undef,
        'still within max_header_size');
};

done_testing;
