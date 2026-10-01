use strict;
use warnings;
use Test2::V0;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server::Protocol::HTTP1;

# RFC 9112 3.2: origin-form; absolute-form reduced to its path and query
# (400 when its authority differs from Host); asterisk-form for OPTIONS.
# Anything else is 400, so path and raw_path always begin with "/".

my $proto = PAGI::Server::Protocol::HTTP1->new;
sub parse {
    my ($line, $host) = @_;
    my $request = "$line HTTP/1.1\r\n" . (defined $host ? "Host: $host\r\n" : '') . "\r\n";
    my ($parsed) = $proto->parse_request(\$request);
    return $parsed;
}
sub target { my $p = parse(@_); return $p->{error} ? $p->{error} : [@$p{qw(path raw_path query_string)}] }

is(target('GET /a%20b?x=1', 'example.com'), ['/a b', '/a%20b', 'x=1'], 'origin-form unchanged');
is(target('GET http://example.com/a?b=1', 'example.com'), ['/a', '/a', 'b=1'], 'absolute-form reduced');
is(target('GET http://EXAMPLE.com:80', 'example.com'), ['/', '/', ''], 'empty path, case and default port');
is(target('GET https://example.com:443/s', 'example.com'), ['/s', '/s', ''], 'https default port');
is(target('GET http://evil.example/a', 'example.com'), 400, 'authority differs from Host');
is(target('GET http://example.com:8080/a', 'example.com'), 400, 'a non-default port is part of the authority');
is(target('GET ftp://example.com/a', 'example.com'), 400, 'another scheme');
is(target('GET a/b', 'example.com'), 400, 'not origin-form');
is(target('OPTIONS *', 'example.com'), ['*', '*', ''], 'asterisk-form with OPTIONS');
is(target('GET *', 'example.com'), 400, 'asterisk-form with another method');

done_testing;
