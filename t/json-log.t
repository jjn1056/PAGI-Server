use strict;
use warnings;
use Test2::V0;
use JSON::MaybeXS ();

use PAGI::Server::JSONLog;

my $decoder = JSON::MaybeXS->new(utf8 => 1);

subtest 'timestamp is RFC 3339 UTC with milliseconds' => sub {
    is(PAGI::Server::JSONLog::timestamp(0), '1970-01-01T00:00:00.000Z', 'the epoch');
    is(PAGI::Server::JSONLog::timestamp(1790000000.1234),
        '2026-09-21T14:13:20.123Z', 'milliseconds truncated, not rounded');
    like(PAGI::Server::JSONLog::timestamp(),
        qr/\A\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z\z/, 'defaults to now');
};

subtest 'object keeps key order and value types' => sub {
    my $line = PAGI::Server::JSONLog::object(
        zeta => 'z', alpha => 1, mid => undef, ratio => 0.5,
    );
    is($line, '{"zeta":"z","alpha":1,"mid":null,"ratio":0.5}',
        'keys in the order given; numbers, null and strings encoded as such');

    my $nested = PAGI::Server::JSONLog::object(
        message => 'm', notes => [serving => './app.pl', loop => 'Poll'],
    );
    is($nested, '{"message":"m","notes":{"serving":"./app.pl","loop":"Poll"}}',
        'an arrayref of pairs becomes an ordered nested object');
};

subtest 'text turns any input into characters without losing bytes' => sub {
    is(PAGI::Server::JSONLog::text(undef), undef, 'undef stays undef');
    is(PAGI::Server::JSONLog::text("caf\xc3\xa9"), "caf\x{e9}",
        'valid UTF-8 bytes are decoded');
    is(PAGI::Server::JSONLog::text("caf\x{e9} \x{263a}"), "caf\x{e9} \x{263a}",
        'a character string is left alone');
    is(PAGI::Server::JSONLog::text("\xff\xfe/x"), "\x{ff}\x{fe}/x",
        'invalid UTF-8 keeps one code point per byte');
};

subtest 'every line is valid JSON on one line' => sub {
    for my $message ("plain", "two\nlines", "caf\xc3\xa9", "\xff\xfe", "wide \x{263a}", qq{"quoted" \\ back}) {
        my $line = PAGI::Server::JSONLog::object(
            message => PAGI::Server::JSONLog::text($message));
        unlike($line, qr/\n/, 'no raw newline inside the line');
        my $decoded = $decoder->decode($line);
        is($decoded->{message}, PAGI::Server::JSONLog::text($message),
            'round-trips through a JSON parser');
    }
};

done_testing;
