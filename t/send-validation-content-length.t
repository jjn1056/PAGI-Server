use strict;
use warnings;
use Test2::V0;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server::EventValidator;

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

done_testing;
