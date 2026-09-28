use strict;
use warnings;
use Test2::V0;
use Scalar::Util qw(weaken);
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

# A named async receive must not retain its connection in either its lexical
# variables or @_. Exercise the public-to-the-app receive closure while keeping
# the Future alive, so a strong reference hidden in the suspended frame fails.
for my $body_complete (0, 1) {
    subtest 'pending receive does not own connection: ' .
        ($body_complete ? 'after body' : 'waiting for body') => sub {
        my $conn = PAGI::Server::Connection->new(
            protocol => PAGI::Server::Protocol::HTTP1->new);
        my $receive = $conn->_create_receive({ content_length => 4 });
        if ($body_complete) {
            $conn->{buffer} = 'body';
            is($receive->()->get, { type => 'http.request', body => 'body', more => 0 },
                'body consumed before waiting for disconnect');
        }
        my $future = $receive->();
        ok(!$future->is_ready, 'receive is suspended');
        weaken(my $weak_conn = $conn);
        undef $conn;
        ok(!defined $weak_conn, 'suspended receive does not keep connection alive');
        $future->cancel;
        ok($future->is_cancelled, 'suspended receive can be cancelled');
        is($receive->()->get, { type => 'http.disconnect' },
            'retained receive closure reports a released connection');
    };
}

subtest 'cancelling a receive releases its awaited Future' => sub {
    my $conn = PAGI::Server::Connection->new;
    my $receive = $conn->_create_receive({ content_length => 4 });
    my $future = $receive->();
    my $pending = $conn->{receive_pending};
    ok(!$pending->is_ready, 'waiting for body data');
    $future->cancel;
    ok($future->is_cancelled, 'caller cancellation settles receive');
    ok($pending->is_cancelled, 'cancellation reaches the awaited Future');
};

subtest 'repeated receives preserve body position and wake for new data' => sub {
    my $conn = PAGI::Server::Connection->new;
    my $receive = $conn->_create_receive({ content_length => 65540 });
    $conn->{buffer} = 'x' x 65536;
    is($receive->()->get, { type => 'http.request', body => 'x' x 65536, more => 1 },
        'first chunk leaves more body');
    my $future = $receive->();
    ok(!$future->is_ready, 'next call waits for the remaining body');
    $conn->{buffer} = 'tail';
    my $pending = $conn->{receive_pending};
    $conn->{receive_pending} = undef;
    $pending->done;
    is($future->get, { type => 'http.request', body => 'tail', more => 0 },
        'resumed call reads only remaining bytes');
    my $after = $receive->();
    ok(!$after->is_ready, 'completed body waits for disconnect on next call');
    $after->cancel;
};

done_testing;
