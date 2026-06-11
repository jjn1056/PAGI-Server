#!/usr/bin/env perl

# =============================================================================
# Test: PAGI::Server::TransportState
#
# The server-provided pagi.transport handle for outbound flow-control
# introspection: buffered_amount (bytes queued but not yet on the wire) and the
# high/low water marks. Reads live from the Connection via a weak ref.
# =============================================================================

use strict;
use warnings;
use Test2::V0;

require PAGI::Server::TransportState;

# Minimal duck-typed Connection: TransportState reads _get_write_buffer_size()
# and the watermark fields.
{
    package MockConn;
    sub new {
        my ($class, %a) = @_;
        bless {
            write_high_watermark => $a{high} // 65536,
            write_low_watermark  => $a{low}  // 16384,
            _bufsize             => $a{buf}  // 0,
        }, $class;
    }
    sub _get_write_buffer_size { $_[0]{_bufsize} }
}

subtest 'buffered_amount reflects the connection write buffer (live)' => sub {
    my $conn = MockConn->new(buf => 0);
    my $t = PAGI::Server::TransportState->new(connection => $conn);

    is($t->buffered_amount, 0, 'zero when drained');

    $conn->{_bufsize} = 4096;
    is($t->buffered_amount, 4096, 'reflects queued bytes on a live re-read');
};

subtest 'watermarks expose the backpressure band' => sub {
    my $conn = MockConn->new(high => 65536, low => 16384);
    my $t = PAGI::Server::TransportState->new(connection => $conn);

    is($t->high_water_mark, 65536, 'high_water_mark');
    is($t->low_water_mark,  16384, 'low_water_mark');
};

subtest 'graceful when there is no connection' => sub {
    my $t = PAGI::Server::TransportState->new(connection => undef);

    is($t->buffered_amount, 0,     'buffered_amount is 0 without a connection');
    is($t->high_water_mark, undef, 'high_water_mark undef without a connection');
    is($t->low_water_mark,  undef, 'low_water_mark undef without a connection');
};

subtest 'connection reference is weak (no cycle, graceful after free)' => sub {
    my $t;
    {
        my $conn = MockConn->new(buf => 99);
        $t = PAGI::Server::TransportState->new(connection => $conn);
        is($t->buffered_amount, 99, 'reads while the connection is alive');
    }
    # $conn is now out of scope; the weak ref should have cleared.
    is($t->buffered_amount, 0, 'buffered_amount 0 after the connection is freed');
};

done_testing;
