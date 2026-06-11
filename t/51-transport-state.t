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
use Future;

require PAGI::Server::TransportState;

# Minimal duck-typed Connection: TransportState reads _get_write_buffer_size()
# and the watermark fields, and arms drain detection via _wait_for_drain().
{
    package MockConn;
    sub new {
        my ($class, %a) = @_;
        bless {
            write_high_watermark => $a{high} // 65536,
            write_low_watermark  => $a{low}  // 16384,
            _bufsize             => $a{buf}  // 0,
            _drain_futures       => [],
        }, $class;
    }
    sub _get_write_buffer_size { $_[0]{_bufsize} }
    sub _wait_for_drain {
        my $self = shift;
        my $f = Future->new;
        push @{$self->{_drain_futures}}, $f;
        return $f;
    }
    # Test helper: simulate the buffer draining below the low mark.
    sub _drain {
        my $self = shift;
        $self->{_bufsize} = 0;
        $_->done for splice @{$self->{_drain_futures}};
    }
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

# =============================================================================
# Backpressure callbacks (on_high_water / on_drain) - hysteresis
# =============================================================================

subtest 'on_high_water fires once on crossing; on_drain after high->low' => sub {
    my $conn = MockConn->new(high => 100, low => 20, buf => 0);
    my $t = PAGI::Server::TransportState->new(connection => $conn);

    my ($high, $drain) = (0, 0);
    $t->on_high_water(sub { $high++ });
    $t->on_drain(sub { $drain++ });

    # Below the mark: nothing fires.
    $t->_check_watermarks;
    is([$high, $drain], [0, 0], 'nothing while below high mark');

    # Cross above the high mark.
    $conn->{_bufsize} = 150;
    $t->_check_watermarks;
    is([$high, $drain], [1, 0], 'on_high_water fired once on crossing');

    # Still above: edge-triggered, must not re-fire.
    $t->_check_watermarks;
    is([$high, $drain], [1, 0], 'on_high_water does not re-fire while above');

    # Drain below the low mark.
    $conn->_drain;
    is([$high, $drain], [1, 1], 'on_drain fired once after high->low');

    # Re-arms: crossing high again fires on_high_water again.
    $conn->{_bufsize} = 150;
    $t->_check_watermarks;
    is([$high, $drain], [2, 1], 'cycle re-arms');
};

subtest 'on_high_water registered while already above fires immediately' => sub {
    my $conn = MockConn->new(high => 100, low => 20, buf => 150);
    my $t = PAGI::Server::TransportState->new(connection => $conn);

    my $fired = 0;
    $t->on_high_water(sub { $fired++ });
    is($fired, 1, 'late registrant fires immediately when already above');
};

subtest 'on_drain does not fire on registration while below low' => sub {
    my $conn = MockConn->new(high => 100, low => 20, buf => 0);
    my $t = PAGI::Server::TransportState->new(connection => $conn);

    my $fired = 0;
    $t->on_drain(sub { $fired++ });
    is($fired, 0, 'on_drain only fires on an actual high->low transition');
};

subtest 'multiple callbacks fire in registration order' => sub {
    my $conn = MockConn->new(high => 100, low => 20, buf => 0);
    my $t = PAGI::Server::TransportState->new(connection => $conn);

    my @order;
    $t->on_high_water(sub { push @order, 'a' });
    $t->on_high_water(sub { push @order, 'b' });

    $conn->{_bufsize} = 150;
    $t->_check_watermarks;
    is(\@order, ['a', 'b'], 'high-water callbacks in registration order');
};

subtest 'a callback error does not break the others' => sub {
    my $conn = MockConn->new(high => 100, low => 20, buf => 0);
    my $t = PAGI::Server::TransportState->new(connection => $conn);

    my $second = 0;
    $t->on_high_water(sub { die "boom" });
    $t->on_high_water(sub { $second++ });

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, @_ };

    $conn->{_bufsize} = 150;
    $t->_check_watermarks;

    is($second, 1, 'second callback still ran');
    like($warnings[0], qr/callback error/, 'warning emitted');
};

done_testing;
