package PAGI::Server::TransportState;

use strict;
use warnings;

our $VERSION = '0.002000';

use Scalar::Util qw(weaken);

=head1 NAME

PAGI::Server::TransportState - Outbound flow-control introspection for a connection

=head1 SYNOPSIS

    my $transport = $scope->{'pagi.transport'};

    # Bytes queued for the client but not yet written to the network
    my $pending = $transport->buffered_amount;

    # The backpressure band (sends block at high, resume at low)
    my $ceiling = $transport->high_water_mark;
    my $floor   = $transport->low_water_mark;

=head1 DESCRIPTION

PAGI::Server::TransportState is the object placed in the C<pagi.transport> scope
key. It gives an application a synchronous, read-only view of B<outbound flow
control> -- how much data the server has queued for the client but not yet
written to the network -- so it can conflate, coalesce, shed load, or disconnect
a slow client instead of only blocking until the buffer drains. It is the
server-side analogue of the browser WebSocket API's C<bufferedAmount>.

All reads are live: the object holds a weak reference to the parent connection
and reports its current state at call time. See the "Transport Flow Control"
section in L<PAGI::Spec::Www> for the full specification.

=head1 METHODS

=head2 new

    my $transport = PAGI::Server::TransportState->new(connection => $connection);

Creates a transport-state handle. The C<connection> argument is the parent
connection, held weakly to avoid a reference cycle.

=cut

sub new {
    my ($class, %args) = @_;

    my $self = bless {
        _connection => $args{connection},
    }, $class;

    # Weaken to avoid a cycle: Connection -> scope -> TransportState -> Connection
    weaken($self->{_connection}) if $self->{_connection};

    return $self;
}

=head2 buffered_amount

    my $pending = $transport->buffered_amount;

Returns the number of bytes queued for the client but not yet written to the
network, as an integer; C<0> when the send buffer is fully drained (or once the
underlying connection has gone away). A synchronous, non-blocking,
non-destructive read.

=cut

sub buffered_amount {
    my $self = shift;
    my $conn = $self->{_connection};
    return 0 unless $conn;
    return $conn->_get_write_buffer_size;
}

=head2 high_water_mark

    my $ceiling = $transport->high_water_mark;

Returns the buffered-byte threshold at or above which the server applies
backpressure (a C<$send> that would exceed it blocks until the buffer drains),
or C<undef> if unavailable. Applications use it to threshold relative to the
ceiling rather than hard-coding a byte count.

=cut

sub high_water_mark {
    my $self = shift;
    my $conn = $self->{_connection};
    return undef unless $conn;
    return $conn->{write_high_watermark};
}

=head2 low_water_mark

    my $floor = $transport->low_water_mark;

Returns the buffered-byte threshold the buffer must fall back to before the
server releases backpressure (the drain point), or C<undef> if unavailable.

=cut

sub low_water_mark {
    my $self = shift;
    my $conn = $self->{_connection};
    return undef unless $conn;
    return $conn->{write_low_watermark};
}

1;

__END__

=head1 SEE ALSO

L<PAGI::Spec::Www> - "Transport Flow Control" specification

L<PAGI::Server::ConnectionState> - HTTP disconnect-state introspection (sibling handle)

L<PAGI::Server::Connection> - Per-connection state machine (internal)

=cut
