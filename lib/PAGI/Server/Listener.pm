package PAGI::Server::Listener;

use strict;
use warnings;

our $VERSION = '0.002006';

use parent 'IO::Async::Listener';

use Errno qw(EAGAIN EWOULDBLOCK);
use IO::Async::Stream;

=encoding utf8

=head1 NAME

PAGI::Server::Listener - accept-batching listener for cleartext connections

=head1 DESCRIPTION

Internal to PAGI::Server; not a public API surface.

L<IO::Async::Listener> accepts one connection per readiness event, which
caps a busy process's accept rate at its event-loop iteration rate — a few
dozen accepts per second under load. A connection storm (mass reconnect
after a deploy, load-balancer failover, a benchmark ramp) then pins the
kernel accept queue full, further SYNs are dropped into retransmit backoff,
and clients are admitted at the loop's crawl: seconds of first-request
latency that no amount of workers fully removes.

This subclass drains up to C<ACCEPT_BATCH> pending connections per
readiness event. The bound keeps one storm from monopolizing a loop
iteration at the expense of established connections' I/O.

It dispatches accepted sockets directly and so bypasses the parent's
pluggable acceptor; it must therefore never be used for listeners whose
acceptor performs the TLS handshake (the L<IO::Async::SSL> listen
extension). PAGI::Server selects it for cleartext listeners only.

=cut

# Per-event accept bound: high enough to drain a storm in a few loop
# iterations, low enough that accepting can't monopolize an iteration.
use constant ACCEPT_BATCH => 64;

sub on_read_ready {
    my $self = shift;

    my $socket = $self->read_handle or return;

    for (1 .. ACCEPT_BATCH) {
        my $accepted = $socket->accept;

        if (!defined $accepted) {
            # Queue drained
            last if $! == EAGAIN || $! == EWOULDBLOCK;
            # Real accept failure (e.g. EMFILE): dispatch like the parent
            $self->maybe_invoke_event(on_accept_error => $socket, $!)
                or $self->invoke_error("accept() failed - $!", accept => $socket, $!);
            last;
        }

        $accepted->blocking(0);
        $self->invoke_event(on_stream => IO::Async::Stream->new(handle => $accepted));
    }
}

1;

__END__

=head1 SEE ALSO

L<IO::Async::Listener>, L<PAGI::Server>

=head1 AUTHOR

John Napiorkowski E<lt>jjnapiork@cpan.orgE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
