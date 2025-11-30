package PAGI::Server::Connection;
use strict;
use warnings;
use experimental 'signatures';

our $VERSION = '0.001';

=head1 NAME

PAGI::Server::Connection - Per-connection state machine

=head1 SYNOPSIS

    # Internal use by PAGI::Server
    my $conn = PAGI::Server::Connection->new(
        stream   => $stream,
        app      => $app,
        protocol => $protocol,
    );

=head1 DESCRIPTION

PAGI::Server::Connection manages the state machine for a single client
connection. It handles:

=over 4

=item * Request parsing via Protocol::HTTP1

=item * Scope creation for the application

=item * Event queue management for $receive and $send

=item * Protocol upgrades (WebSocket)

=item * Connection lifecycle and cleanup

=back

=cut

sub new ($class, %args) {
    my $self = bless {
        stream   => $args{stream},
        app      => $args{app},
        protocol => $args{protocol},
        # TODO: Add state tracking
    }, $class;
    return $self;
}

sub handle_request ($self) {
    # TODO: Implement in Step 1
}

sub create_scope ($self, $request) {
    # TODO: Implement in Step 1
}

sub create_receive ($self) {
    # TODO: Implement in Step 1
}

sub create_send ($self) {
    # TODO: Implement in Step 1
}

1;

__END__

=head1 SEE ALSO

L<PAGI::Server>, L<PAGI::Server::Protocol::HTTP1>

=head1 AUTHOR

John Napiorkowski E<lt>jjnapiork@cpan.orgE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
