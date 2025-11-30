package PAGI::Server;
use strict;
use warnings;
use experimental 'signatures';
use parent 'IO::Async::Notifier';

our $VERSION = '0.001';

=head1 NAME

PAGI::Server - PAGI Reference Server Implementation

=head1 SYNOPSIS

    use IO::Async::Loop;
    use PAGI::Server;

    my $loop = IO::Async::Loop->new;

    my $server = PAGI::Server->new(
        app  => \&my_pagi_app,
        host => '127.0.0.1',
        port => 5000,
    );

    $loop->add($server);
    $server->listen->get;  # Start accepting connections

=head1 DESCRIPTION

PAGI::Server is a reference implementation of a PAGI-compliant HTTP server.
It supports HTTP/1.1, WebSocket, and Server-Sent Events (SSE) as defined
in the PAGI specification.

This is NOT a production server - it prioritizes spec compliance and code
clarity over performance optimization. It serves as the canonical reference
for how PAGI servers should behave.

=head1 CONSTRUCTOR

=head2 new

    my $server = PAGI::Server->new(%options);

Creates a new PAGI::Server instance. Options:

=over 4

=item app => \&coderef (required)

The PAGI application coderef with signature: async sub ($scope, $receive, $send)

=item host => $host

Bind address. Default: '127.0.0.1'

=item port => $port

Bind port. Default: 5000

=item ssl => \%config

Optional TLS configuration with keys: cert_file, key_file, ca_file, verify_client

=item extensions => \%extensions

Extensions to advertise (e.g., { fullflush => {} })

=item on_error => \&callback

Error callback receiving ($error)

=item access_log => $filehandle

Access log filehandle. Default: STDERR

=back

=head1 METHODS

=head2 listen

    my $future = $server->listen;

Starts listening for connections. Returns a Future that completes when
the server is ready to accept connections.

=head2 shutdown

    my $future = $server->shutdown;

Initiates graceful shutdown. Returns a Future that completes when
shutdown is complete.

=head2 port

    my $port = $server->port;

Returns the bound port number. Useful when port => 0 is used.

=head2 is_running

    my $bool = $server->is_running;

Returns true if the server is accepting connections.

=cut

sub new ($class, %args) {
    my $self = $class->SUPER::new(%args);
    # TODO: Implement in Step 1
    return $self;
}

sub listen ($self) {
    # TODO: Implement in Step 1
}

sub shutdown ($self) {
    # TODO: Implement in Step 6
}

sub port ($self) {
    # TODO: Implement in Step 1
}

sub is_running ($self) {
    # TODO: Implement in Step 1
}

1;

__END__

=head1 SEE ALSO

L<PAGI::Server::Connection>, L<PAGI::Server::Protocol::HTTP1>

=head1 AUTHOR

John Napiorkowski E<lt>jjnapiork@cpan.orgE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
