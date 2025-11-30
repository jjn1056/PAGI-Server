package PAGI::Server;
use strict;
use warnings;
use experimental 'signatures';
use parent 'IO::Async::Notifier';
use IO::Async::Listener;
use IO::Async::Stream;
use Future;
use Future::AsyncAwait;
use Scalar::Util qw(weaken);

use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

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

sub _init ($self, $params) {
    $self->{app}        = delete $params->{app} or die "app is required";
    $self->{host}       = delete $params->{host} // '127.0.0.1';
    $self->{port}       = delete $params->{port} // 5000;
    $self->{ssl}        = delete $params->{ssl};
    $self->{extensions} = delete $params->{extensions} // {};
    $self->{on_error}   = delete $params->{on_error} // sub { warn @_ };
    $self->{access_log} = delete $params->{access_log} // \*STDERR;
    $self->{quiet}      = delete $params->{quiet} // 0;

    $self->{running}     = 0;
    $self->{bound_port}  = undef;
    $self->{listener}    = undef;
    $self->{connections} = [];
    $self->{protocol}    = PAGI::Server::Protocol::HTTP1->new;
    $self->{state}       = {};  # Shared state from lifespan

    $self->SUPER::_init($params);
}

sub configure ($self, %params) {
    if (exists $params{app}) {
        $self->{app} = delete $params{app};
    }
    if (exists $params{host}) {
        $self->{host} = delete $params{host};
    }
    if (exists $params{port}) {
        $self->{port} = delete $params{port};
    }
    if (exists $params{ssl}) {
        $self->{ssl} = delete $params{ssl};
    }
    if (exists $params{extensions}) {
        $self->{extensions} = delete $params{extensions};
    }
    if (exists $params{on_error}) {
        $self->{on_error} = delete $params{on_error};
    }
    if (exists $params{access_log}) {
        $self->{access_log} = delete $params{access_log};
    }
    if (exists $params{quiet}) {
        $self->{quiet} = delete $params{quiet};
    }

    $self->SUPER::configure(%params);
}

async sub listen ($self) {
    return if $self->{running};

    weaken(my $weak_self = $self);

    my $listener = IO::Async::Listener->new(
        on_stream => sub ($listener, $stream) {
            return unless $weak_self;
            $weak_self->_on_connection($stream);
        },
    );

    $self->add_child($listener);
    $self->{listener} = $listener;

    # Start listening
    my $listen_future = $listener->listen(
        addr => {
            family   => 'inet',
            socktype => 'stream',
            ip       => $self->{host},
            port     => $self->{port},
        },
    );

    await $listen_future;

    # Store the actual bound port from the listener's read handle
    my $socket = $listener->read_handle;
    $self->{bound_port} = $socket->sockport if $socket && $socket->can('sockport');
    $self->{running} = 1;

    unless ($self->{quiet}) {
        my $log = $self->{access_log};
        print $log "PAGI Server listening on http://$self->{host}:$self->{bound_port}/\n";
    }

    return $self;
}

sub _on_connection ($self, $stream) {
    weaken(my $weak_self = $self);

    my $conn = PAGI::Server::Connection->new(
        stream     => $stream,
        app        => $self->{app},
        protocol   => $self->{protocol},
        server     => $self,
        extensions => $self->{extensions},
        state      => $self->{state},
    );

    # Track the connection
    push @{$self->{connections}}, $conn;

    # Configure stream with callbacks BEFORE adding to loop
    $conn->start;

    # Add stream to the loop so it can read/write
    $self->add_child($stream);
}

async sub shutdown ($self) {
    return unless $self->{running};
    $self->{running} = 0;

    # Stop accepting new connections
    if ($self->{listener}) {
        $self->remove_child($self->{listener});
        $self->{listener} = undef;
    }

    # TODO: Wait for existing connections to complete
    # TODO: Emit lifespan.shutdown events (Step 6)

    return $self;
}

sub port ($self) {
    return $self->{bound_port} // $self->{port};
}

sub is_running ($self) {
    return $self->{running} ? 1 : 0;
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
