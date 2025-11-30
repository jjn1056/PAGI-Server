package PAGI::Server::Scope;
use strict;
use warnings;
use experimental 'signatures';

our $VERSION = '0.001';

=head1 NAME

PAGI::Server::Scope - Scope hashref factory

=head1 SYNOPSIS

    use PAGI::Server::Scope;

    my $scope = PAGI::Server::Scope->build_http_scope(
        request    => $request,
        client     => [$host, $port],
        server     => [$host, $port],
        extensions => \%extensions,
        state      => \%state,
    );

=head1 DESCRIPTION

PAGI::Server::Scope provides factory methods for building scope hashrefs
that conform to the PAGI specification.

=cut

sub build_http_scope ($class, %args) {
    # TODO: Implement in Step 1
    return {
        type         => 'http',
        pagi         => { version => '0.1', spec_version => '0.1' },
        http_version => '1.1',
        method       => $args{request}{method},
        scheme       => $args{scheme} // 'http',
        path         => $args{request}{path},
        raw_path     => $args{request}{raw_path},
        query_string => $args{request}{query_string} // '',
        root_path    => '',
        headers      => $args{request}{headers} // [],
        client       => $args{client},
        server       => $args{server},
        state        => $args{state} // {},
        extensions   => $args{extensions} // {},
    };
}

sub build_websocket_scope ($class, %args) {
    # TODO: Implement in Step 4
    return {
        type         => 'websocket',
        pagi         => { version => '0.1', spec_version => '0.1' },
        # ... additional fields
    };
}

sub build_sse_scope ($class, %args) {
    # TODO: Implement in Step 5
    return {
        type => 'sse',
        # ... copy from http scope
    };
}

sub build_lifespan_scope ($class, %args) {
    # TODO: Implement in Step 6
    return {
        type  => 'lifespan',
        pagi  => { version => '0.1', spec_version => '0.1' },
        state => $args{state} // {},
    };
}

1;

__END__

=head1 SEE ALSO

L<PAGI::Server::Connection>

=head1 AUTHOR

John Napiorkowski E<lt>jjnapiork@cpan.orgE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
