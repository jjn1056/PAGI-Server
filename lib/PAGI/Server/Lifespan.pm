package PAGI::Server::Lifespan;
use strict;
use warnings;
use experimental 'signatures';

our $VERSION = '0.001';

=head1 NAME

PAGI::Server::Lifespan - Lifespan scope management

=head1 SYNOPSIS

    use PAGI::Server::Lifespan;

    my $lifespan = PAGI::Server::Lifespan->new(app => $app);
    my $state = await $lifespan->startup;
    # ... server runs ...
    await $lifespan->shutdown;

=head1 DESCRIPTION

PAGI::Server::Lifespan manages the lifespan scope, handling startup
and shutdown events and maintaining shared state.

=cut

sub new ($class, %args) {
    my $self = bless {
        app   => $args{app},
        state => {},
    }, $class;
    return $self;
}

sub startup ($self) {
    # TODO: Implement in Step 6
}

sub shutdown ($self) {
    # TODO: Implement in Step 6
}

sub state ($self) {
    return $self->{state};
}

1;

__END__

=head1 SEE ALSO

L<PAGI::Server>

=head1 AUTHOR

John Napiorkowski E<lt>jjnapiork@cpan.orgE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
