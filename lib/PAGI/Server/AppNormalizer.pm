package PAGI::Server::AppNormalizer;

use strict;
use warnings;

our $VERSION = '0.002006';

use Scalar::Util qw(blessed reftype);

sub normalize_app {
    my ($candidate, $source) = @_;
    $source //= 'Application';

    return $candidate if (reftype($candidate) // '') eq 'CODE';

    my $type = ref($candidate) || 'non-reference';
    die "$source must be a coderef or instantiated application-provider object, got: $type\n"
        unless blessed($candidate);

    die "$source returned an application-provider object without to_app()\n"
        unless $candidate->can('to_app');

    my $app = eval { $candidate->to_app };
    die "Error normalizing $source through to_app(): $@\n" if $@;

    my $app_type = ref($app) || 'non-reference';
    die "$source to_app() must return a coderef, got: $app_type\n"
        unless (reftype($app) // '') eq 'CODE';

    return $app;
}

1;
