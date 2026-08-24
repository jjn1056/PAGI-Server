package PAGI::Server::AppNormalizer;

use strict;
use warnings;

our $VERSION = '0.002007';

use Scalar::Util qw(blessed reftype);

=head1 NAME

PAGI::Server::AppNormalizer - Normalizes PAGI 0.4 application providers to a coderef

=head1 SYNOPSIS

    use PAGI::Server::AppNormalizer ();

    my $app = PAGI::Server::AppNormalizer::normalize_app($candidate, 'app');

=head1 DESCRIPTION

B<Note:> This is a PAGI::Server internal module, not part of the public
PAGI::Server API, and is not indexed on CPAN.

PAGI 0.4 allows an application source (an app file, a module constructor, an
C<-e> expression, direct C<PAGI::Server> construction, or C<configure(app =>
...)>) to hand PAGI::Server either a native PAGI coderef or an instantiated
application-provider object exposing a C<to_app> method. This module is the
single normalization point every one of those sources calls through, so a
provider is normalized to its coderef exactly once regardless of which
source produced it. A blessed coderef is returned as-is (native-app
precedence); a package-name string is never treated as a provider.

=head1 FUNCTIONS

=head2 normalize_app($candidate, $source)

    my $app = PAGI::Server::AppNormalizer::normalize_app($candidate, $source);

Returns C<$candidate> unchanged if it is already a coderef (including a
blessed one). Otherwise C<$candidate> must be a blessed object providing
C<to_app>; that method is called once and its coderef return value is
returned. C<$source> is a human-readable label (e.g. C<'App file'>, C<'-e
code'>) used only to identify the origin in die messages; it defaults to
C<'Application'>.

Dies if C<$candidate> is neither a coderef nor a blessed object, if the
object has no C<to_app> method, if C<to_app> raises, or if C<to_app>'s
return value is not itself a coderef.

=head1 SEE ALSO

L<PAGI::Server>, L<PAGI::Server::Runner>

=cut

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
