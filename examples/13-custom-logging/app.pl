use strict;
use warnings;
use Future::AsyncAwait;

# A deliberately misbehaving app, so the server has something to diagnose.
#
#   /        a normal response
#   /boom    throws, so the server reports an application error
#   /silent  returns without starting a response, which is a protocol error

async sub app {
    my ($scope, $receive, $send) = @_;

    return unless ($scope->{type} // '') eq 'http';

    my $path = $scope->{path} // '/';

    die "the database is on fire\n" if $path eq '/boom';
    return                          if $path eq '/silent';

    await $send->({
        type    => 'http.response.start',
        status  => 200,
        headers => [['content-type', 'text/plain']],
    });
    await $send->({
        type => 'http.response.body',
        body => "try /boom and /silent, then look at your logs\n",
        more => 0,
    });
    return;
}

\&app;
