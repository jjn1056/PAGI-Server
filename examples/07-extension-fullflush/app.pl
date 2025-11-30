use strict;
use warnings;
use Future::AsyncAwait;
use experimental 'signatures';

async sub base_app ($scope, $receive, $send) {
    die "Unsupported scope" if $scope->{type} ne 'http';

    # Drain request body if present
    while (1) {
        my $event = await $receive->();
        last if $event->{type} ne 'http.request';
        last unless $event->{more};
    }

    await $send->({
        type    => 'http.response.start',
        status  => 200,
        headers => [ [ 'content-type', 'text/plain' ] ],
    });

    await $send->({ type => 'http.response.body', body => "Body sent", more => 0 });
}

sub fullflush_middleware ($app) {
    return async sub ($scope, $receive, $send) {
        my $supports_fullflush = exists $scope->{extensions}{fullflush};
        await $app->($scope, $receive, $send);
        if ($supports_fullflush && $scope->{type} eq 'http') {
            await $send->({ type => 'http.fullflush' });
        }
    };
}

my $wrapped = fullflush_middleware(\&base_app);
return $wrapped unless caller;
