package PAGITest::RunnerApp;

use strict;
use warnings;
use Future::AsyncAwait;

# A module application for PAGI::Server::Runner tests, loadable the way
# `pagi-server Some::Module key=value` loads one: Runner calls new with the
# key=value pairs, then to_app. It answers every HTTP request with its
# `greeting` argument, so a test can see the arguments reached the app.

sub new {
    my ($class, %args) = @_;
    return bless { greeting => $args{greeting} // 'hello' }, $class;
}

sub to_app {
    my ($self) = @_;
    my $greeting = $self->{greeting};
    return async sub {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            while (1) {
                my $event = await $receive->();
                if ($event->{type} eq 'lifespan.startup') {
                    await $send->({ type => 'lifespan.startup.complete' });
                }
                elsif ($event->{type} eq 'lifespan.shutdown') {
                    await $send->({ type => 'lifespan.shutdown.complete' });
                    return;
                }
            }
        }
        return unless $scope->{type} eq 'http';
        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [ [ 'content-type', 'text/plain' ] ],
        });
        await $send->({ type => 'http.response.body', body => $greeting });
    };
}

1;
