use strict;
use warnings;
use Future::AsyncAwait;
use experimental 'signatures';

async sub watch_sse_disconnect ($receive) {
    while (1) {
        my $event = await $receive->();
        return $event if $event->{type} eq 'sse.disconnect';
    }
}

async sub app ($scope, $receive, $send) {
    die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'sse';

    await $send->({
        type    => 'sse.start',
        status  => 200,
        headers => [ [ 'content-type', 'text/event-stream' ] ],
    });

    my $disconnect = watch_sse_disconnect($receive);
    my @events = (
        { event => 'tick', data => '1' },
        { event => 'tick', data => '2' },
        { event => 'done', data => 'finished' },
    );

    for my $msg (@events) {
        last if $disconnect->is_ready;
        await $send->({ type => 'sse.send', %$msg });
    }

    $disconnect->cancel if $disconnect->can('cancel') && !$disconnect->is_ready;
}

return \&app unless caller;
