use strict;
use warnings;
use Future::AsyncAwait;
use experimental 'signatures';
use IO::Async::Loop;

# Drain the request body - keeps reading http.request events until
# we get one with more => 0 (end of body) or a non-request event.
# The await yields control to the event loop - this is NOT blocking.
async sub drain_request ($receive) {
    while (1) {
        my $event = await $receive->();
        # Exit if not a request event (e.g., http.disconnect)
        last if $event->{type} ne 'http.request';
        # Exit if this is the final body chunk
        last unless $event->{more};
    }
}

async sub app ($scope, $receive, $send) {
    die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'http';

    # Get the event loop for non-blocking delays
    my $loop = IO::Async::Loop->new;

    # First, drain any request body
    await drain_request($receive);

    # Start the response with chunked encoding and trailers
    await $send->({
        type     => 'http.response.start',
        status   => 200,
        headers  => [ [ 'content-type', 'text/plain' ] ],
        trailers => 1,
    });

    my @chunks = (
        "Chunk 1\n",
        "Chunk 2\n",
        "Chunk 3\n",
    );

    # Start a task that waits for client disconnect.
    # This returns a Future that completes when http.disconnect is received.
    my $disconnect_future = $receive->();

    for my $i (0 .. $#chunks) {
        my $body = $chunks[$i];
        my $more = ($i < $#chunks) ? 1 : 0;

        # Check if client disconnected before sending
        if ($disconnect_future->is_ready) {
            warn "Client disconnected before all chunks sent\n";
            return;
        }

        await $send->({ type => 'http.response.body', body => $body, more => $more });

        # Sleep 1 second between chunks (non-blocking - yields to event loop)
        if ($more && $loop) {
            await $loop->delay_future(after => 1);
        }
    }

    # Check disconnect before sending trailers
    if ($disconnect_future->is_ready) {
        warn "Client disconnected before trailers\n";
        return;
    }

    await $send->({
        type    => 'http.response.trailers',
        headers => [ [ 'x-stream-complete', '1' ] ],
    });
}

\&app;
