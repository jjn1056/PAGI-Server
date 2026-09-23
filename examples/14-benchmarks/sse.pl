use strict;
use warnings;
use Future::AsyncAwait;
use Future::IO;

# Fixed data, no serialization work in the measured loop.
my $data = 'x' x 128;
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Expected SSE scope (send Accept: text/event-stream)" unless $scope->{type} eq 'sse';
    my $paced = ($scope->{query_string} // '') =~ /(?:^|&)paced=1(?:&|$)/;
    await $send->({type => 'sse.start', status => 200,
        headers => [['content-type', 'text/event-stream']]});
    for my $i (1..100) {
        await Future::IO->sleep(0.01) if $paced && $i > 1;
        await $send->({type => 'sse.send', id => "$i", event => 'tick', data => $data});
    }
    await $send->({type => 'sse.close'});
};
$app;
