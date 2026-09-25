use strict;
use warnings;
use Future::AsyncAwait;

# Same small body as get.pl, with ten application response fields. This exposes
# per-field processing cost separately from body size and chunk count.
my $headers = [
    ['content-type', 'text/plain'],
    ['cache-control', 'private, no-cache'],
    ['vary', 'accept-encoding'],
    ['etag', '"benchmark-v1"'],
    ['content-language', 'en'],
    ['x-content-type-options', 'nosniff'],
    ['referrer-policy', 'same-origin'],
    ['permissions-policy', 'camera=(), microphone=()'],
    ['link', '</assets/app.css>; rel=preload; as=style'],
    ['x-benchmark-case', 'ten-headers'],
];
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Expected HTTP scope" unless $scope->{type} eq 'http';
    await $send->({type=>'http.response.start', status=>200, headers=>$headers});
    await $send->({type=>'http.response.body', body=>'Hello from PAGI', more=>0});
};
$app;
