use strict;
use warnings;
use Future::AsyncAwait;
my $body = 'x' x 65536;
async sub {
    my ($scope, $receive, $send) = @_;
    die "Expected HTTP scope" unless $scope->{type} eq 'http';
    my $out = $scope->{path} eq '/small' ? 'Hello from PAGI' : $body;
    await $send->({type=>'http.response.start', status=>200, headers=>[['content-type','application/octet-stream']]});
    await $send->({type=>'http.response.body', body=>$out, more=>0});
};
