use strict;
use warnings;
use Future::AsyncAwait;
async sub {
    my ($scope, $receive, $send) = @_;
    die "Expected HTTP scope" unless $scope->{type} eq 'http';
    my $body = 'Hello from PAGI';
    if ($scope->{path} ne '/small') {
        my $bytes = 0;
        while (1) {
            my $event = await $receive->();
            return if $event->{type} eq 'http.disconnect';
            die "Expected HTTP body" unless $event->{type} eq 'http.request';
            $bytes += length($event->{body} // '');
            last unless $event->{more};
        }
        $body = "$bytes\n";
    }
    await $send->({type=>'http.response.start', status=>200, headers=>[['content-type','text/plain']]});
    await $send->({type=>'http.response.body', body=>$body, more=>0});
};
