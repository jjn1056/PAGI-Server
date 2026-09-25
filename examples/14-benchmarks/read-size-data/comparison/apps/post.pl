use strict;
use warnings;
use Future::AsyncAwait;

my $completed = 0;
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Expected HTTP scope" unless $scope->{type} eq 'http';
    if (($scope->{query_string} // '') =~ /(?:^|&)observe=1(?:&|$)/) {
        $scope->{'pagi.connection'}->on_complete(sub { ++$completed });
    }
    my $bytes = 0;
    while (1) {
        my $event = await $receive->();
        return if $event->{type} eq 'http.disconnect';
        die "Expected request body" unless $event->{type} eq 'http.request';
        $bytes += length($event->{body} // '');
        last unless $event->{more};
    }
    await $send->({type => 'http.response.start', status => 200,
        headers => [['content-type', 'text/plain']]});
    await $send->({type => 'http.response.body', body => "$bytes\n", more => 0});
};
$app;
