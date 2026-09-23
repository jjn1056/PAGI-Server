use strict;
use warnings;
use Future::AsyncAwait;

my $completed = 0; # Worker-local; never log in the measured path.
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Expected HTTP scope" unless $scope->{type} eq 'http';
    if (($scope->{query_string} // '') =~ /(?:^|&)observe=1(?:&|$)/) {
        $scope->{'pagi.connection'}->on_complete(sub { ++$completed });
    }
    await $send->({type => 'http.response.start', status => 200,
        headers => [['content-type', 'text/plain']]});
    await $send->({type => 'http.response.body', body => 'Hello from PAGI', more => 0});
};
$app;
