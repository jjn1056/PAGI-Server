use strict;
use warnings;
use Future::AsyncAwait;

my $completed = 0;
my $chunk = 'x' x 1024;
my $whole = $chunk x 64;
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Expected HTTP scope" unless $scope->{type} eq 'http';
    my $query = $scope->{query_string} // '';
    if ($query =~ /(?:^|&)observe=1(?:&|$)/) {
        $scope->{'pagi.connection'}->on_complete(sub { ++$completed });
    }
    await $send->({type => 'http.response.start', status => 200,
        headers => [['content-type', 'application/octet-stream']]});
    if ($query =~ /(?:^|&)single=1(?:&|$)/) {
        await $send->({type => 'http.response.body', body => $whole, more => 0});
    } else {
        for my $i (1..64) {
            await $send->({type => 'http.response.body', body => $chunk, more => ($i < 64 ? 1 : 0)});
        }
    }
};
$app;
