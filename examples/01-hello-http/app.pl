use strict;
use warnings;
use Future::AsyncAwait;
use experimental 'signatures';

# Return anonymous coderef directly (avoids "Subroutine redefined" warnings
# when file is loaded multiple times via do)
my $app = async sub ($scope, $receive, $send) {
    die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'http';

    await $send->({
        type    => 'http.response.start',
        status  => 200,
        headers => [ [ 'content-type', 'text/plain' ] ],
    });

    my $timestamp = scalar localtime;
    await $send->({
        type  => 'http.response.body',
        body  => "Hello from PAGI at $timestamp",  # bytes; encode explicitly if needed
        more  => 0,
    });
};

$app;  # Return coderef when loaded via do
