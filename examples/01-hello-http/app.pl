use strict;
use warnings;
use Future::AsyncAwait;

# Return anonymous coderef directly (avoids "Subroutine redefined" warnings
# when file is loaded multiple times via do)
my $app = async sub  {
        my ($scope, $receive, $send) = @_;
    # A scope this app does not serve -- lifespan, most often. Returning is
    # the clean decline; dying is also legal but makes an ordinary startup
    # look like a failure in the server's log.
    return if $scope->{type} ne 'http';

    await $send->({
        type    => 'http.response.start',
        status  => 200,
        headers => [ [ 'content-type', 'text/plain' ] ],
    });

  #my $timestamp = scalar localtime;
    await $send->({
        type  => 'http.response.body',
        body  => "Hello from PAGI",  # bytes; encode explicitly if needed
        more  => 0,
    });
};

$app;  # Return coderef when loaded via do
