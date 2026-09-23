use strict;
use warnings;
use Future::AsyncAwait;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);
my ($requests,$completed,$peak_lag,$max_delay)=(0,0,0,0);
my $app=async sub {
    my ($scope,$receive,$send)=@_;
    die 'Expected HTTP scope' unless $scope->{type} eq 'http';
    ++$requests;
    my $started=clock_gettime(CLOCK_MONOTONIC);
    $scope->{'pagi.connection'}->on_complete(sub {
        ++$completed;
        my $delay=clock_gettime(CLOCK_MONOTONIC)-$started;
        $max_delay=$delay if $delay>$max_delay;
    });
    my $lag=$requests-$completed;
    $peak_lag=$lag if $lag>$peak_lag;
    print STDERR "PROGRESS requests=$requests completed=$completed lag=$lag\n" if $requests%1000==0;
    await $send->({type=>'http.response.start',status=>200,headers=>[['content-type','text/plain']]});
    await $send->({type=>'http.response.body',body=>'Hello from PAGI',more=>0});
};
END { print STDERR "COMPLETION requests=$requests completed=$completed peak_lag=$peak_lag max_delay_s=$max_delay\n"; }
$app;
