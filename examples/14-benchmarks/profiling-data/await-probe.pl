use strict;
use warnings;
use Future;
use Future::AsyncAwait;
my $gate=Future->new;
my $run=(async sub { await $gate; return 42 })->();
$gate->done;
die "bad result" unless $run->get == 42;
print "suspended await completed\n";
