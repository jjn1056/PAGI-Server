use strict;
use warnings;
use Test2::V0;

use File::Temp qw(tempdir);
use PAGI::Server::Runner;

# Workers share the --access-log file. A buffered handle flushes each worker's
# 8 KB block at an arbitrary byte, so lines from different workers tear into
# each other -- and a torn JSON line is a lost record. One write per line keeps
# every line whole.

my $dir = tempdir(CLEANUP => 1);

my $runner = PAGI::Server::Runner->new(
    env        => 'production',
    access_log => "$dir/access.log",
);
$runner->{app} = sub { };
my $fh = $runner->load_server->{access_log};

ok($fh, 'the server was given the opened access log');
my $previous = select($fh);
my $autoflush = $|;
select($previous);
ok($autoflush, 'the --access-log handle writes each line as it is printed');

done_testing;
