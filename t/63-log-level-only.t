use strict;
use warnings;
use Test2::V0;

use PAGI::Server;

# log_level is the only threshold. `quiet` was a second control over the same
# axis that overrode the first, which made --log-level debug --workers N a dead
# letter: workers were spawned quiet, so nothing below error escaped the
# processes actually serving traffic.

my $app = sub { };

sub server_with { PAGI::Server->new(app => $app, @_) }

# Capturing warns is safe here: no await spans the local, so Future::AsyncAwait's
# savestack limitation does not apply.
sub emitted {
    my ($server, @calls) = @_;
    my @lines;
    local $SIG{__WARN__} = sub { push @lines, $_[0] };
    $server->_log(@$_) for @calls;
    return \@lines;
}

subtest 'log_level filters, and nothing overrides it' => sub {
    my $debug = server_with(log_level => 'debug');
    is(scalar @{ emitted($debug, ['debug', 'd'], ['info', 'i'], ['error', 'e']) }, 3,
        'debug level passes everything');

    my $error = server_with(log_level => 'error');
    my $lines = emitted($error, ['debug', 'd'], ['info', 'i'], ['warn', 'w'], ['error', 'e']);
    is(scalar @$lines, 1, 'error level passes only errors');
    like($lines->[0], qr/\be\b/, 'and it is the error that survived');
};

subtest 'quiet is accepted but is only sugar for log_level error' => sub {
    my $quiet = server_with(quiet => 1);
    my $lines = emitted($quiet, ['info', 'i'], ['warn', 'w'], ['error', 'e']);
    is(scalar @$lines, 1, 'quiet => 1 suppresses below error');

    # The point of the change: quiet must no longer WIN over a more verbose
    # log_level. Previously this combination emitted only the error.
    my $both = server_with(quiet => 1, log_level => 'debug');
    is(scalar @{ emitted($both, ['debug', 'd'], ['info', 'i'], ['error', 'e']) }, 3,
        'an explicit log_level beats the deprecated quiet flag');
};

subtest 'worker messages identify themselves' => sub {
    my $worker = server_with(log_level => 'debug');
    $worker->{is_worker}  = 1;
    $worker->{worker_num} = 2;

    my $lines = emitted($worker, ['info', 'serving']);
    like($lines->[0], qr/\AWorker 2 \(\Q$$\E\): serving/,
        'a worker prefixes its number and pid');

    my $master = server_with(log_level => 'debug');
    is(emitted($master, ['info', 'serving'])->[0], "serving\n",
        'the master does not, so an unprefixed line means master');
};

subtest 'the prefix is applied once, not on top of a hand-written one' => sub {
    my $worker = server_with(log_level => 'debug');
    $worker->{is_worker}  = 1;
    $worker->{worker_num} = 1;

    my $line = emitted($worker, ['info', 'reached max_requests (1), shutting down'])->[0];
    my $count = () = $line =~ /Worker /g;
    is($count, 1, 'exactly one Worker prefix on the line');
};

done_testing;
