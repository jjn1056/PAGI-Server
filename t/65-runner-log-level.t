use strict;
use warnings;
use Test2::V0;

use PAGI::Server::Runner;

# Runner's own messages had no threshold: --log-level reaches it only inside
# the server_options hashref, so only -q could silence them. They now answer to
# the same verbosity the operator asked for.

sub emitted {
    my ($runner, @calls) = @_;
    my @lines;
    local $SIG{__WARN__} = sub { push @lines, $_[0] };
    $runner->_log(@$_) for @calls;
    return \@lines;
}

subtest 'default threshold matches the server default' => sub {
    my $runner = PAGI::Server::Runner->new;
    my $lines = emitted($runner, ['debug', 'd'], ['info', 'i'], ['error', 'e']);
    is(scalar @$lines, 2, 'info and error pass, debug does not');
};

subtest '-q is sugar for the error threshold' => sub {
    my $runner = PAGI::Server::Runner->new(quiet => 1);
    my $lines = emitted($runner, ['info', 'i'], ['warn', 'w'], ['error', 'e']);
    is($lines, ["e\n"], 'only the error survives');
};

subtest 'a log level asked for on the command line governs Runner too' => sub {
    my $debug = PAGI::Server::Runner->new(server_options => { log_level => 'debug' });
    is(scalar @{ emitted($debug, ['debug', 'd'], ['info', 'i']) }, 2,
        'debug passes when the operator asked for debug');

    my $error = PAGI::Server::Runner->new(server_options => { log_level => 'error' });
    is(scalar @{ emitted($error, ['info', 'i'], ['warn', 'w']) }, 0,
        'and nothing below error passes when they asked for error');
};

subtest 'an explicit level beats -q, as it does on the server' => sub {
    my $runner = PAGI::Server::Runner->new(
        quiet          => 1,
        server_options => { log_level => 'debug' },
    );
    is(scalar @{ emitted($runner, ['debug', 'd'], ['info', 'i']) }, 2,
        'the threshold wins over the shorthand');
};

subtest 'a server that has no log_level leaves Runner on its default' => sub {
    my $runner = PAGI::Server::Runner->new(server_options => { workers => 4 });
    is(scalar @{ emitted($runner, ['info', 'i']) }, 1,
        'an absent key is not an error -- Runner stays server-agnostic');
};

subtest "Runner's own messages go through it" => sub {
    my $runner = PAGI::Server::Runner->new(server_options => { log_level => 'error' });

    my @lines;
    local $SIG{__WARN__} = sub { push @lines, $_[0] };
    $runner->_parse_app_args('novalue');

    is(\@lines, [], 'the argument warning answers to the threshold');
};

done_testing;
