use strict;
use warnings;
use Test2::V0;

use PAGI::Server;

# The server's log destination is replaceable. The level machinery already
# existed; only the sink was hardcoded to warn.

my $app = sub { };

sub server_with { PAGI::Server->new(app => $app, @_) }

subtest 'the default sink emits exactly what it always did' => sub {
    my $server = server_with(log_level => 'info');

    my @warned;
    local $SIG{__WARN__} = sub { push @warned, $_[0] };
    $server->_log(info => 'listening on http://127.0.0.1:5000/');

    is(\@warned, ['listening on http://127.0.0.1:5000/' . "\n"],
        'a trailing newline on STDERR, byte for byte');
};

subtest 'a custom sink receives the event as one hashref' => sub {
    my @events;
    my $server = server_with(
        log_level => 'info',
        logger    => sub { push @events, $_[0] },
    );

    my @warned;
    local $SIG{__WARN__} = sub { push @warned, $_[0] };
    $server->_log(warn => 'listener saturated');

    is(scalar @events, 1, 'the sink was called once');
    is($events[0]{level},    'warn',               'level is passed');
    is($events[0]{message},  'listener saturated', 'message is passed without a newline');
    is($events[0]{category}, 'PAGI::Server',       'category names the emitter');
    is(\@warned, [], 'and nothing reached STDERR');
};

subtest 'the threshold is applied before the sink is consulted' => sub {
    my $calls = 0;
    my $server = server_with(
        log_level => 'error',
        logger    => sub { $calls++ },
    );

    $server->_log(debug => 'd');
    $server->_log(info  => 'i');
    $server->_log(warn  => 'w');
    is($calls, 0, 'a filtered message never reaches the sink at all');

    $server->_log(error => 'e');
    is($calls, 1, 'an error does');
};

subtest 'fatal sorts above error but is not a threshold' => sub {
    my @events;
    my $server = server_with(
        log_level => 'error',
        logger    => sub { push @events, $_[0] },
    );

    $server->_log(fatal => 'the loop is gone');
    is(scalar @events, 1, 'fatal passes the strictest threshold');

    # PSGI and Rack both carry five levels, so an adapter can be handed one
    # unchanged. A fatal *threshold* would silence errors, which is useless.
    like(
        dies { server_with(log_level => 'fatal') },
        qr/Invalid log_level/,
        'but fatal is rejected as a log_level',
    );
};

subtest 'a worker identifies itself to the sink too' => sub {
    my @events;
    my $server = server_with(log_level => 'info', logger => sub { push @events, $_[0] });
    $server->{is_worker}  = 1;
    $server->{worker_num} = 3;

    $server->_log(info => 'serving');
    like($events[0]{message}, qr/\AWorker 3 \(\Q$$\E\): serving\z/,
        'the prefix reaches a custom sink, not just STDERR');
};

done_testing;
