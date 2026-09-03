use strict;
use warnings;
use Test2::V0;

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::ConnectionState;

# The server's most valuable diagnostics -- application errors, protocol
# anomalies, TLS failures -- reached STDERR directly, so log_level did not
# govern them and a replaced sink never saw them.

sub collector {
    my $events = shift;
    return sub { push @$events, $_[0] };
}

subtest 'the server names the emitter, and a caller may say otherwise' => sub {
    my @events;
    my $server = PAGI::Server->new(app => sub { }, logger => collector(\@events));

    $server->_log(warn => 'from the core');
    is($events[0]{category}, 'PAGI::Server', 'defaults to the server itself');

    $server->_log(warn => 'from elsewhere', 'PAGI::Server::Connection');
    is($events[1]{category}, 'PAGI::Server::Connection',
        'so a sink can filter connection noise from lifecycle events');
};

subtest 'a connection logs through the server that owns it' => sub {
    my @events;
    my $server = PAGI::Server->new(app => sub { }, logger => collector(\@events));
    my $conn = bless { server => $server }, 'PAGI::Server::Connection';

    $conn->_log(error => 'PAGI application error: boom');

    is(scalar @events, 1, 'the event reached the server sink');
    is($events[0]{level},    'error',                        'at the level asked for');
    is($events[0]{message},  'PAGI application error: boom', 'with the message unchanged');
    is($events[0]{category}, 'PAGI::Server::Connection',     'attributed to the connection');
};

subtest 'a connection without a server still says something' => sub {
    my $conn = bless { }, 'PAGI::Server::Connection';

    my @warned;
    local $SIG{__WARN__} = sub { push @warned, $_[0] };
    $conn->_log(error => 'PAGI connection error: boom');

    is(\@warned, ["PAGI connection error: boom\n"],
        'it falls back to STDERR rather than vanishing');
};

subtest 'connection state logs through the server it was handed' => sub {
    my @events;
    my $server = PAGI::Server->new(app => sub { }, logger => collector(\@events));
    my $state = PAGI::Server::ConnectionState->new(server => $server);

    $state->_log(error => 'on_disconnect callback error: boom');

    is(scalar @events, 1, 'reached the sink without reaching through a weak reference');
    is($events[0]{category}, 'PAGI::Server::ConnectionState', 'and says which class it was');
};

subtest 'connection state without a server falls back too' => sub {
    my $state = PAGI::Server::ConnectionState->new;

    my @warned;
    local $SIG{__WARN__} = sub { push @warned, $_[0] };
    $state->_log(error => 'on_complete callback error: boom');

    is(\@warned, ["on_complete callback error: boom\n"], 'still reported');
};

subtest 'the threshold now governs these diagnostics' => sub {
    my @events;
    my $server = PAGI::Server->new(
        app       => sub { },
        log_level => 'error',
        logger    => collector(\@events),
    );
    my $conn = bless { server => $server }, 'PAGI::Server::Connection';

    $conn->_log(warn  => 'connection-specific header stripped');
    is(scalar @events, 0, 'a warn-level connection message answers to log_level');

    $conn->_log(error => 'PAGI application error: boom');
    is(scalar @events, 1, 'an error still gets through');
};

subtest 'a migrated message carries exactly one newline' => sub {
    my $server = PAGI::Server->new(app => sub { }, log_level => 'info');
    my $conn = bless { server => $server }, 'PAGI::Server::Connection';

    my @warned;
    local $SIG{__WARN__} = sub { push @warned, $_[0] };
    $conn->_log(warn => 'PAGI: connection-specific header stripped');

    # The sites being migrated all ended their strings with \n, and the default
    # sink adds one. Leaving both gives a blank line between every diagnostic.
    is(\@warned, ["PAGI: connection-specific header stripped\n"],
        'no doubled newline');
};

done_testing;
