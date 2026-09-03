#!/usr/bin/env perl
use strict;
use warnings;
use File::Basename qw(dirname);
use File::Spec;
use Log::Dispatch;
use PAGI::Server;

# Sending the server's diagnostics somewhere other than STDERR.
#
# The logger option is a coderef, so it cannot be set from the pagi-server
# command line -- this is a runner script instead. Run it directly:
#
#     perl examples/13-custom-logging/run.pl
#
# then in another terminal:
#
#     curl localhost:5000/          # fine
#     curl localhost:5000/boom      # the app throws
#     curl localhost:5000/silent    # the app never starts a response
#
# Watch the terminal and tail examples/13-custom-logging/server.log.

# Absolute, because do() no longer searches '.' and this script is meant to be
# runnable from anywhere.
my $dir = File::Spec->rel2abs(dirname(__FILE__));

my $log = Log::Dispatch->new(
    outputs => [
        # Everything on the terminal while developing...
        ['Screen', min_level => 'debug', newline => 1],
        # ...but only the problems in the file you would keep.
        ['File',   min_level => 'warning', newline => 1,
                   filename  => "$dir/server.log", mode => 'append'],
    ],
);

# Log::Dispatch names its levels after syslog and has no 'fatal'. Handing it
# one is not an error -- level_is_valid('fatal') is false, so the message is
# discarded with no output, no exception and no warning. Translating here is
# exactly why the sink is a coderef and not a logger object: an object with
# duck-typed level methods could not be adapted without a wrapper class.
my %LEVEL = (fatal => 'critical');

my $server = PAGI::Server->new(
    app       => do "$dir/app.pl",
    host      => '127.0.0.1',
    port      => 5000,

    # The operator's threshold. Nothing below it reaches the sink at all.
    log_level => 'debug',

    logger => sub {
        my ($event) = @_;   # { level, message, category }

        $log->log(
            level   => $LEVEL{ $event->{level} } // $event->{level},
            # category names the emitting class -- PAGI::Server for lifecycle
            # events, PAGI::Server::Connection for per-request diagnostics --
            # so a real deployment can route or filter on it.
            message => sprintf('[%s] %s', $event->{category}, $event->{message}),
        );
    },
);

$server->run;
