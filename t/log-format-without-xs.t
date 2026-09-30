use strict;
use warnings;

# Hide Cpanel::JSON::XS before anything loads it, as if it were not installed.
BEGIN {
    unshift @INC, sub {
        die "Cpanel::JSON::XS hidden for this test\n" if $_[1] eq 'Cpanel/JSON/XS.pm';
        return;
    };
}

use Test2::V0;

use PAGI::Server;
use PAGI::Server::Runner;

# JSON lines are written for every request, so they need the XS encoder; the
# pure-Perl one costs about thirty times a clf line. Without it, asking for
# JSON is an error, and production's JSON default quietly becomes text.

my $app = sub { };

ok(!eval { require Cpanel::JSON::XS; 1 }, 'Cpanel::JSON::XS is hidden');

is(dies { PAGI::Server->new(app => $app, log_format => 'json') },
    "log_format 'json' requires Cpanel::JSON::XS, which is not installed\n",
    'an explicit json log_format is refused, naming the module');

is(dies { PAGI::Server->new(app => $app, access_log_format => 'json') },
    "access_log_format 'json' requires Cpanel::JSON::XS, which is not installed\n",
    'so is the json access-log preset');

is(PAGI::Server->new(app => $app)->{log_format}, 'text', 'text needs nothing');

subtest 'production falls back to text and says so' => sub {
    my $runner = PAGI::Server::Runner->new(env => 'production');
    $runner->{app} = $app;
    my $server = $runner->load_server;

    is($server->{log_format}, 'text', 'the production default becomes text');
    is({ map { @$_ } @{ $server->{startup_notes} } }->{log_format},
        'text (install Cpanel::JSON::XS for JSON lines)',
        'and the startup banner names the module to install');
};

subtest 'an explicit --log-format json still refuses' => sub {
    my $runner = PAGI::Server::Runner->new(
        env            => 'production',
        server_options => { log_format => 'json' },
    );
    $runner->{app} = $app;
    like(dies { $runner->load_server }, qr/\Alog_format 'json' requires Cpanel::JSON::XS/,
        'the operator asked for JSON, so falling back would hide the problem');
};

done_testing;
