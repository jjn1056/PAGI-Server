use strict;
use warnings;
use Test2::V0;

use PAGI::Server::Runner;

# Production output is read by machines, development output by people. The
# runner knows the mode; the server only knows what it is told.

sub server_for {
    my (%args) = @_;
    my $runner = PAGI::Server::Runner->new(
        env            => $args{env},
        server_options => $args{server_options} // {},
    );
    $runner->{app} = sub { };
    return $runner->load_server;
}

is(server_for(env => 'production')->{log_format}, 'json', 'production defaults to json');
is(server_for(env => 'development')->{log_format}, 'text', 'development defaults to text');
is(server_for(env => 'production', server_options => { log_format => 'text' })->{log_format},
    'text', 'an explicit --log-format wins in production');
is(server_for(env => 'development', server_options => { log_format => 'json' })->{log_format},
    'json', 'and in development');

done_testing;
