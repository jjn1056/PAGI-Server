use strict;
use warnings;
use Test2::V0;

use PAGI::Server::Runner;

# Starting a server said nothing about what it had loaded, and reported its mode
# only in development -- so a production box that had silently booted in
# development, wrapping every request in Lint, looked exactly like one that had
# not.

sub summary_from {
    my ($runner, %args) = @_;
    my @lines;
    local $SIG{__WARN__} = sub { push @lines, $_[0] };
    $runner->{app_spec} = $args{app_spec} if exists $args{app_spec};
    $runner->_log_startup_summary($args{middleware});
    chomp @lines;
    return $lines[0];
}

subtest 'the summary names the mode and what is being served' => sub {
    my $runner = PAGI::Server::Runner->new(env => 'production');
    $runner->{_mode_source} = '--env';

    is(summary_from($runner, app_spec => 'MyApp::Handler'),
        'production mode (--env), serving MyApp::Handler',
        'production says so, which it previously never did');
};

subtest 'it says how the mode was decided' => sub {
    # mode() resolves --env, then PAGI_ENV, then whether STDIN is a terminal.
    # The last one means anything without a terminal -- systemd, docker, cron,
    # a backgrounded shell -- silently becomes production.
    my %cases = (
        '--env'    => 'production mode (--env), serving ./app.pl',
        'PAGI_ENV' => 'production mode (PAGI_ENV), serving ./app.pl',
        'no tty'   => 'production mode (no tty), serving ./app.pl',
    );

    for my $source (sort keys %cases) {
        my $runner = PAGI::Server::Runner->new(env => 'production');
        $runner->{_mode_source} = $source;
        is(summary_from($runner, app_spec => './app.pl'), $cases{$source},
            "reports $source");
    }
};

subtest 'development reports what it added to the stack' => sub {
    my $runner = PAGI::Server::Runner->new(env => 'development');
    $runner->{_mode_source} = 'tty';

    is(summary_from($runner, app_spec => './app.pl', middleware => 'Lint enabled'),
        'development mode (tty), serving ./app.pl, Lint enabled',
        'so an added wrapper is never a surprise');

    is(summary_from($runner, app_spec => './app.pl',
                    middleware => 'default middleware disabled'),
        'development mode (tty), serving ./app.pl, default middleware disabled',
        'and neither is an absent one');
};

subtest 'the mode source survives run() exporting PAGI_ENV' => sub {
    # run() sets $ENV{PAGI_ENV} from the resolved mode before prepare_app is
    # reached. Detecting the source at that point would report PAGI_ENV every
    # time, whatever actually decided it.
    local $ENV{PAGI_ENV};
    delete $ENV{PAGI_ENV};

    my $runner = PAGI::Server::Runner->new(env => 'development');
    my (undef, $source) = $runner->_resolve_mode;
    is($source, '--env', 'the flag is credited before anything is exported');

    $ENV{PAGI_ENV} = 'development';
    my (undef, $still) = $runner->_resolve_mode;
    is($still, '--env', 'and still is once the variable exists');
};

subtest 'run() notes the mode source before it exports PAGI_ENV' => sub {
    # The ordering is the whole point and is invisible: run() assigns
    # $ENV{PAGI_ENV} from the resolved mode, so a source detected any later
    # reads back as PAGI_ENV no matter what actually decided it. Exercised
    # through run() rather than by calling _resolve_mode directly, because
    # calling it directly cannot catch a capture placed too late.
    {
        package ModeProbe;
        use parent -norequire, 'PAGI::Server::Runner';
        our $source;
        sub prepare_app {
            my ($self) = @_;
            $source = $self->{_mode_source};
            die "stop before the server runs\n";
        }
    }

    local $ENV{PAGI_ENV};
    delete $ENV{PAGI_ENV};

    # No --env and no PAGI_ENV, so only the terminal check can decide.
    eval { ModeProbe->run('--port', '0', 'unused-app.pl') };

    like($ModeProbe::source, qr/\A(?:tty|no tty)\z/,
        'the terminal check is credited, not the variable run() just set');
    isnt($ModeProbe::source, 'PAGI_ENV',
        'which is what a capture placed after the export would have reported');
};

subtest 'mode() itself is unchanged by the refactor' => sub {
    local $ENV{PAGI_ENV};
    delete $ENV{PAGI_ENV};

    is(PAGI::Server::Runner->new(env => 'production')->mode, 'production',
        '--env wins');

    $ENV{PAGI_ENV} = 'staging';
    is(PAGI::Server::Runner->new->mode, 'staging', 'then the environment');

    delete $ENV{PAGI_ENV};
    my $inferred = PAGI::Server::Runner->new->mode;
    like($inferred, qr/\A(?:development|production)\z/,
        'then the terminal check, which yields one or the other');
};

done_testing;
