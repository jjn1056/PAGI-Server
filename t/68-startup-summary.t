use strict;
use warnings;
use Test2::V0;

use PAGI::Server::Runner;

# Starting a server said nothing about what it had loaded, and reported its mode
# only in development -- so a production box that had silently booted in
# development, wrapping every request in Lint, looked exactly like one that had
# not.

# The runner no longer prints these; it hands them to the server, which renders
# one aligned block. A fresh runner each time, because the notes accumulate.
sub notes_for {
    my (%args) = @_;
    my $runner = PAGI::Server::Runner->new(env => $args{env});
    $runner->{_mode_source} = $args{source};
    $runner->{app_spec}     = $args{app_spec};
    $runner->_record_startup_summary($args{middleware});
    return { map { @$_ } @{ $runner->{_startup_notes} || [] } };
}

subtest 'the summary names the mode and what is being served' => sub {
    my $note = notes_for(env => 'production', source => '--env',
                         app_spec => 'MyApp::Handler');

    is($note->{serving}, 'MyApp::Handler',  'the application is named');
    is($note->{mode}, 'production (--env)', 'production says so, which it never did');
};

subtest 'it says how the mode was decided' => sub {
    # mode() resolves --env, then PAGI_ENV, then whether STDIN is a terminal.
    # The last one means anything without a terminal -- systemd, docker, cron,
    # a backgrounded shell -- silently becomes production.
    for my $source ('--env', 'PAGI_ENV', 'no tty') {
        my $note = notes_for(env => 'production', source => $source,
                             app_spec => './app.pl');
        is($note->{mode}, "production ($source)", "reports $source");
    }
};

subtest 'development reports what it added to the stack' => sub {
    my $with = notes_for(env => 'development', source => 'tty',
                         app_spec => './app.pl', middleware => 'Lint enabled');
    is($with->{mode}, 'development (tty), Lint enabled',
        'so an added wrapper is never a surprise');

    my $without = notes_for(env => 'development', source => 'tty',
                            app_spec => './app.pl',
                            middleware => 'default middleware disabled');
    is($without->{mode}, 'development (tty), default middleware disabled',
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
