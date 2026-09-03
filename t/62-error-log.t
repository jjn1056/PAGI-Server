use strict;
use warnings;
use Test2::V0;
use File::Temp qw(tempdir);
use POSIX ();

use PAGI::Server::Runner;

# Daemonizing discards every diagnostic in the ecosystem: _daemonize reopens
# STDERR on /dev/null, and there is no destination to name instead. The access
# log already solves this -- its filehandle is opened before the fork and
# survives -- so the error log follows the same shape.

my $tmp = tempdir(CLEANUP => 1);

subtest 'CLI option parsing - error log' => sub {
    my $runner = PAGI::Server::Runner->new;
    $runner->parse_options('--error-log', "$tmp/err.log");

    is($runner->{error_log}, "$tmp/err.log", '--error-log option parsed');
};

subtest 'error log filehandle is opened eagerly, before any fork' => sub {
    my $runner = PAGI::Server::Runner->new(error_log => "$tmp/eager.log");

    $runner->_open_error_log;

    ok($runner->{_error_log_fh}, 'error log filehandle stored on the runner');
    ok(-e "$tmp/eager.log", 'error log file created at config time');
};

# The redirect runs inside the daemon grandchild, so exercise it in a forked
# child: the parent must keep its own STDERR intact for the harness.
sub redirect_in_child {
    my (%args) = @_;

    my $pid = fork();
    die "cannot fork: $!" unless defined $pid;

    if (!$pid) {
        my $runner = PAGI::Server::Runner->new(
            $args{error_log} ? (error_log => $args{error_log}) : ()
        );
        $runner->_open_error_log if $args{error_log};
        $runner->_redirect_std_handles;
        print STDERR "diagnostic from daemon\n";
        POSIX::_exit(0);
    }

    waitpid($pid, 0);
    return $? >> 8;
}

subtest 'daemonize redirect sends STDERR to the error log when configured' => sub {
    my $path = "$tmp/daemon.log";

    my $status = redirect_in_child(error_log => $path);
    is($status, 0, 'child exited cleanly');

    open my $fh, '<', $path or die "cannot read $path: $!";
    my $contents = do { local $/; <$fh> };
    close $fh;

    like($contents, qr/diagnostic from daemon/,
        'STDERR written after redirect lands in the error log');
};

subtest 'daemonize redirect still discards STDERR when no error log is set' => sub {
    my $status = redirect_in_child();
    is($status, 0, 'child exited cleanly with no error log configured');
    # No destination named, so the pre-existing /dev/null behaviour stands;
    # there is nothing to assert beyond the child surviving the redirect.
};

# run() must open the log before anything can diagnose through STDERR.
# prepare_app and _configure_future_io both warn, and both run well before the
# server is built -- opening the log alongside the fork would lose them.
{
    package OrderProbe;
    use parent -norequire, 'PAGI::Server::Runner';

    our $log_was_open;

    sub prepare_app {
        my ($self) = @_;
        $log_was_open = $self->{_error_log_fh} ? 1 : 0;
        warn "diagnostic from prepare_app\n";
        die "stop before the server runs\n";
    }
}

subtest 'the error log is open before prepare_app can diagnose' => sub {
    my $path = "$tmp/early.log";

    my $pid = fork();
    die "cannot fork: $!" unless defined $pid;

    if (!$pid) {
        # Forked so the probe's die and STDERR redirect cannot touch the harness.
        eval { OrderProbe->run('--error-log', $path, 'unused-app.pl') };
        POSIX::_exit($OrderProbe::log_was_open ? 0 : 1);
    }

    waitpid($pid, 0);
    is($? >> 8, 0, 'error log filehandle existed by the time prepare_app ran');

    open my $fh, '<', $path or die "cannot read $path: $!";
    my $contents = do { local $/; <$fh> };
    close $fh;

    like($contents, qr/diagnostic from prepare_app/,
        'a warning from prepare_app reaches the error log');
};

done_testing;
