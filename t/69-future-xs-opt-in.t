use strict;
use warnings;
use Test2::V0;
use FindBin;
use IPC::Open3;
use Symbol qw(gensym);

# Future::XS is opt-in: PAGI_FUTURE_XS=1, or pagi-server --future-xs. Future
# itself uses XS whenever it is installed unless PERL_FUTURE_NO_XS is set when
# Future compiles, so without the opt-in PAGI::Server sets it -- and
# pagi-server and PAGI::Server::Runner load PAGI::Server before anything that
# loads Future, so the default holds on every path they own.

use lib "$FindBin::Bin/lib";
use PAGITest::FutureXS qw(old_future_xs);

# The checks that need the opt-in to work skip without a Future::XS 0.16 or
# later: with none installed there is nothing to opt into, and an older one is
# refused (see the subtest that pins that). The refusals use stand-ins, so they
# run either way.
my $no_xs = !eval { require Future::XS; 1 } ? 'Future::XS not installed; nothing to opt into'
          : old_future_xs()                 ? 'needs Future::XS 0.16 or later (RT #181389)'
          :                                   '';

my $lib = "$FindBin::Bin/../lib";
my $bin = "$FindBin::Bin/../bin/pagi-server";

# Run a child perl with a clean Future-related environment; returns
# (stdout, stderr).
sub child {
    my (%args) = @_;
    local %ENV = %ENV;
    delete @ENV{qw(PERL_FUTURE_NO_XS PAGI_FUTURE_XS)};
    $ENV{$_} = $args{env}{$_} for keys %{ $args{env} || {} };
    my $err = gensym;
    my $pid = open3(my $in, my $out, $err, $^X, "-I$lib", @{ $args{args} });
    close $in;
    local $/;
    my $stdout = <$out> // '';
    my $stderr = <$err> // '';
    waitpid($pid, 0);
    return ($stdout, $stderr);
}

my $report = 'require Future; print Future->isa("Future::XS") ? "XS" : "PP"';

# Runs bin/pagi-server with the given arguments and, as the process ends,
# prints which Future it has (loading it then if nothing did), or "died" when
# pagi-server did not exit cleanly: Future loads with XS after a failed
# startup too, so only a clean exit says the flag worked.
sub pagi_server_future {
    my (@argv) = @_;
    my ($out) = child(args => ['-e', 'my $bin = shift; END { if ($?) { print "died" } else { ' . $report . ' } } do $bin; die $@ if $@',
        $bin, @argv, '--version']);
    return $out =~ /(XS|PP|died)\z/ ? $1 : "unknown: $out";
}

subtest 'by default Future is pure-perl, silently' => sub {
    my ($out, $err) = child(args => ['-e', "use PAGI::Server; $report"]);
    is($out, 'PP', 'Future is pure-perl');
    is($err, '', 'and nothing is said about it');
};

subtest 'PAGI_FUTURE_XS=1 opts in' => sub {
    skip_all $no_xs if $no_xs;
    my ($out, $err) = child(env => { PAGI_FUTURE_XS => 1 },
                            args => ['-e', "use PAGI::Server; $report"]);
    is($out, 'XS', 'Future is XS');
    is($err, '', 'silently');
};

subtest 'asking for Future::XS when it is not installed stops startup' => sub {
    my $hide = 'BEGIN { unshift @INC, sub { die "Can\x27t locate Future/XS.pm in \@INC\n" if $_[1] eq "Future/XS.pm"; return } }';
    my ($out, $err) = child(env => { PAGI_FUTURE_XS => 1 },
                            args => ['-e', "$hide use PAGI::Server; print 'started'"]);
    is($out, '', 'it does not start');
    like($err, qr/PAGI_FUTURE_XS=1 set but Future::XS is not installed/, 'and says why');
    like($err, qr/cpanm Future::XS/, 'and how to fix it');
};

subtest 'asking for Future::XS older than 0.16 stops startup' => sub {
    # A stand-in Future::XS 0.15: the version check runs before Future loads,
    # so it needs nothing but its version.
    my $old = 'BEGIN { unshift @INC, sub { return unless $_[1] eq "Future/XS.pm"; '
        . 'my $src = "package Future::XS; our \\$VERSION = q(0.15); 1;"; open my $fh, "<", \\$src; return $fh } }';
    my ($out, $err) = child(env => { PAGI_FUTURE_XS => 1 },
                            args => ['-e', "$old use PAGI::Server; print 'started'"]);
    is($out, '', 'it does not start');
    like($err, qr/PAGI_FUTURE_XS=1 needs Future::XS 0\.16 or later \(installed: 0\.15\)/, 'and says why');
    like($err, qr/cpanm Future::XS/, 'and how to fix it');
};

subtest 'pagi-server: pure-perl by default, XS with --future-xs' => sub {
    is(pagi_server_future(), 'PP', 'no flag: pure-perl');
    SKIP: {
        skip $no_xs, 1 if $no_xs;
        is(pagi_server_future('--future-xs'), 'XS', '--future-xs: XS');
    }
};

subtest 'PAGI::Server::Runner keeps the default although it loads Future::IO first' => sub {
    # The runner binds Future::IO (which loads Future) before it loads the
    # server; the default must already be in force by then.
    my ($out) = child(args => ['-e', 'use PAGI::Server::Runner; PAGI::Server::Runner->new->_configure_future_io; ' . $report]);
    is($out, 'PP', 'Future is pure-perl');
};

done_testing;
