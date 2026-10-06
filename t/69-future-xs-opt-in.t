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

plan skip_all => 'Future::XS not installed; nothing to opt into'
    unless eval { require Future::XS; 1 };

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
# prints which Future it has (loading it then if nothing did).
sub pagi_server_future {
    my (@argv) = @_;
    my ($out) = child(args => ['-e', 'my $bin = shift; END { ' . $report . ' } do $bin; die $@ if $@',
        $bin, @argv, '--version']);
    return $out =~ /(XS|PP)\z/ ? $1 : "unknown: $out";
}

subtest 'by default Future is pure-perl, silently' => sub {
    my ($out, $err) = child(args => ['-e', "use PAGI::Server; $report"]);
    is($out, 'PP', 'Future is pure-perl');
    is($err, '', 'and nothing is said about it');
};

subtest 'PAGI_FUTURE_XS=1 opts in' => sub {
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

subtest 'pagi-server: pure-perl by default, XS with --future-xs' => sub {
    is(pagi_server_future(), 'PP', 'no flag: pure-perl');
    is(pagi_server_future('--future-xs'), 'XS', '--future-xs: XS');
};

subtest 'PAGI::Server::Runner keeps the default although it loads Future::IO first' => sub {
    # The runner binds Future::IO (which loads Future) before it loads the
    # server; the default must already be in force by then.
    my ($out) = child(args => ['-e', 'use PAGI::Server::Runner; PAGI::Server::Runner->new->_configure_future_io; ' . $report]);
    is($out, 'PP', 'Future is pure-perl');
};

done_testing;
