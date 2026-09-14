use strict;
use warnings;
use Test2::V0;
use FindBin;
use IPC::Open3;
use Symbol qw(gensym);

# PAGI::Server keeps Future pure-perl for now: Future::XS 0.15 warns "lost a
# sequence Future" whenever a without_cancel observer is dropped before its
# original resolves, which is what Future->wait_any($work,
# $conn->disconnect_future) does on every race the work wins (reported to the
# Future-XS RT queue). Future reads PERL_FUTURE_NO_XS once, when it is
# compiled, so the server can only prevent XS, never unload it.

plan skip_all => 'Future::XS not installed; nothing to keep out'
    unless eval { require Future::XS; 1 };

my $lib = "$FindBin::Bin/../lib";

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

subtest 'loading PAGI::Server first keeps Future pure-perl' => sub {
    my ($out, $err) = child(args => ['-e', "use PAGI::Server; $report"]);
    is($out, 'PP', 'Future is pure-perl');
    is($err, '', 'silently');
};

subtest 'the old opt-in is refused, not honoured' => sub {
    my ($out, $err) = child(env => { PAGI_FUTURE_XS => 1 },
                            args => ['-e', "use PAGI::Server; $report"]);
    is($out, 'PP', 'Future stays pure-perl');
    like($err, qr/Future::XS support is currently unavailable/, 'and the setting is answered');

    my (undef, $flag_err) = child(args => ["$FindBin::Bin/../bin/pagi-server", '--future-xs', '--version']);
    like($flag_err, qr/--future-xs is currently unavailable/, 'the command-line flag says the same');
};

subtest 'a Future loaded before the server keeps XS, which startup reports' => sub {
    my ($out) = child(args => ['-e', "use Future; use PAGI::Server; $report"]);
    is($out, 'XS', 'the server cannot unload what is already compiled');

    # A real startup, with Future compiled first: the block names it and one
    # warn-level line follows (t/66 pins the wording; this pins that it fires).
    my $start = 'use Future; use PAGI::Server; use IO::Async::Loop;'
        . ' my $loop = IO::Async::Loop->new;'
        . ' my $s = PAGI::Server->new(app => sub {}, host => "127.0.0.1", port => 0);'
        . ' $loop->add($s); $s->listen->get; $s->shutdown->get;';
    my (undef, $err) = child(args => ['-e', $start]);
    like($err, qr/future_xs on \(loaded before PAGI::Server\)/, 'the startup block says on');
    like($err, qr/Future::XS \S+ is in use/, 'and the warning is logged at startup');
};

done_testing;
