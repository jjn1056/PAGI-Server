use strict;
use warnings;
use Test2::V0;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Test::TempDir::Tiny;
use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# max_sends_per_turn: how many sends that leave their scope open a worker runs
# before it returns to the event loop. This file covers the option itself --
# its default, what it accepts and refuses, configure, and the pagi-server
# flag. What the server does with it is t/99-send-budget-h1.t and
# t/http2/60-send-budget-h2.t.

my $app     = async sub { return };
my $REFUSAL = qr/\AInvalid max_sends_per_turn '.*' - must be 0 \(off\) or a positive count/;

sub server { PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1, @_) }

subtest 'default is 384' => sub {
    is(server()->{max_sends_per_turn}, 384, 'a server built without the option');
};

subtest '0 is accepted' => sub {
    is(server(max_sends_per_turn => 0)->{max_sends_per_turn}, 0, '0 turns it off');
    is(server(max_sends_per_turn => 1)->{max_sends_per_turn}, 1, 'and so is 1');
};

subtest 'a negative, fractional or non-numeric value is refused' => sub {
    for my $value (-1, 1.5, 'many') {
        like(dies { server(max_sends_per_turn => $value) }, $REFUSAL, "new refuses '$value'");
        my $server = server();
        like(dies { $server->configure(max_sends_per_turn => $value) }, $REFUSAL, "configure refuses '$value'");
        is($server->{max_sends_per_turn}, 384, 'and leaves the value as it was');
    }
};

subtest 'configure changes it' => sub {
    my $server = server();
    $server->configure(max_sends_per_turn => 64);
    is($server->{max_sends_per_turn}, 64, 'to another count');
    $server->configure(max_sends_per_turn => 0);
    is($server->{max_sends_per_turn}, 0, 'and to 0');
};

# bin/pagi-server -> server_options -> the server's constructor.
# PAGITest::FakeServer (t/lib) stands in for PAGI::Server and prints the
# options it was given.
my $pagi_server = "$FindBin::Bin/../bin/pagi-server";
my $tlib        = "$FindBin::Bin/lib";
my $app_file    = tempdir() . '/app.pl';
open my $app_fh, '>', $app_file or die "Cannot write $app_file: $!";
print {$app_fh} "sub {}\n";
close $app_fh;

sub run_cli {
    my (@flags) = @_;
    local $ENV{PAGI_ENV} = 'production';   # deterministic mode, no Lint wrap
    return `$^X -Ilib -I$tlib $pagi_server @flags -s PAGITest::FakeServer $app_file 2>&1`;
}

subtest '--max-sends-per-turn reaches the server' => sub {
    like(run_cli('--max-sends-per-turn', '128'), qr/^FAKESERVER max_sends_per_turn=128$/m,
        'the flag passes through as max_sends_per_turn');
    like(run_cli('--max-sends-per-turn', '0'), qr/^FAKESERVER max_sends_per_turn=0$/m,
        '0 passes through as 0, not as no option');
    like(run_cli(), qr/^FAKESERVER max_sends_per_turn=unset$/m,
        'without the flag the server gets no option and uses its default');
};

subtest 'bin/pagi-server documents the flag' => sub {
    open my $fh, '<', $pagi_server or die "Cannot open $pagi_server: $!";
    my $content = do { local $/; <$fh> };
    close $fh;
    like($content, qr/^=item B<--max-sends-per-turn> NUM$/m, 'an =item for --max-sends-per-turn');
};

done_testing;
