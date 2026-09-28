use strict;
use warnings;
use Test2::V0;
use FindBin;
use File::Temp qw(tempfile);

plan skip_all => 'CLI process tests unavailable on Windows' if $^O eq 'MSWin32';

my ($app_fh, $app_file) = tempfile(SUFFIX => '.pl', UNLINK => 1);
print {$app_fh} "sub {}\n";
close $app_fh;

sub run_cli {
    my ($server_class, @flags) = @_;
    local $ENV{PAGI_ENV} = 'production';
    # List-form pipe keeps paths and arguments intact; merge stderr in the child.
    my $pid = open my $pipe, '-|';
    die "Cannot fork CLI: $!" unless defined $pid;
    if (!$pid) {
        open STDERR, '>&', STDOUT or die "Cannot redirect stderr: $!";
        exec $^X, "-I$FindBin::Bin/../lib", "-I$FindBin::Bin/lib",
            "$FindBin::Bin/../bin/pagi-server", @flags,
            '-s', $server_class, $app_file;
        die "Cannot exec CLI: $!";
    }
    my $out = do { local $/; <$pipe> };
    close $pipe;
    return ($out, $?);
}

subtest 'CLI forwards each size independently and together' => sub {
    for my $case (
        [[], 'unset', 'unset'],
        [['--read-buffer-size', '16384'], '16384', 'unset'],
        [['--write-buffer-size', '32768'], 'unset', '32768'],
        [['--read-buffer-size', '16384', '--write-buffer-size', '32768'], '16384', '32768'],
    ) {
        my ($args, $read, $write) = @$case;
        my ($out, $status) = run_cli('PAGITest::FakeServer', @$args);
        is($status, 0, 'CLI succeeds');
        like($out, qr/^FAKESERVER read_buffer_size=\Q$read\E$/m, 'read forwarded');
        like($out, qr/^FAKESERVER write_buffer_size=\Q$write\E$/m, 'write forwarded');
    }
};

subtest 'CLI rejects non-integer arguments' => sub {
    for my $flag (qw(read-buffer-size write-buffer-size)) {
        (my $option = $flag) =~ s/-/_/g;
        for my $bad ('1.5', '64k', '0', '-1') {
            my ($out, $status) = run_cli('PAGI::Server', "--$flag", $bad);
            isnt($status, 0, 'CLI fails');
            like($out, qr/\Q$option\E.*positive integer/, 'integer error names option');
        }
    }
};

done_testing;
