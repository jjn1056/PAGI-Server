package PAGITest::FakeServer;
use strict;
use warnings;

our $VERSION = '0.001';

# Minimal implementation of the PAGI server-runner contract
# (new(%options) + run) used by this Runner plugin seam to test CLI -> server_options ->
# constructor chain without opening sockets.

sub new {
    my ($class, %options) = @_;
    return bless { options => \%options }, $class;
}

sub run {
    my ($self) = @_;
    for my $opt (qw(http2 write_high_watermark write_low_watermark)) {
        my $val = $self->{options}{$opt};
        print "FAKESERVER $opt=" . (defined $val ? $val : 'unset') . "\n";
    }
    return;
}

1;
