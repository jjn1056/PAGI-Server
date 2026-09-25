package WatchIdleProbe;
use strict;
use warnings;
use IO::Async::Loop::EV;
my ($scheduled, $delivered, $pending, $peak)=(0,0,0,0);
my $original=\&IO::Async::Loop::EV::watch_idle;
{
    no warnings 'redefine';
    *IO::Async::Loop::EV::watch_idle=sub {
        my ($self,%args)=@_;
        my $code=$args{code};
        ++$scheduled;
        ++$pending;
        $peak=$pending if $pending>$peak;
        $args{code}=sub { --$pending; ++$delivered; goto &$code; };
        return $original->($self,%args);
    };
}
END {
    print STDERR "WATCH_IDLE scheduled=$scheduled delivered=$delivered pending=$pending peak=$peak\n";
}
1;
