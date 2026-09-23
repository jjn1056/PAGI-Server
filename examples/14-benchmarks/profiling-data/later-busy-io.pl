use strict;
use warnings;
use IO::Async::Loop::EV;
my $loop=IO::Async::Loop::EV->new;
pipe(my $reader,my $writer) or die $!;
my ($reads,$later,$during)=(0,0,undef);
$loop->watch_io(handle=>$reader,on_read_ready=>sub {
    sysread($reader,my $byte,1)==1 or die "read: $!";
    ++$reads;
    syswrite($writer,'x')==1 or die "write: $!";
});
syswrite($writer,'x')==1 or die $!;
$loop->later(sub { ++$later });
$loop->watch_time(after=>0.1,code=>sub {
    $during=$later;
    $loop->unwatch_io(handle=>$reader,on_read_ready=>1);
    sysread($reader,my $byte,1)==1 or die "drain: $!";
    $loop->stop;
});
$loop->run;
# A zero timeout itself creates an immediately-ready timer in this backend.
# Allow a short bounded wait with no immediately-ready I/O or timer instead.
$loop->loop_once(0.01);
print "read_callbacks=$reads later_during_busy_io=$during later_after_io_stopped=$later\n";
die 'probe did not exercise readable IO' unless $reads>0;
die 'later failed to run after IO stopped' unless $later==1;
