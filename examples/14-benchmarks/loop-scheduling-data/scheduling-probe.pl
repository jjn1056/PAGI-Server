use strict;
use warnings;
use JSON::PP;
my $backend=shift // die "backend required\n";
my $class="IO::Async::Loop::$backend";
eval "require $class; 1" or die $@;
my $loop=$class->new;
pipe(my $reader,my $writer) or die $!;
my ($round,$reads)=(0,0);
my @events;
my $record=sub { push @events, {event=>$_[0],round=>$round,reads=>$reads} };
$loop->watch_io(handle=>$reader,on_read_ready=>sub {
    sysread($reader,my $byte,1)==1 or die "read: $!";
    ++$reads;
    $record->('io');
    if($reads==1) {
        $loop->later(sub {
            $record->('from_io');
            $loop->later(sub { $record->('nested') });
        });
    }
    syswrite($writer,'x')==1 or die "write: $!";
});
syswrite($writer,'x')==1 or die $!;
$loop->later(sub { $record->('before_loop') });
my $cancel=$loop->later(sub { $record->('cancelled') });
$loop->unwatch_idle($cancel);
my $future=$loop->later;
for (1..5) { ++$round; $loop->loop_once(0.01) }
my @busy=@events;
my $ready_busy=$future->is_ready ? 1 : 0;
$loop->unwatch_io(handle=>$reader,on_read_ready=>1);
sysread($reader,my $byte,1)==1 or die "drain: $!";
for (1..2) { ++$round; $loop->loop_once(0.01) }
print JSON::PP->new->canonical->pretty->encode({backend=>$backend,
    version=>$class->VERSION, busy_events=>\@busy, all_events=>\@events,
    future_ready_during_io=>$ready_busy, future_ready_after=>$future->is_ready?1:0});
