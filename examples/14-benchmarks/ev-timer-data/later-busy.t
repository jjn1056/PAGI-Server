use strict;
use warnings;
use Test::More;
use IO::Async::Loop::EV;

for my $driver ('IO::Async', 'native EV') {
    subtest $driver => sub {
        my $loop = IO::Async::Loop::EV->new;
        pipe(my $reader, my $writer) or die $!;
        my ($round, $reads, $cancelled) = (0, 0, 0);
        my @events;
        my $record = sub { push @events, [$_[0], $round] };
        $loop->watch_io(handle => $reader, on_read_ready => sub {
            sysread($reader, my $byte, 1) == 1 or die "read: $!";
            ++$reads;
            if ($reads == 1) {
                $loop->later(sub {
                    $record->('from_io');
                    $loop->later(sub { $record->('nested') });
                });
            }
            syswrite($writer, 'x') == 1 or die "write: $!";
        });
        syswrite($writer, 'x') == 1 or die $!;
        $loop->later(sub { $record->('before_loop') });
        my $id = $loop->later(sub { ++$cancelled });
        $loop->unwatch_idle($id);
        my $future = $loop->later;
        my $cancel_future = $loop->later;
        $cancel_future->cancel;
        is(scalar @events, 0, 'callbacks are not invoked inline');
        for (1 .. 5) {
            ++$round;
            $driver eq 'native EV' ? EV::run(EV::RUN_ONCE) : $loop->loop_once(0.01);
        }
        my %seen = map { $_->[0] => $_->[1] } @events;
        ok($seen{before_loop}, 'initial callback runs while I/O stays ready');
        ok($seen{from_io}, 'I/O-queued callback runs while I/O stays ready');
        ok($seen{nested}, 'nested callback also makes progress');
        ok($future->is_done, 'Future form makes progress');
        is($cancelled, 0, 'cancelled callback does not run');
        ok($cancel_future->is_cancelled, 'cancelled Future stays cancelled');
        if ($seen{nested} && $seen{from_io}) {
            cmp_ok($seen{nested}, '>', $seen{from_io}, 'nested callback waits for another iteration');
        }
        $loop->unwatch_io(handle => $reader, on_read_ready => 1);
        sysread($reader, my $byte, 1) == 1 or die "drain: $!";
        $loop->loop_once(0.01) for 1 .. 2;
        my %count;
        ++$count{$_->[0]} for @events;
        is_deeply(\%count, {before_loop => 1, from_io => 1, nested => 1}, 'each callback delivered exactly once');
    };
}
done_testing;
