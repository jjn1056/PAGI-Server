use strict;
use warnings;
use Test2::V0;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server::TransportState;

# The drain fire is delivered deferred (next loop tick for h2, drain-future
# resolution for h1). A fast client can complete the whole request and tear
# the transport handle down before delivery; the armed on_drain callbacks
# must still fire. Regression test for the drain fire silently no-oping
# when the handle is garbage-collected between arm and delivery.

my $buffered = 100_000;   # above the high mark
my $armed_fire;

my $ts = PAGI::Server::TransportState->new(
    measure   => sub { $buffered },
    high      => 65_536,
    low       => 16_384,
    arm_drain => sub { $armed_fire = shift },
);

my ($hit_high, $hit_drain) = (0, 0);
$ts->on_high_water(sub { $hit_high++ });   # already above: fires and arms
$ts->on_drain(sub { $hit_drain++ });

is($hit_high, 1, 'on_high_water fired at registration (already above the mark)');
ok(defined $armed_fire, 'drain delivery was armed');

# Request completes and the handle is torn down before the deferred
# delivery runs (the scope, per-stream state, and connection are gone).
undef $ts;
$buffered = 0;

$armed_fire->();

is($hit_drain, 1, 'on_drain delivered even though the handle was torn down first');

done_testing;
