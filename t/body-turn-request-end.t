use strict;
use warnings;
use Test2::V0;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

# A request's end clears its body waits (_end_body_waits), so a wait left over
# from it can neither keep the next request from taking the turn nor end that
# request's turn when it settles late. Every live path settles parked receives
# before a request ends, so this drives the connection directly. The stale wait
# settles by its receive being cancelled: resolving its wake-up would resume it.

sub turn_of {
    my ($conn) = @_;
    return [ $conn->{_client_turn} // 0, scalar keys %{ $conn->{_body_waits} // {} } ];
}

my $conn = PAGI::Server::Connection->new(protocol => PAGI::Server::Protocol::HTTP1->new);

my $stale = $conn->_create_receive({ content_length => 4 })->();
is(turn_of($conn), [1, 1], 'a receive waiting for the body gives the client the turn');

$conn->{receive_pending} = undef;    # the ended request's wake-up is dropped
$conn->_end_body_waits;
is(turn_of($conn), [0, 0], 'the request ends: no turn, no waits');

my $next = $conn->_create_receive({ content_length => 4 })->();
my $wake = $conn->{receive_pending};
is(turn_of($conn), [1, 1], 'the next request waiting for its body takes the turn');

$stale->cancel;
is(turn_of($conn), [1, 1], 'the stale wait settling late leaves that turn alone');

$conn->{buffer} = 'body';
$conn->{receive_pending} = undef;
$wake->done;
is(turn_of($conn), [0, 0], 'the next request is woken: the application has the turn');
is($next->get, { type => 'http.request', body => 'body', more => 0 }, 'and gets its body');

done_testing;
