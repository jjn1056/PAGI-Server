use strict;
use warnings;
use Test2::V0;
use Scalar::Util qw(weaken);
use IO::Async::Loop;
use PAGI::Server;
use PAGI::Server::ConnectionState;

my $loop = IO::Async::Loop->new;
my $server = PAGI::Server->new(app => sub {}, quiet => 1);
$loop->add($server);
my $connection = { server => $server };
sub state { PAGI::Server::ConnectionState->new(connection => $connection, @_) }

subtest 'unobserved completion releases the scope without a loop turn' => sub {
    my $state = state();
    my $weak = $state;
    weaken($weak);
    $state->_mark_complete;
    ok($state->response_complete, 'completion fact is immediate');
    ok(!$state->is_connected, 'scope is terminal');
    undef $state;
    ok(!defined $weak, 'no empty notification keeps the scope alive');
    $loop->loop_once(0);  # drain queued work when run against old code
};

subtest 'clean completion releases unused disconnect and abort closures' => sub {
    my $capture = [];
    my $weak = $capture;
    weaken($weak);
    my $callback = do { my $held = $capture; sub { die 'must not run' if $held } };
    my $state = state(on_abort => $callback);
    $state->on_disconnect($callback);
    undef $callback;
    undef $capture;
    $state->_mark_complete;
    ok(!defined $weak, 'unused closures released without a loop turn');
    $loop->loop_once(0);
};

for my $method (qw(on_complete on_end)) {
    subtest "$method alone still receives deferred delivery exactly once" => sub {
        my $state = state();
        my @calls;
        $state->$method(sub { push @calls, [@_] });
        $state->_mark_complete;
        $state->_mark_complete;
        is(\@calls, [], 'nothing called inline');
        undef $state;
        $loop->loop_once(0);
        is(\@calls, $method eq 'on_complete' ? [[]] : [[undef, undef]],
            'delivery survives caller releasing the state');
    };
}

subtest 'end_future alone still resolves later' => sub {
    my $state = state();
    my $future = $state->end_future;
    $state->_mark_complete;
    ok(!$future->is_ready, 'future stays pending inside terminal transition');
    undef $state;
    $loop->loop_once(0);
    ok($future->is_done, 'future completes on the loop');
    is([$future->get], [undef], 'clean outcome preserved');
};

subtest 'late observers work after an unobserved completion' => sub {
    my $state = state();
    $state->_mark_complete;
    my @calls;
    $state->on_complete(sub { push @calls, ['complete', @_] });
    $state->on_end(sub { push @calls, ['end', @_] });
    is(\@calls, [['complete'], ['end', undef, undef]], 'late callbacks run immediately');
    my $future = $state->end_future;
    ok($future->is_done, 'late future already resolved');
    is([$future->get], [undef], 'late future has clean outcome');
    $loop->loop_once(0);
    is(scalar @calls, 2, 'late callbacks not repeated');
};

subtest 'unobserved disconnect releases state and unused clean-only hooks' => sub {
    my $capture = [];
    my $weak_capture = $capture;
    weaken($weak_capture);
    my $callback = do { my $held = $capture; sub { die 'must not run' if $held } };
    my $state = state(on_abort => $callback);
    $state->on_complete($callback);
    undef $callback;
    undef $capture;
    $state->_mark_disconnected('client_closed', 'partial upload');
    $state->_mark_disconnected('server_error', 'later error');
    ok(!$state->is_connected, 'terminal immediately');
    ok(!$state->response_complete, 'not a clean completion');
    is([$state->disconnect_reason, $state->disconnect_detail],
        ['client_closed', 'partial upload'], 'first terminal record wins');
    ok(!defined $weak_capture, 'unused clean-only and abort hooks released');
    my $weak = $state;
    weaken($weak);
    undef $state;
    ok(!defined $weak, 'no empty delivery retains disconnected scope');
    $loop->loop_once(0);
};

for my $method (qw(on_disconnect on_end disconnect_future end_future)) {
    subtest "$method alone receives deferred abnormal delivery" => sub {
        my $state = state();
        my @calls;
        my $future;
        if ($method =~ /future$/) {
            my $cancelled = $state->$method;
            $future = $state->$method;
            $cancelled->cancel;
            $future->on_done(sub { push @calls, [@_] });
        } else {
            $state->$method(sub { push @calls, [@_] });
        }
        $state->_mark_disconnected('client_closed', 'partial upload');
        is(\@calls, [], 'not delivered in caller stack');
        undef $state;
        $loop->loop_once(0);
        is(\@calls, $future ? [['client_closed']] : [['client_closed', 'partial upload']],
            'one delivery survives state release and observer cancellation');
    };
}

subtest 'late abnormal observers read the saved record' => sub {
    my $state = state();
    $state->_mark_disconnected('client_closed', 'partial upload');
    my @calls;
    $state->on_disconnect(sub { push @calls, [@_] });
    $state->on_end(sub { push @calls, [@_] });
    is(\@calls, [['client_closed', 'partial upload'], ['client_closed', 'partial upload']], 'late callbacks fire immediately');
    for my $method (qw(disconnect_future end_future)) {
        my $future = $state->$method;
        ok($future->is_done, "$method already resolved");
        is([$future->get], ['client_closed'], "$method keeps reason");
    }
    $loop->loop_once(0);
    is(scalar @calls, 2, 'late observers never repeated');
};

subtest 'shutdown synchronous override still delivers all abnormal observers' => sub {
    my $state = state();
    my @calls;
    $state->on_disconnect(sub { push @calls, 'disconnect' });
    $state->on_end(sub { push @calls, 'end' });
    my $disconnect = $state->disconnect_future;
    my $end = $state->end_future;
    $state->_mark_disconnected('server_shutdown', 'shutdown', 1);
    is(\@calls, ['disconnect', 'end'], 'shutdown delivers without pumping the loop');
    is([$disconnect->get, $end->get], ['server_shutdown', 'server_shutdown'],
        'shutdown resolves both futures');
};

$loop->remove($server);
done_testing;
