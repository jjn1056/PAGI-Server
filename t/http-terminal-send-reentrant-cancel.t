#!/usr/bin/env perl

# A receiver resumed by a backpressured terminal send may cancel that send
# (for example, when abandoning a producer after observing scope completion).
# Www.pod permits the receive to resume before or after the send Future is
# ready. Either ordering must preserve the clean outcome and deferred, once-only
# terminal notifications. A controlled drain makes the reentrant path exact.

use strict;
use warnings;
use Test2::V0;
use Future::AsyncAwait;
use IO::Async::Loop;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
{
    package Local::Stream;
    sub write { $_[0]{output} .= $_[1]; }
    package Local::Server;
    sub loop { $_[0]{loop} }
    package Local::Connection;
    our @ISA = ('PAGI::Server::Connection');
    sub _get_write_buffer_size { 100 }
    sub _wait_for_drain { $_[0]{test_drain} }
}
my @warnings;
local $SIG{__WARN__} = sub { push @warnings, @_ };
my $loop = IO::Async::Loop->new;
my $server = bless { loop => $loop }, 'Local::Server';
my $stream = bless {}, 'Local::Stream';
my $conn = Local::Connection->new(server => $server, stream => $stream,
    protocol => PAGI::Server::Protocol::HTTP1->new, write_high_watermark => 10);
my $cs = PAGI::Server::ConnectionState->new(connection => $conn);
$conn->{current_connection_state} = $cs;
$conn->{test_drain} = Future->new;
my $request = { method => 'GET', headers => [] };
my $receive = $conn->_create_receive($request);
my $send = $conn->_create_send($request);
$receive->()->get;
$send->({ type => 'http.response.start', status => 200, headers => [] })->get;
my ($send_future, $seen, $complete, $end) = (undef, undef, 0, 0);
$cs->on_complete(sub { $complete++ });
$cs->on_end(sub { $end++ });
my $watch = (async sub {
    $seen = await $receive->();
    ok($cs->response_complete, 'terminal facts before receiver resumes');
    $send_future->cancel;
})->();
$send_future = $send->({ type => 'http.response.body', body => 'done', more => 0 });
ok(!$send_future->is_ready, 'terminal send parked');
my $ok = eval { $conn->{test_drain}->done; 1 };
ok($ok, 'drain resumption does not throw') or diag($@);
is($seen, { type => 'http.disconnect' }, 'receive completed');
ok($watch->is_done, 'receiver finishes');
is([$complete, $end], [0, 0], 'terminal callbacks remain deferred');
ok($send_future->is_ready, 'send is settled after reentrant cancellation');
ok(!$send_future->is_failed, 'send did not fail');
like($stream->{output}, qr/done/, 'terminal body was written');
$loop->loop_once(0);
is($complete, 1, 'one completion notification');
is($end, 1, 'one end notification');
ok($cs->response_complete, 'clean outcome remains');
is($cs->disconnect_reason, undef, 'no false abnormal outcome');
is(\@warnings, [], 'no warnings');
done_testing;
