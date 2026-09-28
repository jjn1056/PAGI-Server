use strict;
use warnings;
use Test2::V0;
use Scalar::Util qw(weaken);
use Future::AsyncAwait;
use IO::Async::Loop;
use IO::Socket::INET;
use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

# Reusing the transport must not make an old scope's completed send machine
# writable again. This exercises the real app-return reset, not just factories.
subtest 'a saved send stays terminal across keep-alive requests' => sub {
    plan skip_all => 'socket integration is not supported on Windows' if $^O eq 'MSWin32';
    my ($old_send, $late_failed);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die 'HTTP only' unless $scope->{type} eq 'http';
        if ($old_send) {
            my $late = $old_send->({type=>'http.response.start',status=>201,headers=>[]});
            $late_failed = $late->is_failed;
            $late->failure if $late_failed;
        } else {
            $old_send = $send;
        }
        await $send->({type=>'http.response.start',status=>200,headers=>[['content-length','2']]});
        await $send->({type=>'http.response.body',body=>'ok',more=>0});
    };
    my $loop = IO::Async::Loop->new;
    my $server = PAGI::Server->new(app=>$app,host=>'127.0.0.1',port=>0,quiet=>1);
    $loop->add($server);
    $server->listen->get;
    my $sock = IO::Socket::INET->new(PeerAddr=>'127.0.0.1',PeerPort=>$server->port,Proto=>'tcp',Timeout=>5)
        or die "connect: $!";
    $sock->blocking(0);
    for my $i (1..2) {
        print $sock "GET /$i HTTP/1.1\r\nHost: x\r\n\r\n";
        my $wire = '';
        my $deadline = time + 5;
        while (time < $deadline && $wire !~ /\r\n\r\nok\z/) {
            $loop->loop_once(0.01);
            my $n = sysread($sock, my $buf, 4096);
            last if defined($n) && !$n;
            $wire .= $buf if $n;
        }
        like($wire, qr/\AHTTP\/1\.1 200[^\r]*\r\n.*\r\n\r\nok\z/s, "response $i remains intact");
    }
    ok($late_failed, 'old send rejects a second start after the transport is reused');
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

# Taking a reference to state must not accidentally keep its connection alive.
subtest 'saved send does not retain its connection' => sub {
    my $conn = PAGI::Server::Connection->new(protocol=>PAGI::Server::Protocol::HTTP1->new);
    my $weak = $conn;
    weaken($weak);
    my $send = $conn->_create_send({method=>'GET',headers=>[]});
    undef $conn;
    is($weak, undef, 'connection can be released while send is retained');
    ok($send->({type=>'http.response.body',body=>'late'})->is_done, 'send after owner disappears is a no-op');
};
# The shared coroutine must not retain its connection while parked.
{
    package Local::DrainConnection;
    our @ISA = ('PAGI::Server::Connection');
    sub _get_write_buffer_size { 100 }
    sub _wait_for_drain { $_[0]{drain} }
}
subtest 'pending body send does not retain its connection' => sub {
    my $drain = Future->new;
    my $conn = Local::DrainConnection->new(
        protocol=>PAGI::Server::Protocol::HTTP1->new, write_high_watermark=>10);
    $conn->{drain} = $drain;
    my $weak = $conn;
    weaken($weak);
    my $send = $conn->_create_send({method=>'GET',headers=>[]});
    $send->({type=>'http.response.start',status=>200,headers=>[]})->get;
    my $pending = $send->({type=>'http.response.body',body=>'later',more=>0});
    ok(!$pending->is_ready, 'body is waiting on real Future backpressure');
    undef $conn;
    is($weak, undef, 'suspended send has no strong connection ownership');
    $drain->done;
    ok($pending->is_done, 'send settles when drain resumes after owner disappears');
};
done_testing;
