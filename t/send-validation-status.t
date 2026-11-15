use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;
use PAGI::Server::EventValidator;

# A response status is a final status, 200 to 599: three digits, no sign,
# no padding, no fraction. 1xx is not an application's to send.

my $V = 'PAGI::Server::EventValidator';

my @events = (
    [ 'http.response.start', sub { $V->can('validate_http_send')->({ type => 'http.response.start', status => $_[0] }) } ],
    [ 'sse.start',           sub { $V->can('validate_sse_send')->({ type => 'sse.start', status => $_[0] }, {}) } ],
);

for my $event (@events) {
    my ($type, $send) = @$event;
    subtest "$type status" => sub {
        for my $v (200, '200', 204, 599) {
            ok(lives { $send->($v) }, "$v passes");
        }
        for my $v (0, 99, 100, 101, 103, 199, 600, 999, 1000, '0200', '0204', '200.0', '200 ') {
            like(dies { $send->($v) },
                qr/\A\Q$type\E 'status' must be 200 to 599 \(got '\Q$v\E'\)\n\z/, "'$v' fails");
        }
        like(dies { $send->("50\n0") }, qr/\(got '50\\x0a0'\)\n\z/, 'a newline in the status is shown escaped');
    };
}

subtest 'WebSocket refusal status is 300 to 599' => sub {
    my $refuse = sub { $V->can('validate_websocket_send')->({ type => 'http.response.start', status => $_[0] }, {}) };
    like(dies { $refuse->(299) }, qr/WebSocket refusal status must be 300 or above \(got 299\)/, '299 fails as before');
    like(dies { $refuse->(600) }, qr/\Ahttp\.response\.start 'status' must be 200 to 599 \(got '600'\)\n\z/, '600 fails');
    ok(lives { $refuse->(404) }, '404 passes');
};

subtest "HTTP/1.1: status '0204' gives a 500" => sub {
    my $loop = IO::Async::Loop->new;
    my @logged;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope" if $scope->{type} ne 'http';
        await $receive->();
        await $send->({ type => 'http.response.start', status => '0204', headers => [] });
        await $send->({ type => 'http.response.body', body => '', more => 0 });
    };
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0,
        log_level => 'info', logger => sub { push @logged, $_[0] }, access_log => undef);
    $loop->add($server);
    $server->listen->get;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port,
        Proto => 'tcp', Timeout => 2) or die "Cannot connect: $!";
    print $sock "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    $sock->blocking(0);
    my ($wire, $eof) = ('', 0);
    my $deadline = time + 3;
    while (!$eof && time < $deadline) {
        $loop->loop_once(0.05);
        my $n = sysread($sock, my $buf, 4096);
        if (defined $n) { $n ? ($wire .= $buf) : ($eof = 1) }
    }
    close $sock;
    $server->shutdown->get;
    $loop->remove($server);

    like($wire, qr{\AHTTP/1\.1 500 }, 'the response is a 500');
    unlike($wire, qr{HTTP/1\.1 0204}, 'the status never reaches the wire');
    my @errors = grep { $_->{level} eq 'error' } @logged;
    is(scalar @errors, 1, 'one error is logged');
    like($errors[0]{message} // '', qr/'status' must be 200 to 599 \(got '0204'\)/, 'the log says why');
};

done_testing;
