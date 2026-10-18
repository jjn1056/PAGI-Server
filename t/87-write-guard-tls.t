use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Net::Async::HTTP;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

BEGIN {
    eval { require IO::Async::SSL; require IO::Socket::SSL; 1 }
        or do { require Test2::V0; Test2::V0::plan(skip_all => 'TLS modules not installed (optional)') };
}
plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Test: the write guard on a TLS connection
# ============================================================
# A TLS stream writes through IO::Async::SSL's own writer (sslwrite), which
# calls IO::Socket::SSL's syswrite and does a bare utf8::downgrade that dies
# on a wide character. The guard must wrap that writer, not replace it (TLS
# requests still work through it), and a die inside it must end only its own
# connection: logged, the connection closed, the server still serving.

use PAGI::Server;

my $cert_dir = "$FindBin::Bin/certs";
plan skip_all => 'Test certificates not found' unless -f "$cert_dir/server.crt" && -f "$cert_dir/server.key";

our ($ARMED, $DIES) = (0, 0);
{
    no warnings 'redefine';
    my $syswrite = \&IO::Socket::SSL::syswrite;
    *IO::Socket::SSL::syswrite = sub {
        if ($main::ARMED) { $main::ARMED = 0; $main::DIES++; die "armed tls write failure\n" }
        goto &$syswrite;
    };
}

my $loop = IO::Async::Loop->new;
$SIG{ALRM} = sub { die "t/87 exceeded its time bound\n" };
alarm 120;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    if ($scope->{type} eq 'lifespan') {
        while (1) {
            my $e = await $receive->();
            await $send->({ type => "$e->{type}.complete" });
            return if $e->{type} eq 'lifespan.shutdown';
        }
    }
    $ARMED = 1 if $scope->{path} eq '/boom';
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => "ok $scope->{path}", more => 0 });
};

my @log;
my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
    access_log => undef, shutdown_timeout => 1,
    logger => sub { push @log, $_[0] },
    ssl => { cert_file => "$cert_dir/server.crt", key_file => "$cert_dir/server.key" },
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;

# A fresh client per request: each gets its own TLS connection.
sub get {
    my ($path) = @_;
    my $http = Net::Async::HTTP->new(SSL_verify_mode => 0, max_connections_per_host => 1);
    $loop->add($http);
    my $f = $http->GET("https://127.0.0.1:$port$path");
    my $done = $loop->delay_future(after => 10);
    # A request whose connection closes fails its Future: that is an outcome
    # here, not an error of the test.
    Future->wait_any($f->without_cancel->else(sub { Future->done }), $done)->get;
    my $response = $f->is_done ? $f->get : undef;
    $loop->remove($http);
    return $response;
}

my $first = get('/before');
is($first && $first->code, 200, 'a TLS request works through the guard (it wraps the TLS writer)');
is($first && $first->content, 'ok /before', 'with its body');

$DIES = 0;
my $boom = get('/boom');
ok(1, 'the server process survived a TLS write that died');
ok(!($boom && $boom->is_success && ($boom->content // '') eq 'ok /boom'), 'that response did not arrive whole');
cmp_ok($DIES, '<=', 2, "the dying write is not retried ($DIES attempts)");
my @died = grep { $_->{message} =~ /socket write died/ } @log;
is(scalar @died, 1, 'the server logs it once');
like($died[0]{message} // '', qr/\Asocket write died: armed tls write failure; the connection was closed\z/,
    'naming the exception');

my $after = get('/after');
is($after && $after->code, 200, 'the TLS listener still serves');

$server->shutdown->get;
done_testing;
