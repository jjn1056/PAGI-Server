use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Test: an exception inside a socket write ends that connection only
# ============================================================
# IO::Async::Stream writes from the event loop, after the application's send
# returned, through a bare $handle->syswrite. A die there (Perl's "Wide
# character in syswrite", IO::Async::SSL's utf8::downgrade) used to unwind
# the loop and end the whole server process. It must end only its own
# connection, while every other connection and the listener carry on. The
# die is logged at error level with its message: it is a server or library
# defect, the operator's to see. A response whose last send was accepted has
# already ended cleanly (Www.pod: the server finished its output processing),
# so only the log sees it; a response still streaming ends as write_error.
#
# The fault is injected where IO::Async calls it: the syswrite method of the
# server's accepted sockets (IO::Socket::IP), armed by the application just
# before it sends -- dying once, or on every attempt as a wide character
# would. The test's own client writes use the syswrite builtin and are
# untouched.

use PAGI::Server;

our ($ARMED, $ALWAYS, $DIES) = (0, 0, 0);
{
    no warnings 'redefine';
    package IO::Socket::IP;
    sub syswrite {
        if ($main::ARMED)  { $main::ARMED = 0; $main::DIES++; die "armed write failure\n" }
        if ($main::ALWAYS) { $main::DIES++; die "persistent write failure\n" }
        my $self = shift;
        return $self->SUPER::syswrite(@_);
    }
}

my $loop = IO::Async::Loop->new;
$SIG{ALRM} = sub { die "t/86 exceeded its time bound\n" };
alarm 120;

my %ended;    # path => [reason, detail]
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    if ($scope->{type} eq 'lifespan') {
        while (1) {
            my $e = await $receive->();
            await $send->({ type => "$e->{type}.complete" });
            return if $e->{type} eq 'lifespan.shutdown';
        }
    }
    my $path = $scope->{path};
    my $conn = $scope->{'pagi.connection'};
    $conn->on_disconnect(sub { $ended{$path} = [ $conn->disconnect_reason, $conn->disconnect_detail ] });
    $ARMED  = 1 if $path eq '/boom' || $path eq '/stream';
    $ALWAYS = 1 if $path eq '/stuck' || $path eq '/stream-stuck' || $path =~ m{\A/close};
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    if ($path =~ m{\A/stream}) {
        # Still streaming when the write dies: the scope has not ended.
        await $send->({ type => 'http.response.body', body => "part $path", more => 1 });
        while ((await $receive->())->{type} ne 'http.disconnect') {}
        return;
    }
    await $send->({ type => 'http.response.body', body => "ok $path", more => 0 });
};

my @log;
my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
    access_log => undef, shutdown_timeout => 1,
    logger => sub { push @log, $_[0] },
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;

# A keep-alive client: send a request, pump until its whole response arrived.
sub client { IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5)
    or die "connect: $!" }
sub request {
    my ($sock, $path, $head) = @_;
    syswrite($sock, $head // "GET $path HTTP/1.1\r\nHost: x\r\n\r\n");
    $sock->blocking(0);
    my ($wire, $eof) = ('', 0);
    for (1 .. 100) {
        $loop->loop_once(0.02);
        my $n = sysread($sock, my $buf, 65536);
        if (defined $n) { $n ? ($wire .= $buf) : ($eof = 1) }
        last if $eof || $wire =~ /\r\n\r\n.*\Q$path\E/s;
    }
    # Read on briefly so a close right after the response is seen too.
    for (1 .. 10) {
        last if $eof;
        $loop->loop_once(0.02);
        my $n = sysread($sock, my $buf, 65536);
        if (defined $n) { $n ? ($wire .= $buf) : ($eof = 1) }
    }
    return ($wire, $eof);
}

my $other = client();
my ($first) = request($other, '/before');
like($first, qr/\A HTTP\/1\.1\ 200 .* ok\ \/before/xs, 'a keep-alive connection is open and served');

for my $case (['/boom', qr/armed write failure/, 'once'],
              ['/stuck', qr/persistent write failure/, 'on every attempt'],
              ['/stream', qr/armed write failure/, 'while the response streams'],
              ['/stream-stuck', qr/persistent write failure/, 'on every attempt while it streams'],
              # A close-after-response request ends its scope, and frees its
              # Connection, before the loop flushes: the guard must not need it.
              ['/close-stuck', qr/persistent write failure/, 'on a Connection: close request',
               "GET /close-stuck HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"],
              ['/close-http10', qr/persistent write failure/, 'on an HTTP/1.0 request',
               "GET /close-http10 HTTP/1.0\r\n\r\n"]) {
    my ($path, $message, $how, $head) = @$case;
    ($DIES, @log) = (0);
    my ($wire, $eof) = request(client(), $path, $head);
    $ALWAYS = 0;
    ok(1, "the server process survived a write that died $how");
    unlike($wire, qr/\Q$path\E/, "$path: nothing of the response is sent");
    ok($eof, "$path: the connection whose write died is closed");
    is($DIES, 1, "$path: the dying write is not retried");
    for (1 .. 20) { last if exists $ended{$path}; $loop->loop_once(0.02) }
    my @died = grep { $_->{message} =~ /socket write died/ } @log;
    is(scalar @died, 1, "$path: the server logs it once");
    is($died[0]{level}, 'error', "$path: at error level");
    like($died[0]{message} // '', qr/\Asocket write died: $message; the connection was closed\z/,
        "$path: naming the exception");
    if ($path =~ m{\A/stream}) {
        is($ended{$path}[0], 'write_error', "$path: the unfinished response ends as a write_error");
        like($ended{$path}[1] // '', $message, "$path: carrying the write's own message");
    }
    else {
        ok(!exists $ended{$path}, "$path: a response already ended cleanly gets no disconnect");
    }
}

my ($again) = request($other, '/after');
like($again, qr/HTTP\/1\.1 200 .*ok \/after/s, 'the other open connection still works');

my ($fresh) = request(client(), '/new');
like($fresh, qr/HTTP\/1\.1 200 .*ok \/new/s, 'and the listener still accepts');

$server->shutdown->get;
done_testing;
