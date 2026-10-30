use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use IO::Socket::INET;
use Time::HiRes qw(time);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}
use PAGI::Server;
use Net::HTTP2::nghttp2::Session;

# write_timeout on HTTP/2 is per stream: a stream with bytes waiting that the
# client has taken none of for write_timeout seconds is reset, and its
# application told write_timeout. Flow control holds a stalled client's bytes
# in the stream, not in the socket, so a socket measure would never see it.

my $loop = IO::Async::Loop->new;
my (@log, @warnings);
$SIG{__WARN__} = sub { push @warnings, $_[0] };

sub start_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, log_level => 'warn', access_log => undef,
        shutdown_timeout => 1, logger => sub { push @log, $_[0] }, http2 => 1, write_timeout => 0.5,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

# Each path's application sends 128 x 64 KB without awaiting, then records how
# its scope ended and waits for its sends, as an application keeping its send
# Futures does.
sub flood_app {
    my ($seen) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'http';
        my $conn = $scope->{'pagi.connection'};
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my @sent = map { $send->({ type => 'http.response.body', body => 'x' x 65_536, more => 1 }) } 1 .. 128;
        $seen->{ $scope->{path} }{pending} = grep { !$_->is_ready } @sent;
        my $event;
        do { $event = await $receive->() } until $event->{type} =~ /disconnect\z/;
        $seen->{ $scope->{path} }{reason} = $conn->disconnect_reason;
        await Future->wait_all(@sent);
        $seen->{ $scope->{path} }{all_resolved} = (grep { $_->is_done } @sent) == @sent ? 1 : 0;
    };
}

sub h2_open {
    my ($server, %preface) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp')
        or die "connect: $!";
    my (%closed, %bytes, %path, %headers);
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { 0 },
        on_frame_recv      => sub {
            my ($frame) = @_;
            $headers{ $path{ $frame->{stream_id} } // '' } = 1
                if $frame->{stream_id} && $frame->{type} == Net::HTTP2::nghttp2::NGHTTP2_HEADERS();
            0;
        },
        on_data_chunk_recv => sub { $bytes{ $path{ $_[0] } // '' } += length $_[1]; 0 },
        on_stream_close    => sub { $closed{ $path{ $_[0] } // '' } = $_[1]; 0 },
    });
    $client->send_connection_preface(%preface);
    return { sock => $sock, client => $client, closed => \%closed, bytes => \%bytes, path => \%path, headers => \%headers };
}

sub h2_request {
    my ($h, $path) = @_;
    my $sid = $h->{client}->submit_request(method => 'GET', path => $path, scheme => 'http',
        authority => 'localhost', headers => []);
    $h->{path}{$sid} = $path;
    return $sid;
}

sub h2_flush { my ($h) = @_; my $out = $h->{client}->mem_send; syswrite($h->{sock}, $out) if length $out }

sub h2_read {
    my ($h) = @_;
    $h->{sock}->blocking(0);
    while (defined(my $n = sysread($h->{sock}, my $buf, 1 << 20))) { last unless $n; $h->{client}->mem_recv($buf) }
    h2_flush($h);
}

subtest 'a client that stops reading: its stream is reset with write_timeout' => sub {
    my %seen;
    @log = ();
    my $server = start_server(flood_app(\%seen));
    my $h = h2_open($server);
    h2_request($h, '/stalled');
    h2_flush($h);
    my $deadline = time + 5;
    $loop->loop_once(0.01) until exists $seen{'/stalled'}{all_resolved} || time > $deadline;   # reads nothing
    ok($seen{'/stalled'}{pending} > 0, "$seen{'/stalled'}{pending} sends were queued");
    is($seen{'/stalled'}{reason}, 'write_timeout', 'the application saw write_timeout');
    is($seen{'/stalled'}{all_resolved}, 1, 'every pending send resolved');
    is(scalar(grep { ($_->{message} // '') =~ /Write timeout \(0\.5s\).*stream/ } @log), 1, 'one warn line names the stream');
    # Read now: the stream was reset (CANCEL, 8).
    my $until = time + 3;
    while (time < $until && !exists $h->{closed}{'/stalled'}) { $loop->loop_once(0.01); h2_read($h) }
    is($h->{closed}{'/stalled'}, 8, 'the client sees the stream reset with CANCEL');
    close $h->{sock};
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'one starved stream is reset while another on the connection carries on' => sub {
    my %seen;
    @log = ();
    my $server = start_server(flood_app(\%seen));
    # Streams start with no window; the client opens it only for /moving.
    my $h = h2_open($server, initial_window_size => 0);
    my $moving = h2_request($h, '/moving');
    h2_request($h, '/starved');
    h2_flush($h);
    # Once /moving's response has begun the client opens its window by 64 KB,
    # once; its automatic updates then hand back only what it consumes, and it
    # reads the socket 64 KB every 0.1s. So /moving always has bytes waiting
    # for window yet makes steady progress, for longer than write_timeout.
    # /starved's window stays at 0.
    my ($opened, $next, $deadline) = (0, time, time + 3);
    $h->{sock}->blocking(0);
    while (time < $deadline) {
        $loop->loop_once(0.01);
        if (time >= $next) {
            my $n = sysread($h->{sock}, my $buf, 65_536);
            $h->{client}->mem_recv($buf) if $n;
            $next = time + 0.1;
        }
        if (!$opened && $h->{headers}{'/moving'}) {
            $h->{client}->submit_window_update($moving, 65_536);
            $opened = 1;
        }
        h2_flush($h);
    }
    is($seen{'/starved'}{reason}, 'write_timeout', 'the starved stream was reset with write_timeout');
    ok(!exists $seen{'/moving'}{reason}, 'the other stream was not');
    ok(($h->{bytes}{'/moving'} // 0) > 300_000 && ($h->{bytes}{'/moving'} // 0) < 8_388_608,
        "and kept receiving, slowly ($h->{bytes}{'/moving'} of 8,388,608 bytes in 3s)");
    ok(!exists $h->{closed}{'/moving'}, 'and is still open');
    close $h->{sock};
    $server->shutdown->get;
    $loop->remove($server);
};

is([map { s/\s+\z//r } @warnings], [], 'no warnings');

done_testing;
