use strict;
use warnings;
use Future::AsyncAwait;
use IO::Async::Loop;
use Net::Async::WebSocket::Client;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);
use JSON::PP qw(encode_json);

my ($url, $seconds, $connections) = @ARGV;
die "Usage: perl ws-client.pl ws://host:port/ seconds connections\n"
    unless $url && $seconds > 0 && $connections > 0;
$SIG{ALRM} = sub { die "WebSocket benchmark timed out\n" };
alarm(int($seconds) + 30);
sub now { clock_gettime(CLOCK_MONOTONIC) }
sub percentile {
    my ($values, $q) = @_;
    my @sorted = sort {$a <=> $b} @$values;
    return $sorted[int($q * $#sorted)];
}
my $loop = IO::Async::Loop->new;
my (@clients, @replies, @closing, @setup, @latencies, @close_times);
my $payload = 'x' x 128;
my @connect;
for my $i (0..$connections-1) {
    my $client = Net::Async::WebSocket::Client->new(
        on_binary_frame => sub {
            my (undef, $bytes) = @_;
            die "Incorrect echo" unless $bytes eq $payload;
            die "Unexpected echo" unless $replies[$i] && !$replies[$i]->is_ready;
            $replies[$i]->done;
        },
        on_close_frame => sub {
            my (undef, $bytes) = @_;
            die "Unexpected Close" unless $closing[$i];
            die "Bad Close code" unless length($bytes) >= 2 && unpack('n', $bytes) == 1000;
            $closing[$i]->done unless $closing[$i]->is_ready;
        },
    );
    $loop->add($client);
    push @clients, $client;
    my $start = now();
    push @connect, $client->connect(url => $url)->on_done(sub { push @setup, now() - $start });
}
Future->needs_all(@connect)->get;

async sub exchange_until {
    my ($i, $deadline, $measure) = @_;
    while (now() < $deadline) {
        $replies[$i] = $loop->new_future;
        my $start = now();
        await $clients[$i]->send_binary_frame($payload);
        await $replies[$i];
        push @latencies, now() - $start if $measure;
    }
}
my $warm_until = now() + 1;
Future->needs_all(map { exchange_until($_, $warm_until, 0) } 0..$#clients)->get;
my $start = now();
Future->needs_all(map { exchange_until($_, $start + $seconds, 1) } 0..$#clients)->get;
my $elapsed = now() - $start;

async sub close_client {
    my ($i) = @_;
    $closing[$i] = $loop->new_future;
    my $start = now();
    await $clients[$i]->send_close_frame(pack('n', 1000));
    await $closing[$i];
    push @close_times, now() - $start;
    $clients[$i]->close;
}
Future->needs_all(map { close_client($_) } 0..$#clients)->get;
die "No echo samples" unless @latencies;
print encode_json({messages => scalar(@latencies), elapsed => $elapsed,
    messages_per_second => @latencies / $elapsed,
    p50_ms => 1000 * percentile(\@latencies, 0.50),
    p99_ms => 1000 * percentile(\@latencies, 0.99),
    connect_p50_ms => 1000 * percentile(\@setup, 0.50),
    close_reply_p50_ms => 1000 * percentile(\@close_times, 0.50),
    connections => 0 + $connections}), "\n";
alarm 0;
