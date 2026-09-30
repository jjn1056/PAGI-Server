use strict;
use warnings;
use Test2::V0;
use JSON::MaybeXS ();

use PAGI::Server;

# JSON output is for machines reading STDERR: one object per line, the event
# as fields rather than folded into a sentence.

my $app     = sub { };
my $decoder = JSON::MaybeXS->new(utf8 => 1);

sub server_with { PAGI::Server->new(app => $app, @_) }

sub warned_by {
    my ($code) = @_;
    my @warned;
    local $SIG{__WARN__} = sub { push @warned, $_[0] };
    $code->();
    return \@warned;
}

subtest 'log_format defaults to text and accepts only text or json' => sub {
    is(server_with()->{log_format}, 'text', 'text when not given');
    is(server_with(log_format => 'json')->{log_format}, 'json', 'json when asked');
    is(dies { server_with(log_format => 'xml') },
        "Invalid log_format 'xml' - must be 'text' or 'json'\n",
        'anything else is refused with the exact message');
};

subtest 'the JSON sink writes one ordered object per event' => sub {
    my $server = server_with(log_format => 'json', log_level => 'info');
    my $warned = warned_by(sub {
        $server->_log(error => "PAGI application error: boom\n", 'PAGI::Server::Connection');
    });

    is(scalar @$warned, 1, 'one line');
    like($warned->[0], qr/\A\{"time":"[^"]+","level":"error","category":"PAGI::Server::Connection","message":"PAGI application error: boom","pid":\d+\}\n\z/,
        'keys in order, trailing newline stripped from the message, one newline after the object');
    my $event = $decoder->decode($warned->[0]);
    like($event->{time}, qr/\A\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z\z/, 'RFC 3339 UTC ms');
    is($event->{pid}, $$, 'pid is a number');
};

subtest 'a multi-line message stays one JSON line' => sub {
    my $server = server_with(log_format => 'json');
    my $warned = warned_by(sub { $server->_log(error => "first\nsecond") });
    is(scalar @$warned, 1, 'one warn call');
    is(($warned->[0] =~ tr/\n//), 1, 'exactly one newline, at the end');
    is($decoder->decode($warned->[0])->{message}, "first\nsecond", 'the newline survives as \\n');
};

subtest 'a worker is a field in JSON, not a prefix' => sub {
    my @events;
    my $server = server_with(log_format => 'json', logger => sub { push @events, $_[0] });
    $server->{is_worker}  = 1;
    $server->{worker_num} = 2;
    my $warned = warned_by(sub { $server->_log(info => 'serving') });

    is($events[0]{message}, 'serving', 'the message is bare');
    is($events[0]{worker}, 2, 'the worker is a field');
    is($warned, [], 'a custom logger replaces the JSON writer: nothing on STDERR');

    my $plain = server_with(log_format => 'json');
    $plain->{is_worker}  = 1;
    $plain->{worker_num} = 2;
    my $line = warned_by(sub { $plain->_log(info => 'serving') })->[0];
    my $event = $decoder->decode($line);
    is([@$event{qw(message worker pid)}], ['serving', 2, $$], 'the built-in sink writes both fields');
};

subtest 'bytes and characters both produce valid JSON' => sub {
    my $server = server_with(log_format => 'json');
    for my $message ("bad \xff byte", "caf\xc3\xa9", "wide \x{263a}") {
        my $line = warned_by(sub { $server->_log(warn => $message) })->[0];
        ok(eval { $decoder->decode($line); 1 }, 'decodes') or diag($@);
    }
};

subtest 'text format is unchanged' => sub {
    my $server = server_with(log_format => 'text');
    is(warned_by(sub { $server->_log(info => 'listening') }), ["listening\n"],
        'the bare message, byte for byte');
};

done_testing;
