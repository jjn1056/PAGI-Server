use strict;
use warnings;
use Test::More;
use Future;
use FindBin;

# Small contract check for the examples themselves; no server processes.
for my $case (
    ['get', 'http', [], 'Hello from PAGI'],
    ['post', 'http', [
        {type => 'http.request', body => 'x' x 400, more => 1},
        {type => 'http.request', body => 'x' x 624, more => 0},
    ], "1024\n"],
    ['stream', 'http', [], 'x' x 65536],
    ['sse', 'sse', [], undef],
    ['websocket', 'websocket', [
        {type => 'websocket.connect'},
        {type => 'websocket.receive', bytes => 'x' x 128},
        {type => 'websocket.disconnect', code => 1000},
    ], undef],
) {
    my ($name, $type, $events, $expected) = @$case;
    subtest $name => sub {
        my $app = do "$FindBin::Bin/$name.pl";
        ok(ref($app) eq 'CODE', 'example loads') or return;
        my @sent;
        my $future = $app->({type => $type, query_string => ''},
            sub { die 'unexpected receive' unless @$events; Future->done(shift @$events) },
            sub { push @sent, $_[0]; Future->done });
        $future->get;
        if (defined $expected) {
            is(join('', map { $_->{body} // '' } @sent), $expected, 'complete response bytes');
            is($sent[-1]{more} // 0, 0, 'response ends');
        } elsif ($name eq 'sse') {
            my @data = grep { $_->{type} eq 'sse.send' } @sent;
            is(scalar @data, 100, '100 events');
            is_deeply([map { $_->{id} } @data], [map { "$_" } 1..100], 'ordered IDs');
            is($sent[-1]{type}, 'sse.close', 'clean stream end');
        } else {
            is($sent[0]{type}, 'websocket.accept', 'accepts handshake');
            is($sent[1]{bytes}, 'x' x 128, 'exact echo');
        }
    };
}
done_testing;
