use strict;
use warnings;
use Test2::V0;

use PAGI::Server;

# The banner is what an operator reads first, and it did not say which server
# they were running. Its capability list was also duplicated across four listen
# paths, so the four could drift.

my $app = sub { };

sub banner_for {
    my (%args) = @_;
    my $where      = delete $args{where} // 'http://127.0.0.1:5000/';
    my $per_worker = delete $args{per_worker};
    my $server = PAGI::Server->new(app => $app, %args);
    return [ $server->_startup_banner($where, $per_worker) ];
}

subtest 'the first line names the server and where it is listening' => sub {
    my $lines = banner_for();

    like($lines->[0], qr/\APAGI::Server \Q$PAGI::Server::VERSION\E\b/,
        'it leads with the server and its version');
    like($lines->[0], qr{listening on http://127\.0\.0\.1:5000/\z},
        'and ends with the address, which is what people are looking for');
};

subtest 'the identity line carries nothing but the server' => sub {
    my $lines = banner_for();

    # The spec distribution is not a runtime dependency and is normally not
    # installed alongside the server, so its version is not the operator's
    # concern and never appears, present or not.
    is($lines->[0],
        "PAGI::Server $PAGI::Server::VERSION listening on http://127.0.0.1:5000/",
        'server, version, address; nothing about the spec');
};

subtest 'the capabilities are the last note in the block' => sub {
    my $lines = banner_for();

    is(scalar @$lines, 2, 'identity, then one note, with nothing else contributed');

    my $loop = $lines->[-1];
    like($loop, qr/\A\s+loop\s+\S/, 'indented and labelled');
    like($loop, qr/max_conn \d+/,      'max_conn is reported');
    like($loop, qr/http2 /,            'so is http2');
    like($loop, qr/tls /,              'and tls');
    like($loop, qr/future_xs /,        'and future_xs');
};

subtest 'Future::XS in use is named in the block and announced loudly' => sub {
    my @events;
    my $server = PAGI::Server->new(app => $app, logger => sub { push @events, $_[0] });

    # PAGI::Server keeps Future pure-perl for now (Future::XS 0.15 warns on a
    # dropped without_cancel observer, the shape of racing disconnect_future).
    # Future reads PERL_FUTURE_NO_XS only when it is compiled, so a Future
    # loaded before the server keeps XS, and the operator has to hear it.
    no warnings 'redefine';
    local *PAGI::Server::_future_xs_in_use = sub { 0 };
    like(($server->_startup_banner('http://127.0.0.1:5000/'))[-1], qr/future_xs off\z/,
        'pure-perl Future reads as off');
    $server->_warn_if_future_xs;
    is(\@events, [], 'and nothing is said');

    local *PAGI::Server::_future_xs_in_use = sub { 1 };
    like(($server->_startup_banner('http://127.0.0.1:5000/'))[-1],
        qr/future_xs on \(loaded before PAGI::Server\)/,
        'Future::XS in use reads as on, and says how it got there');
    $server->_warn_if_future_xs;
    is(scalar @events, 1, 'one line is logged');
    is($events[0]{level}, 'warn', 'at warn level, not buried in the info block');
    like($events[0]{message}, qr/Future::XS \S+ is in use/, 'it names what is in use');
    like($events[0]{message}, qr/lost a sequence Future/, 'and the warning applications will see');
    like($events[0]{message}, qr/PERL_FUTURE_NO_XS=1/, 'and the way out');
};

subtest 'contributed notes are rendered above it, aligned' => sub {
    my $server = PAGI::Server->new(
        app           => $app,
        startup_notes => [ [serving => './app.pl'], [mode => 'development (tty)'] ],
    );
    my @lines = $server->_startup_banner('http://127.0.0.1:5000/');

    is(scalar @lines, 4, 'identity, the two notes, then the capabilities');
    like($lines[1], qr/\A  serving\s+\.\/app\.pl\z/,     'what is being served leads');
    like($lines[2], qr/\A  mode\s+development \(tty\)\z/, 'then the mode');

    # One column, so the block scans -- which was the point of the exercise.
    my @cols = map { /\A  \S+(\s+)/ ? length("  " . $&) : () } @lines[1 .. $#lines];
    is(scalar(keys %{{ map { $_ => 1 } @cols }}), 1, 'every label pads to one width');
};

subtest 'multi-worker reports its connection limit per worker' => sub {
    my $single = banner_for();
    my $multi  = banner_for(per_worker => 1);

    like($single->[1], qr/max_conn \d+(?!\/)/,  'a single server reports a flat limit');
    like($multi->[1],  qr{max_conn \d+/worker}, 'a multi-worker one says per worker');
};

subtest 'every listen shape shares one capability line' => sub {
    my @suffixes = map { banner_for(where => $_)->[1] }
        'unix:/tmp/pagi.sock',
        'http://127.0.0.1:5000/',
        'http://127.0.0.1:5000/, http://127.0.0.1:5001/';

    is([ @suffixes[1, 2] ], [ ($suffixes[0]) x 2 ],
        'the four listen paths cannot drift apart, because there is one builder');
};

done_testing;
