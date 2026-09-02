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

subtest 'the spec version appears only when the spec is installed' => sub {
    my $with = banner_for();
    like($with->[0], qr/\(PAGI [0-9.]+\)/,
        'PAGI is installed here, so it is named');

    # PAGI::Server has no runtime dependency on PAGI, so the banner has to
    # degrade rather than print "unknown" or die.
    no warnings 'redefine';
    local *PAGI::Server::_pagi_spec_version = sub { undef };
    my $without = banner_for();
    unlike($without->[0], qr/\(PAGI/,
        'and the parenthetical disappears entirely when it is not');
    like($without->[0], qr/\APAGI::Server \S+ listening on/,
        'leaving a well-formed line');
};

subtest 'the capability list is one line, below' => sub {
    my $lines = banner_for();

    is(scalar @$lines, 2, 'two lines: identity, then capabilities');
    like($lines->[1], qr/\A\s+loop \S+/, 'the second is indented and starts with the loop');
    like($lines->[1], qr/max_conn \d+/,     'max_conn is reported');
    like($lines->[1], qr/http2 /,           'so is http2');
    like($lines->[1], qr/tls /,             'and tls');
    like($lines->[1], qr/future_xs /,       'and future_xs');
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
