use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Minimal HTTP responder used by the apps below.
async sub respond_ok {
    my ($scope, $receive, $send) = @_;
    while (1) {
        my $event = await $receive->();
        last if $event->{type} ne 'http.request';
        last unless $event->{more};
    }
    await $send->({ type => 'http.response.start', status => 200, headers => [] });
    await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
}

subtest "lifespan_mode 'on' makes a startup decline fatal" => sub {
    my $loop = IO::Async::Loop->new;

    # An app that declines lifespan by raising (the canonical decline idiom).
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "this app does not implement lifespan" if $scope->{type} eq 'lifespan';
        return await respond_ok($scope, $receive, $send);
    };

    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1,
        lifespan_mode => 'on',
    );
    $loop->add($server);

    my $err = dies { $server->listen->get };
    ok($err, "server refused to start when lifespan_mode='on' and the app declined");

    $loop->remove($server);
};

subtest "lifespan_mode 'off' is rejected (Lifespan spec: no off switch)" => sub {
    my $err = dies {
        PAGI::Server->new(app => sub { }, host => '127.0.0.1', port => 0,
                          quiet => 1, lifespan_mode => 'off');
    };
    like($err, qr/Invalid lifespan_mode 'off'/, 'constructor rejects off');
    like($err, qr/nonconforming/, 'error explains why');
};

subtest "configure(lifespan_mode => 'off') is rejected with the same message" => sub {
    my $server = PAGI::Server->new(app => sub { }, host => '127.0.0.1', port => 0,
                                    quiet => 1);

    my $err = dies { $server->configure(lifespan_mode => 'off') };
    like($err, qr/Invalid lifespan_mode 'off'/, 'configure() rejects off');
    like($err, qr/nonconforming/, 'error explains why');
};

subtest 'an invalid lifespan_mode is rejected at construction' => sub {
    my $err = dies {
        PAGI::Server->new(app => sub { }, lifespan_mode => 'bogus');
    };
    ok($err, 'construction failed');
    like($err, qr/lifespan_mode/, 'error names the offending option');
    like($err, qr/auto.*on/, "error names the two valid modes");
};

subtest "lifespan_startup_timeout => 0 is rejected (spec: must not block startup indefinitely)" => sub {
    my $err = dies {
        PAGI::Server->new(app => sub { }, host => '127.0.0.1', port => 0,
                          quiet => 1, lifespan_startup_timeout => 0);
    };
    ok($err, 'construction failed');
    like($err, qr/lifespan_startup_timeout/, 'error names the offending option');
    like($err, qr/block startup indefinitely/, 'error cites the spec constraint');
};

subtest "configure(lifespan_startup_timeout => 0) is rejected with the same message" => sub {
    my $server = PAGI::Server->new(app => sub { }, host => '127.0.0.1', port => 0,
                                    quiet => 1);

    my $err = dies { $server->configure(lifespan_startup_timeout => 0) };
    ok($err, 'configure() failed');
    like($err, qr/lifespan_startup_timeout/, 'error names the offending option');
    like($err, qr/block startup indefinitely/, 'error cites the spec constraint');
};

subtest "bin/pagi-server --lifespan off exits nonzero with the spec-forbids message" => sub {
    my $pagi_server = "$FindBin::Bin/../bin/pagi-server";
    local $ENV{PAGI_ENV} = 'production';   # deterministic mode, no Lint wrap
    my $out = `$^X -I$FindBin::Bin/../lib $pagi_server --lifespan off --port 0 -e "sub { }" 2>&1`;
    my $exit_code = $? >> 8;

    # An uncaught die's exit status is $! when errno is nonzero (else 255),
    # and ambient errno at die time varies by platform and module-search
    # history -- only "nonzero" is portable.
    isnt($exit_code, 0, '--lifespan off exits nonzero');
    like($out, qr/Invalid lifespan_mode 'off'/, 'stderr carries the same rejection message');
    like($out, qr/the PAGI Lifespan spec forbids skipping the protocol/,
        'stderr explains why per the Lifespan spec');
};

done_testing;
