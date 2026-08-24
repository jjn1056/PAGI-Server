use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my %seen;
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'lifespan';
    while (1) {
        my $msg = await $receive->();
        if ($msg->{type} eq 'lifespan.startup') {
            $seen{unknown}  = do { local $@; eval { await $send->({ type => 'lifespan.startup.done' }) }; $@ };
            $seen{early_shutdown} = do { local $@; eval { await $send->({ type => 'lifespan.shutdown.complete' }) }; $@ };
            $seen{bad_message} = do { local $@; eval { await $send->({ type => 'lifespan.startup.failed', message => {} }) }; $@ };
            await $send->({ type => 'lifespan.startup.complete' });
            $seen{late_startup} = do { local $@; eval { await $send->({ type => 'lifespan.startup.complete' }) }; $@ };
        }
        elsif ($msg->{type} eq 'lifespan.shutdown') {
            await $send->({ type => 'lifespan.shutdown.complete' });
            return;
        }
    }
};

my $loop = IO::Async::Loop->new;
my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
$loop->add($server);
$server->listen->get;
ok( $server->is_running, 'server started despite the app probing invalid lifespan sends' );
$server->shutdown->get;

like( $seen{unknown},        qr/Unrecognized event type .* for lifespan protocol/, 'unknown lifespan type failed the Future' );
like( $seen{early_shutdown}, qr/during lifespan phase 'startup_pending'/,          'shutdown result during startup failed' );
like( $seen{bad_message},    qr/'message' must be a string/,                        'ref message failed' );
like( $seen{late_startup},   qr/during lifespan phase 'running'/,                   'duplicate startup.complete failed' );

done_testing;
