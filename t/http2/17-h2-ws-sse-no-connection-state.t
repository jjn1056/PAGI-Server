use strict;
use warnings;
use Test2::V0;
use FindBin;
use lib "$FindBin::Bin/../../lib";

use PAGI::Server::Connection;

# SYNC D1: pagi.connection is universal (Www 0.6). Reversed under design
# decision D1: Www.pod 0.6 makes pagi.connection a MUST on every http,
# websocket, and sse scope (see "Connection State" / "Connection Object
# Interface"), superseding the earlier "HTTP-only, NOT APPLICABLE to
# websocket/sse" reading this file asserted (SYNC B1). These builders are pure
# functions of $self + the per-stream state, so we can call them directly with
# a minimal fake connection (no nghttp2 handshake needed).

# Minimal duck-typed Connection: the builders only read these fields and call
# _get_scheme / _get_ws_scheme / _get_extensions_for_scope (all trivial).
sub fake_conn {
    return bless {
        tls_enabled => 0,
        extensions  => {},
        client_host => '127.0.0.1',
        client_port => 54321,
        server_host => '127.0.0.1',
        server_port => 8080,
        state       => {},
    }, 'PAGI::Server::Connection';
}

my $stream_state = {
    pseudo => {
        ':path'      => '/stream',
        ':method'    => 'GET',
        ':scheme'    => 'http',
        ':authority' => 'localhost',
    },
    headers => [],
};

my @required_methods = qw(
    is_connected disconnect_reason disconnect_detail on_disconnect on_complete
    disconnect_future response_started response_complete abort
);

subtest 'HTTP/2 websocket scope carries a full pagi.connection (D1)' => sub {
    my $scope = fake_conn()->_h2_create_websocket_scope(1, $stream_state);
    is($scope->{type}, 'websocket', 'built a websocket scope');
    ok(exists $scope->{'pagi.connection'},
        'websocket scope MUST carry pagi.connection (Www 0.6 Connection State)');
    my $conn = $scope->{'pagi.connection'};
    is([grep { !$conn->can($_) } @required_methods], [],
        'all nine required methods present');
};

subtest 'HTTP/2 sse scope carries a full pagi.connection (D1)' => sub {
    my $scope = fake_conn()->_h2_create_sse_scope(1, $stream_state);
    is($scope->{type}, 'sse', 'built an sse scope');
    ok(exists $scope->{'pagi.connection'},
        'sse scope MUST carry pagi.connection (Www 0.6 Connection State)');
    my $conn = $scope->{'pagi.connection'};
    is([grep { !$conn->can($_) } @required_methods], [],
        'all nine required methods present');
};

subtest 'HTTP/2 http scope still carries pagi.connection (control)' => sub {
    my $scope = fake_conn()->_h2_create_scope(1, $stream_state);
    is($scope->{type}, 'http', 'built an http scope');
    ok(exists $scope->{'pagi.connection'},
        'http scope MUST still provide pagi.connection');
};

done_testing;
