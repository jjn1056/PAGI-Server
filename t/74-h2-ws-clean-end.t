use strict;
use warnings;
use Test2::V0;
use FindBin;
use lib "$FindBin::Bin/../lib";

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# ============================================================
# Test: clean-end classification for an h2 WebSocket stream
# ============================================================
# Www.pod "Meaning per scope" splits every websocket ending in two. A CLEAN
# end is the accepted socket completing a closing handshake started by either
# side, or the server finishing its output of a refusal. Everything else --
# including "a server-detected protocol violation, timeout, or transport loss
# before a clean end" -- is ABNORMAL with the applicable standard token.
#
# _h2_ws_clean_end is the one place that decision is made for HTTP/2: both
# _h2_on_close and the dispatch wrapper ask it, and its answer picks
# _mark_complete over _mark_disconnected, which is Server Requirement 3
# ("MUST fire on_disconnect callbacks ONLY on abnormal disconnect, and
# on_complete callbacks ONLY on successful completion").
#
# The integration tests cannot pin the abnormal half. Every server-detected
# protocol close marks its stream's connection_state directly, before it
# enqueues anything, and _mark_disconnected is idempotent -- so the classifier
# could answer "clean" for all of them and no end-to-end assertion would move.
# (Measured: mutating this function to `return 1` left the whole suite green.)
# It is a pure function of stream fields, so it is tested as one, plus one
# behavioural case driving _h2_on_close on an unmarked object -- the only
# shape where the classifier's verdict is what reaches the application.

use IO::Async::Loop;
use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::ConnectionState;

my $loop = IO::Async::Loop->new;

# ============================================================
# The classifier itself
# ============================================================

my @cases = (
    {
        name   => 'application sent websocket.close on an accepted socket',
        stream => { seq_state => 'closed' },
        clean  => 1,
    },
    {
        name   => 'peer Close frame validated on an accepted socket',
        stream => { seq_state => 'accepted', ws_peer_closed => 1 },
        clean  => 1,
    },
    {
        name   => 'server-detected protocol violation closed the socket',
        stream => { seq_state => 'accepted',
                    end_reason => 'protocol_error',
                    end_detail => 'RSV bits must be 0' },
        clean  => 0,
    },
    {
        name   => 'bounded inbound queue overflowed and the server closed',
        stream => { seq_state => 'accepted',
                    end_reason => 'queue_overflow',
                    end_detail => 'inbound message queue at 100' },
        clean  => 0,
    },
    {
        name   => 'accepted socket with no close of any kind',
        stream => { seq_state => 'accepted' },
        clean  => 0,
    },
    {
        name   => 'refusal carried to completion',
        stream => { seq_state => 'refusal_complete' },
        clean  => 1,
    },
    {
        name   => 'refusal started but never finished',
        stream => { seq_state => 'refusing' },
        clean  => 0,
    },
    {
        # The send state alone now says whether the handshake completed:
        # advance_websocket rejects websocket.close before accept, so 'closed'
        # is reachable only from 'accepted' and no separate accept flag is
        # kept. A scope still in the handshake is never a clean end.
        name   => 'still connecting',
        stream => { seq_state => 'connecting' },
        clean  => 0,
    },
    {
        name   => 'stream that never got anywhere',
        stream => {},
        clean  => 0,
    },
);

subtest 'clean-end classification, one case per ending' => sub {
    for my $c (@cases) {
        is(PAGI::Server::Connection::_h2_ws_clean_end($c->{stream}) ? 1 : 0,
            $c->{clean}, "$c->{name}: " . ($c->{clean} ? 'clean' : 'abnormal'));
    }
};

subtest 'the classifier reads the stream, not the object' => sub {
    # Both halves of a clean end must be sufficient on their own, and neither
    # may be confused for the other.
    ok(PAGI::Server::Connection::_h2_ws_clean_end(
        { seq_state => 'closed' }), 'send half alone is enough');
    ok(PAGI::Server::Connection::_h2_ws_clean_end(
        { seq_state => 'accepted', ws_peer_closed => 1 }),
        'receive half alone is enough');
    ok(!PAGI::Server::Connection::_h2_ws_clean_end(
        { seq_state => 'accepted', close_received => 1 }),
        'close_received is NOT the receive half: it is set before the frame validates');
};

# ============================================================
# The verdict reaching an application
# ============================================================
# _h2_on_close is where the classifier's answer becomes on_complete or
# on_disconnect. Driving it on a stream whose connection_state has not already
# been marked is the only way to observe that: in the live server the nine
# violation sites mark first, which is why the suite could not see this.

sub drive_h2_on_close {
    my (%stream_fields) = @_;

    # _h2_on_close defers its h2_streams delete through the server's loop, so
    # the connection needs a real one. Nothing here listens or accepts.
    my $server = PAGI::Server->new(app => sub { }, host => '127.0.0.1', port => 0,
                                   quiet => 1, access_log => undef);
    $loop->add($server);
    my $conn = PAGI::Server::Connection->new(app => sub { }, protocol => undef,
                                             server => $server);
    my %seen;
    my $cs = PAGI::Server::ConnectionState->new;
    $cs->on_complete(sub { $seen{complete}++ });
    $cs->on_disconnect(sub { $seen{disconnect} = [@_] });

    $conn->{h2_streams}{9} = {
        is_websocket     => 1,
        receive_queue    => [],
        connection_state => $cs,
        %stream_fields,
    };
    # error_code 0: the stream ended with no h2-level error, so the
    # classifier alone decides between complete and disconnected.
    $conn->_h2_on_close(9, 0);

    my $result = { seen => \%seen, cs => $cs,
                   queue => $conn->{h2_streams}{9}{receive_queue} };
    $loop->remove($server);
    return $result;
}

subtest 'a server protocol close on an unmarked object ends abnormally' => sub {
    my $got = drive_h2_on_close(
        seq_state           => 'accepted',
        end_reason => 'protocol_error',
        end_detail => 'RSV bits must be 0',
    );

    ok(!$got->{seen}{complete}, 'on_complete did not fire');
    is($got->{seen}{disconnect}[0], 'protocol_error', 'on_disconnect fired with protocol_error');
    is($got->{seen}{disconnect}[1], 'RSV bits must be 0', 'and carried the detail');
    is($got->{cs}->disconnect_reason, 'protocol_error', 'object reports protocol_error');
    is($got->{cs}->response_complete, 0, 'response_complete stays false after an abnormal end');
};

subtest 'a completed closing handshake on an unmarked object ends cleanly' => sub {
    my $got = drive_h2_on_close(seq_state => 'closed');

    is($got->{seen}{complete}, 1, 'on_complete fired once');
    ok(!$got->{seen}{disconnect}, 'on_disconnect did not fire');
    is($got->{cs}->disconnect_reason, undef, 'no disconnect reason after a clean end');
    is($got->{cs}->response_complete, 1, 'response_complete is true');
};

subtest 'a completed refusal on an unmarked object ends cleanly and delivers no event' => sub {
    my $got = drive_h2_on_close(seq_state => 'refusal_complete');

    is($got->{seen}{complete}, 1, 'on_complete fired once');
    ok(!$got->{seen}{disconnect}, 'on_disconnect did not fire');
    is($got->{queue}, [], 'no websocket.disconnect queued for a completed refusal');
};

done_testing;
