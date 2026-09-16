#!/usr/bin/env perl
use strict;
use warnings;
use Test2::V0;
use FindBin;
use lib "$FindBin::Bin/../lib";
use IO::Async::Loop;
use IO::Socket::INET;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

use PAGI::Server;
use PAGI::Server::Connection;

# ============================================================================
# ws_close_timeout -- the public config for the bounded WebSocket
# closing-handshake wait.
# ============================================================================
# PAGI::Spec::Www mandates that the server MUST bound the wait for a peer that
# never completes the closing handshake, and that "the bound and its default
# are the server's to choose and document" (Www.pod close_timeout, L627-632).
# This is that bound. It is finite and positive by contract: unlike the idle
# timeouts, there is no "zero disables" spelling -- the closing-handshake wait
# must always be bounded, or it degrades into an unbounded wait.
#
# These tests pin the PUBLIC config surface: the default, an explicit value on
# the server, construction-time rejection of 0/negative/non-finite values,
# configure() parity, the Connection constructor default, and that the value
# actually reaches the connection through the real accept path.

sub server {
    my (%extra) = @_;
    return PAGI::Server->new(
        app        => sub { },
        host       => '127.0.0.1',
        port       => 0,
        quiet      => 1,
        access_log => undef,
        %extra,
    );
}

subtest 'default is 10 seconds' => sub {
    my $s = server();
    is($s->{ws_close_timeout}, 10, 'ws_close_timeout defaults to 10');
};

subtest 'an explicit value is stored on the server' => sub {
    my $s = server(ws_close_timeout => 3);
    is($s->{ws_close_timeout}, 3, 'ws_close_timeout => 3 is stored');
};

subtest 'zero, negative, and non-finite values are rejected at construction' => sub {
    like(dies { server(ws_close_timeout => 0) }, qr/ws_close_timeout/,
        'zero is rejected -- there is no "zero disables" for the close wait');
    like(dies { server(ws_close_timeout => -1) }, qr/ws_close_timeout/,
        'a negative value is rejected');
    like(dies { server(ws_close_timeout => 'soon') }, qr/ws_close_timeout/,
        'a non-numeric value is rejected');
    like(dies { server(ws_close_timeout => 9**9**9) }, qr/ws_close_timeout/,
        'a non-finite value (Inf) is rejected -- the bound must be finite');
};

subtest 'configure() updates and re-validates the value' => sub {
    my $s = server(ws_close_timeout => 3);
    $s->configure(ws_close_timeout => 5);
    is($s->{ws_close_timeout}, 5, 'configure() updates ws_close_timeout');
    like(dies { $s->configure(ws_close_timeout => 0) }, qr/ws_close_timeout/,
        'configure() rejects zero too');
    is($s->{ws_close_timeout}, 5, 'the rejected configure() left the prior value intact');
};

subtest 'the Connection constructor defaults and honors the option' => sub {
    my $s = server();
    my $default = PAGI::Server::Connection->new(
        app => sub { }, protocol => undef, server => $s,
    );
    is($default->{ws_close_timeout}, 10, 'Connection defaults ws_close_timeout to 10');

    my $explicit = PAGI::Server::Connection->new(
        app => sub { }, protocol => undef, server => $s,
        ws_close_timeout => 3,
    );
    is($explicit->{ws_close_timeout}, 3, 'Connection honors an explicit ws_close_timeout');
};

subtest 'the option reaches the connection through the accept path' => sub {
    my $loop = IO::Async::Loop->new;

    # Observe the real accept path: every accepted socket becomes a
    # Connection, so intercepting the constructor captures exactly the value
    # the server threaded through to it. The real constructor still runs.
    my @seen;
    my $orig = \&PAGI::Server::Connection::new;
    no warnings 'redefine';
    local *PAGI::Server::Connection::new = sub {
        my ($class, %args) = @_;
        push @seen, $args{ws_close_timeout};
        return $orig->($class, %args);
    };
    use warnings 'redefine';

    my $s = server(ws_close_timeout => 3);
    $loop->add($s);
    $s->listen->get;
    my $port = $s->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 2,
    ) or die "Cannot connect: $!";

    my $deadline = time + 10;
    $loop->loop_once(0.05) while !@seen && time < $deadline;

    ok(scalar(@seen), 'the accept path constructed a Connection');
    is($seen[0], 3, 'ws_close_timeout => 3 reached Connection->new via the accept path');

    close $sock;
    eval { $s->shutdown->get };
    eval { $loop->remove($s) };
};

done_testing;
