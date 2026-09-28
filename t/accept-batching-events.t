use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Errno qw(EMFILE);
use PAGI::Server::Listener;

plan skip_all => 'Socket integration is not supported on Windows' if $^O eq 'MSWin32';

# A callback may stop this listener. A batch must respect that before
# attempting another accept, just as separate readiness events would.
for my $action (qw(pause close remove)) {
    subtest "callback can $action the listener" => sub {
        my $loop = IO::Async::Loop->new;
        my @streams;
        my $listener = PAGI::Server::Listener->new(on_stream => sub {
            my ($self, $stream) = @_;
            push @streams, $stream;
            $self->want_readready(0) if $action eq 'pause';
            $self->close if $action eq 'close';
            $loop->remove($self) if $action eq 'remove';
        });
        $loop->add($listener);
        $listener->listen(addr => {
            family => 'inet', socktype => 'stream', ip => '127.0.0.1', port => 0,
        }, queuesize => 16)->get;
        my @clients = map {
            IO::Socket::INET->new(PeerAddr => '127.0.0.1',
                PeerPort => $listener->read_handle->sockport,
                Proto => 'tcp', Timeout => 5) or die "connect: $!";
        } 1 .. 3;
        my $ok = eval { $listener->on_read_ready; 1 };
        ok($ok, 'callback ends this readiness dispatch without an accept error') or diag($@);
        is(scalar @streams, 1, 'remaining clients are not accepted after callback stops listener');
        $_->close_now for @streams;
        close $_ for @clients;
        $loop->remove($listener) if $listener->loop;
        $listener->close;
    };
}

{
    package Local::ErrorListener;
    use parent 'PAGI::Server::Listener';
    sub on_accept_error {
        my ($self, $socket, $errno) = @_;
        push @{$self->{errors}}, [$socket, 0 + $errno];
    }
}
subtest 'accept error and empty queue remain distinct' => sub {
    my $loop = IO::Async::Loop->new;
    my $listener = Local::ErrorListener->new(on_stream => sub {
        fail('a failed accept must not deliver a stream');
    });
    $loop->add($listener);
    $listener->listen(addr => {
        family => 'inet', socktype => 'stream', ip => '127.0.0.1', port => 0,
    })->get;
    my $socket = $listener->read_handle;
    $listener->on_read_ready;
    is($listener->{errors} // [], [], 'empty nonblocking queue does not report an error');
    {
        no strict 'refs';
        no warnings 'redefine';
        local *{ref($socket) . '::accept'} = sub { $! = EMFILE; return undef };
        $listener->on_read_ready;
    }
    is(scalar @{$listener->{errors}}, 1, 'descriptor exhaustion is delivered once');
    is($listener->{errors}[0][0], $socket, 'error includes the listening socket');
    is($listener->{errors}[0][1], 0 + EMFILE, 'error includes the original errno');
    $loop->remove($listener);
    $listener->close;
};
done_testing;
