use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Time::HiRes qw(time);
use PAGI::Server;
use PAGI::Server::Connection;

sub server {
    return PAGI::Server->new(
        app => sub {}, host => '127.0.0.1', port => 0,
        quiet => 1, access_log => undef, @_,
    );
}

subtest 'defaults and independent overrides' => sub {
    my $s = server();
    is([@{$s}{qw(read_buffer_size write_buffer_size)}], [65536, 8192], 'defaults');
    $s = server(read_buffer_size => 16384);
    is([@{$s}{qw(read_buffer_size write_buffer_size)}], [16384, 8192], 'read only');
    $s = server(write_buffer_size => 32768);
    is([@{$s}{qw(read_buffer_size write_buffer_size)}], [65536, 32768], 'write only');
    $s = server(read_buffer_size => undef, write_buffer_size => undef);
    is([@{$s}{qw(read_buffer_size write_buffer_size)}], [65536, 8192], 'undefined constructor defaults');
    for my $name (qw(read_buffer_size write_buffer_size)) {
        for my $value (1, '16384') {
            my $configured = server($name => $value);
            is($configured->{$name}, $value, "$name accepts a positive integer");
        }
    }
};

subtest 'configure updates only supplied fields' => sub {
    my $s = server();
    $s->configure(read_buffer_size => 16384);
    is([@{$s}{qw(read_buffer_size write_buffer_size)}], [16384, 8192], 'read updated');
    $s->configure(write_buffer_size => 32768);
    is([@{$s}{qw(read_buffer_size write_buffer_size)}], [16384, 32768], 'write updated');
};

subtest 'invalid byte counts fail without corrupting the configured field' => sub {
    my $s = server();
    for my $name (qw(read_buffer_size write_buffer_size)) {
        for my $bad (0, -1, 1.5, '', '64k', 'NaN', 'Inf', []) {
            like(dies { server($name => $bad) }, qr/\Q$name\E.*positive integer/, "$name rejects invalid constructor value");
            my $before = $s->{$name};
            like(dies { $s->configure($name => $bad) }, qr/\Q$name\E.*positive integer/, "$name rejects invalid configuration");
            is($s->{$name}, $before, 'prior field retained');
        }
        like(dies { $s->configure($name => undef) }, qr/\Q$name\E.*positive integer/, "$name rejects explicit undefined configure value");
    }
};

subtest 'internal Connection constructor keeps matching defaults' => sub {
    my $default = PAGI::Server::Connection->new(app => sub {});
    is([@{$default}{qw(read_buffer_size write_buffer_size)}], [65536, 8192], 'direct construction defaults');
    my $custom = PAGI::Server::Connection->new(
        app => sub {}, read_buffer_size => 16384, write_buffer_size => 32768,
    );
    is([@{$custom}{qw(read_buffer_size write_buffer_size)}], [16384, 32768], 'direct construction overrides');
};

subtest 'accepted connections configure the actual stream' => sub {
    plan skip_all => 'Socket integration unavailable on Windows' if $^O eq 'MSWin32';
    for my $case (
        [{}, [65536, 8192]],
        [{read_buffer_size => 16384, write_buffer_size => 32768}, [16384, 32768]],
    ) {
        my ($options, $expected) = @$case;
        my @seen;
        my $original = \&IO::Async::Stream::configure;
        no warnings 'redefine';
        local *IO::Async::Stream::configure = sub {
            my ($stream, %args) = @_;
            push @seen, [@args{qw(read_len write_len)}]
                if exists $args{read_len} || exists $args{write_len};
            return $original->($stream, %args);
        };

        my $loop = IO::Async::Loop->new;
        my $s = server(%$options);
        $loop->add($s);
        $s->listen->get;
        my $socket = IO::Socket::INET->new(
            PeerAddr => '127.0.0.1', PeerPort => $s->port,
            Proto => 'tcp', Timeout => 2,
        ) or die "Cannot connect: $!";
        my $deadline = time + 2;
        $loop->loop_once(0.05) while !@seen && time < $deadline;
        is($seen[0], $expected, 'sizes reached the real stream configure call');
        close $socket;
        $s->shutdown->get;
        $loop->remove($s);
    }
};

done_testing;
