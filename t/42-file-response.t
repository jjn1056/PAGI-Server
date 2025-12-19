#!/usr/bin/env perl
use strict;
use warnings;
use experimental 'signatures';
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use File::Temp qw(tempfile tempdir);
use Future::AsyncAwait;

use PAGI::Server;

# Create test files
my $tempdir = tempdir(CLEANUP => 1);
my $test_content = "Hello from file response!\n" x 100;  # ~2.7KB
my $test_file = "$tempdir/test.txt";
open my $fh, '>:raw', $test_file or die "Cannot create test file: $!";
print $fh $test_content;
close $fh;

my $binary_content = pack("C*", 0..255) x 10;  # 2560 bytes
my $binary_file = "$tempdir/binary.bin";
open $fh, '>:raw', $binary_file or die;
print $fh $binary_content;
close $fh;

subtest 'file response sends full file' => sub {
    my $loop = IO::Async::Loop->new;

    my $server = PAGI::Server->new(
        app => async sub ($scope, $receive, $send) {
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                    ['content-length', length($test_content)],
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $test_file,
                more => 0,
            });
        },
        host => '127.0.0.1',
        port => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto => 'tcp',
    ) or die "Cannot connect: $!";

    print $sock "GET /test.txt HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    $loop->loop_once(0.1);

    my $response = '';
    while (my $line = <$sock>) {
        $response .= $line;
    }
    close $sock;

    like($response, qr/HTTP\/1\.1 200/, 'got 200 response');
    like($response, qr/\Q$test_content\E/, 'file content received');

    $server->shutdown->get;
};

subtest 'file response with offset and length (range)' => sub {
    my $loop = IO::Async::Loop->new;

    my $offset = 100;
    my $length = 500;
    my $expected = substr($test_content, $offset, $length);

    my $server = PAGI::Server->new(
        app => async sub ($scope, $receive, $send) {
            await $send->({
                type => 'http.response.start',
                status => 206,
                headers => [
                    ['content-type', 'text/plain'],
                    ['content-length', $length],
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $test_file,
                offset => $offset,
                length => $length,
                more => 0,
            });
        },
        host => '127.0.0.1',
        port => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto => 'tcp',
    ) or die;

    print $sock "GET /test.txt HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    $loop->loop_once(0.1);

    my $response = '';
    while (my $line = <$sock>) {
        $response .= $line;
    }
    close $sock;

    like($response, qr/HTTP\/1\.1 206/, 'got 206 response');
    like($response, qr/\Q$expected\E/, 'partial content received');

    $server->shutdown->get;
};

subtest 'fh response sends from filehandle' => sub {
    my $loop = IO::Async::Loop->new;

    my $server = PAGI::Server->new(
        app => async sub ($scope, $receive, $send) {
            open my $fh, '<:raw', $test_file or die "Cannot open: $!";

            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                    ['content-length', length($test_content)],
                ],
            });
            await $send->({
                type => 'http.response.body',
                fh => $fh,
                length => length($test_content),
                more => 0,
            });

            close $fh;
        },
        host => '127.0.0.1',
        port => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto => 'tcp',
    ) or die;

    print $sock "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    $loop->loop_once(0.1);

    my $response = '';
    while (my $line = <$sock>) {
        $response .= $line;
    }
    close $sock;

    like($response, qr/HTTP\/1\.1 200/, 'got 200 response');
    like($response, qr/\Q$test_content\E/, 'filehandle content received');

    $server->shutdown->get;
};

subtest 'binary file response preserves bytes' => sub {
    my $loop = IO::Async::Loop->new;

    my $server = PAGI::Server->new(
        app => async sub ($scope, $receive, $send) {
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'application/octet-stream'],
                    ['content-length', length($binary_content)],
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $binary_file,
                more => 0,
            });
        },
        host => '127.0.0.1',
        port => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto => 'tcp',
    ) or die;

    print $sock "GET /binary.bin HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    $loop->loop_once(0.2);

    local $/;
    my $response = <$sock>;
    close $sock;

    my ($headers, $body) = split /\r\n\r\n/, $response, 2;
    is(length($body), length($binary_content), 'binary length matches');
    is($body, $binary_content, 'binary content matches');

    $server->shutdown->get;
};

subtest 'file not found returns error' => sub {
    my $loop = IO::Async::Loop->new;
    my $error_logged = 0;

    my $server = PAGI::Server->new(
        app => async sub ($scope, $receive, $send) {
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [],
            });
            eval {
                await $send->({
                    type => 'http.response.body',
                    file => '/nonexistent/file.txt',
                    more => 0,
                });
            };
            $error_logged = 1 if $@;
        },
        host => '127.0.0.1',
        port => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto => 'tcp',
    ) or die;

    print $sock "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    $loop->loop_once(0.1);
    close $sock;

    ok($server->is_running, 'server still running after file error');

    $server->shutdown->get;
};

done_testing;
