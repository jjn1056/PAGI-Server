#!/usr/bin/env perl
use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Net::Async::HTTP;
use File::Temp qw(tempdir);
use Future::AsyncAwait;
use Tie::Handle ();

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

{
    package TestTiedHandle;

    use parent 'Tie::Handle';
    use Symbol qw(gensym);

    sub new_handle {
        my ($class, $content_ref) = @_;

        open my $inner_fh, '<', $content_ref
            or die "Cannot open scalar handle: $!";

        my $fh = gensym();
        tie *{$fh}, $class, $inner_fh;
        return $fh;
    }

    sub TIEHANDLE {
        my ($class, $inner_fh) = @_;
        return bless { inner_fh => $inner_fh }, $class;
    }

    sub READ {
        my ($self, undef, $length, $offset) = @_;
        return read($self->{inner_fh}, $_[1], $length, $offset // 0);
    }

    sub SEEK {
        my ($self, $position, $whence) = @_;
        return seek($self->{inner_fh}, $position, $whence);
    }

    sub TELL {
        my ($self) = @_;
        return tell($self->{inner_fh});
    }

    sub FILENO {
        return -1;
    }

    sub CLOSE {
        my ($self) = @_;
        return close($self->{inner_fh});
    }
}

package main;

# Create shared event loop and HTTP client
my $loop = IO::Async::Loop->new;
my $http = Net::Async::HTTP->new;
$loop->add($http);

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

# Large file for async I/O testing
# Size is 65KB - just over the 64KB sync_file_threshold to trigger async path
my $large_content = "X" x (65 * 1024);  # 65KB to exceed sync threshold
my $large_file = "$tempdir/large.bin";
open $fh, '>:raw', $large_file or die;
print $fh $large_content;
close $fh;

# Helper to create a basic app that handles lifespan
sub make_app {
    my ($handler) = @_;

    return async sub  {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            while (1) {
                my $event = await $receive->();
                if ($event->{type} eq 'lifespan.startup') {
                    await $send->({ type => 'lifespan.startup.complete' });
                }
                elsif ($event->{type} eq 'lifespan.shutdown') {
                    await $send->({ type => 'lifespan.shutdown.complete' });
                    last;
                }
            }
            return;
        }
        await $handler->($scope, $receive, $send);
    };
}

# Helper to run a server test
sub with_server {
    my ($app, $test) = @_;

    my $server = PAGI::Server->new(
        app => make_app($app),
        host => '127.0.0.1',
        port => 0,
        quiet => 1,
    );
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    eval { $test->($port, $server) };
    my $err = $@;

    $server->shutdown->get;
    $loop->remove($server);

    die $err if $err;
}

subtest 'file response sends full file with Content-Length' => sub {
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
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
            });
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/test.txt")->get;
            is($response->code, 200, 'got 200 response');
            is($response->content, $test_content, 'file content matches');
            is(length($response->content), length($test_content), 'content length matches');
        }
    );
};

subtest 'file response with chunked encoding' => sub {
    # No Content-Length = chunked encoding
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                    # No content-length, so chunked encoding will be used
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $test_file,
            });
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/test.txt")->get;
            is($response->code, 200, 'got 200 response');
            is($response->content, $test_content, 'file content matches with chunked encoding');
        }
    );
};

subtest 'file response with offset and length (Range request simulation)' => sub {
    my $offset = 100;
    my $length = 500;
    my $expected = substr($test_content, $offset, $length);

    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            await $send->({
                type => 'http.response.start',
                status => 206,
                headers => [
                    ['content-type', 'text/plain'],
                    ['content-length', $length],
                    ['content-range', "bytes $offset-" . ($offset + $length - 1) . "/" . length($test_content)],
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $test_file,
                offset => $offset,
                length => $length,
            });
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/test.txt")->get;
            is($response->code, 206, 'got 206 Partial Content');
            is($response->content, $expected, 'partial content matches');
            is(length($response->content), $length, 'partial content length correct');
        }
    );
};

subtest 'HEAD request suppresses file body without opening the file' => sub {
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                    ['content-length', length($test_content)],
                ],
            });
            # Nonexistent path: h1 has no -f/-r pre-check (it fails at
            # open/stat -- see Task 5 report), so a clean resolve here
            # proves the file arm was never entered for a HEAD request.
            await $send->({
                type => 'http.response.body',
                file => '/nonexistent/head-file-probe.txt',
            });
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->HEAD("http://127.0.0.1:$port/")->get;
            is($response->code, 200, 'HEAD got 200');
            is($response->content, '', 'HEAD file: no body');
            is($response->header('Content-Length'), length($test_content),
                'app Content-Length passes through untouched');
        }
    );
};

subtest 'HEAD request suppresses fh body without reading/seeking the handle' => sub {
    my $tell_after;
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            open my $fh, '<:raw', $test_file or die "Cannot open: $!";

            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                    ['content-length', length($test_content)],
                ],
            });
            # Nonzero offset: if the server ever seeked/read this handle for
            # a HEAD request, $fh's position would move off 0 below.
            await $send->({
                type => 'http.response.body',
                fh => $fh,
                offset => 3,
            });
            $tell_after = tell($fh);
            close $fh;
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->HEAD("http://127.0.0.1:$port/")->get;
            is($response->code, 200, 'HEAD got 200');
            is($response->content, '', 'HEAD fh: no body');
        }
    );
    is($tell_after, 0, 'fh was never seeked/read for a HEAD request');
};

subtest 'file response offset at end of file' => sub {
    my $offset = length($test_content) - 50;
    my $expected = substr($test_content, $offset);

    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            await $send->({
                type => 'http.response.start',
                status => 206,
                headers => [
                    ['content-type', 'text/plain'],
                    ['content-length', length($expected)],
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $test_file,
                offset => $offset,
                # No length = read to EOF
            });
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/test.txt")->get;
            is($response->code, 206, 'got 206 response');
            is($response->content, $expected, 'content from offset to EOF matches');
        }
    );
};

subtest 'fh response sends from filehandle' => sub {
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
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
            });

            close $fh;
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/")->get;
            is($response->code, 200, 'got 200 response');
            is($response->content, $test_content, 'filehandle content matches');
        }
    );
};

subtest 'closed fh fails the send Future' => sub {
    # Intentionally RED until PAGI::Server implements the existing
    # PAGI::Spec::Www contract for http.response.body{fh}. A closed or invalid
    # application-owned handle must fail the Future returned by $send; treating
    # read() failure as EOF silently turns a resource error into a successful,
    # empty or truncated response. Keep this as a release-blocking assertion,
    # not a TODO test, so the server cannot ship while violating the contract.
    #
    # The first assertion also pins the asynchronous boundary: $send itself
    # returns a Future, and validation/resource errors arrive through that
    # Future rather than as a synchronous exception.
    my $sync_error = '';
    my $future_error = '';
    my $returned_future = 0;

    with_server(
        async sub {
            my ($scope, $receive, $send) = @_;

            open my $closed_fh, '<:raw', $test_file
                or die "Cannot open: $!";
            close $closed_fh;

            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'application/octet-stream'],
                ],
            });

            my $send_future;
            eval {
                $send_future = $send->({
                    type => 'http.response.body',
                    fh => $closed_fh,
                    length => 1,
                });
                1;
            } or $sync_error = $@;

            $returned_future = eval { $send_future->isa('Future') } ? 1 : 0
                if defined $send_future;

            if ($returned_future) {
                eval { await $send_future; 1 }
                    or $future_error = $@;
            }

            # Once the implementation rejects the fh send, finish the response
            # so the client side of this regression test cannot wait forever.
            if (!$returned_future || length($sync_error) || length($future_error)) {
                await $send->({
                    type => 'http.response.body',
                    body => '',
                    more => 0,
                });
            }
        },
        sub {
            my ($port, $server) = @_;
            $http->GET("http://127.0.0.1:$port/closed-fh")->get;
        },
    );

    ok(
        $returned_future && !length($sync_error),
        '$send returns a Future instead of throwing synchronously',
    );
    ok(length($future_error), 'closed fh fails the Future returned by $send');
    like($future_error, qr/^Failed to read filehandle: /,
        'error names the failure (mirrors the h2 fh-closed message, t/http2/28-file-fh.t)');
    unlike($future_error, qr/ at .+ line \d+\.?\s*\z/,
        'die message ends with a trailing newline, so Perl does not append " at FILE line N." (matches h2\'s twin die at Connection.pm ~1660)');
};

subtest 'fh response without length reads to EOF' => sub {
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
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
            });

            close $fh;
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/")->get;
            is($response->code, 200, 'got 200 response');
            is($response->content, $test_content, 'filehandle content matches without explicit length');
        }
    );
};

subtest 'fh response without length streams chunked to EOF' => sub {
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            open my $fh, '<:raw', $test_file or die "Cannot open: $!";

            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                ],
            });
            await $send->({
                type => 'http.response.body',
                fh => $fh,
            });

            close $fh;
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/")->get;
            is($response->code, 200, 'got 200 response');
            is($response->content, $test_content, 'chunked filehandle content matches without explicit length');
        }
    );
};

subtest 'scalar-reference fh response without length streams to EOF' => sub {
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            open my $fh, '<', \$test_content or die "Cannot open scalar handle: $!";

            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                ],
            });
            await $send->({
                type => 'http.response.body',
                fh => $fh,
            });

            close $fh;
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/")->get;
            is($response->code, 200, 'got 200 response');
            is($response->content, $test_content, 'scalar-reference filehandle content matches');
        }
    );
};

subtest 'tied fh response without length streams to EOF' => sub {
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            my $fh = TestTiedHandle->new_handle(\$test_content);

            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                ],
            });
            await $send->({
                type => 'http.response.body',
                fh => $fh,
            });

            close $fh;
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/")->get;
            is($response->code, 200, 'got 200 response');
            is($response->content, $test_content, 'tied filehandle content matches');
        }
    );
};

subtest 'fh response with offset (seek)' => sub {
    my $offset = 200;
    my $length = 300;
    my $expected = substr($test_content, $offset, $length);

    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            open my $fh, '<:raw', $test_file or die "Cannot open: $!";

            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                    ['content-length', $length],
                ],
            });
            await $send->({
                type => 'http.response.body',
                fh => $fh,
                offset => $offset,
                length => $length,
            });

            close $fh;
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/")->get;
            is($response->code, 200, 'got 200 response');
            is($response->content, $expected, 'fh with offset content matches');
        }
    );
};

subtest 'fh response offset past EOF sends zero bytes' => sub {
    # PAGI spec (Www.pod, Response Body validation): an offset past the end
    # of the file SHOULD send zero bytes, not fail the response. Mirrors
    # the file-past-EOF subtests above, but for an application-owned fh --
    # unlike the file arm, the fh arm never stats the source, so past-EOF
    # is a plain zero-byte read() at the sought position, not a clamped
    # length computation.
    with_server(
        async sub {
            my ($scope, $receive, $send) = @_;
            open my $fh, '<:raw', $test_file or die "Cannot open: $!";
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                ],
            });
            await $send->({
                type => 'http.response.body',
                fh => $fh,
                offset => 999_999_999,
            });
            close $fh;
        },
        sub {
            my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/test.txt")->get;
            is($response->code, 200, 'got 200 response for fh offset past EOF');
            is($response->content, '', 'fh offset past EOF returns empty content');
        }
    );
};

subtest 'fh response with offset on an unseekable pipe fails the send Future loudly' => sub {
    my $future_error = '';

    with_server(
        async sub {
            my ($scope, $receive, $send) = @_;

            # A pipe's read end is not seekable: a nonzero offset forces
            # _send_fh_response's seek() call, which fails with ESPIPE
            # ("Illegal seek").
            pipe(my $read_fh, my $write_fh) or die "pipe: $!";

            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                ],
            });

            eval {
                await $send->({
                    type   => 'http.response.body',
                    fh     => $read_fh,
                    offset => 1,
                });
                1;
            } or $future_error = $@;
            $future_error =~ s/\n/ /g;

            # Recover with a normal body so the client side doesn't hang --
            # mirrors the closed-fh recovery idiom above.
            await $send->({
                type => 'http.response.body',
                body => "err=$future_error",
                more => 0,
            });

            close $read_fh;
            close $write_fh;
        },
        sub {
            my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/")->get;
            is($response->code, 200, 'got 200 response');
            like($response->content, qr/err=.*Cannot seek/,
                'seek failure on an unseekable fh fails the Future loudly; app recovered');
        }
    );
};

subtest 'binary file response preserves bytes' => sub {
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
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
            });
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/binary.bin")->get;
            is($response->code, 200, 'got 200 response');
            is(length($response->content), length($binary_content), 'binary length matches');
            is($response->content, $binary_content, 'binary content matches byte-for-byte');
        }
    );
};

subtest 'large file streams correctly (tests chunking)' => sub {
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'application/octet-stream'],
                    ['content-length', length($large_content)],
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $large_file,
            });
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/large.bin")->get;
            is($response->code, 200, 'got 200 response');
            is(length($response->content), length($large_content), 'large file length matches');
            is($response->content, $large_content, 'large file content matches');
        }
    );
};

subtest 'large file with chunked encoding' => sub {
    # Force chunked encoding (worker pool path) with large file
    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'application/octet-stream'],
                    # No content-length = chunked
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $large_file,
            });
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/large.bin")->get;
            is($response->code, 200, 'got 200 response');
            is(length($response->content), length($large_content), 'large file length matches (chunked)');
            is($response->content, $large_content, 'large file content matches (chunked)');
        }
    );
};

subtest 'file not found dies with error' => sub {
    my $error_caught = 0;
    my $error_message = '';

    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            # Don't start a response until we know the file exists
            # Test that the file operation fails properly
            eval {
                # Try to stat the file first (this will fail)
                my $file = '/nonexistent/file/that/does/not/exist.txt';
                die "File not found: $file" unless -f $file;

                await $send->({
                    type => 'http.response.start',
                    status => 200,
                    headers => [['content-length', 100]],
                });
                await $send->({
                    type => 'http.response.body',
                    file => $file,
                });
            };
            if ($@) {
                $error_caught = 1;
                $error_message = $@;
                # Send proper error response
                my $body = 'File not found';
                await $send->({
                    type => 'http.response.start',
                    status => 404,
                    headers => [
                        ['content-type', 'text/plain'],
                        ['content-length', length($body)],
                    ],
                });
                await $send->({
                    type => 'http.response.body',
                    body => $body,
                    more => 0,
                });
            }
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/")->get;
            is($response->code, 404, 'got 404 for missing file');
            ok($server->is_running, 'server still running after file error');
        }
    );

    ok($error_caught, 'error was caught for nonexistent file');
    like($error_message, qr/File not found/, 'error message mentions file not found');
};

subtest 'missing file: server pre-check fails the Future, app recovers with a normal body' => sub {
    # h1 parity with h2's file arm (Connection.pm _h2_create_send): the file
    # arm must reject a missing file with "File not found: $file\n" BEFORE
    # ever reaching _send_file_response's -s/open, so a conforming app sees
    # the same failure shape on both transports and can recover cleanly
    # (mirrors the closed-fh recovery idiom above).
    my $sync_error = '';
    my $future_error = '';
    my $returned_future = 0;
    my $missing_file = "$tempdir/does-not-exist.txt";

    with_server(
        async sub {
            my ($scope, $receive, $send) = @_;

            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [['content-type', 'text/plain']],
            });

            my $send_future;
            eval {
                $send_future = $send->({
                    type => 'http.response.body',
                    file => $missing_file,
                });
                1;
            } or $sync_error = $@;

            $returned_future = eval { $send_future->isa('Future') } ? 1 : 0
                if defined $send_future;

            if ($returned_future) {
                eval { await $send_future; 1 }
                    or $future_error = $@;
            }

            # Response already started (200 sent); finish it with a plain
            # body instead of the failed file, same recovery shape the
            # closed-fh subtest above exercises.
            await $send->({
                type => 'http.response.body',
                body => "err=$future_error",
                more => 0,
            });
        },
        sub {
            my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/missing-file")->get;
            is($response->code, 200, 'response completes cleanly (app already sent 200)');
            ok($server->is_running, 'server still running after file pre-check failure');
        },
    );

    ok($returned_future && !length($sync_error),
        '$send returns a Future instead of throwing synchronously');
    ok(length($future_error), 'missing file fails the Future returned by $send');
    is($future_error, "File not found: $missing_file\n",
        'error matches h2 parity message exactly (Connection.pm _h2_create_send file arm)');
};

subtest 'unreadable file: server pre-check fails the Future, app recovers with a normal body' => sub {
    plan skip_all => 'root ignores permission bits, cannot exercise -r failure' if $> == 0;

    my $sync_error = '';
    my $future_error = '';
    my $returned_future = 0;
    my $unreadable_file = "$tempdir/unreadable.txt";
    open my $ufh, '>:raw', $unreadable_file or die "Cannot create test file: $!";
    print $ufh "secret";
    close $ufh;
    chmod 0000, $unreadable_file or die "Cannot chmod test file: $!";

    with_server(
        async sub {
            my ($scope, $receive, $send) = @_;

            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [['content-type', 'text/plain']],
            });

            my $send_future;
            eval {
                $send_future = $send->({
                    type => 'http.response.body',
                    file => $unreadable_file,
                });
                1;
            } or $sync_error = $@;

            $returned_future = eval { $send_future->isa('Future') } ? 1 : 0
                if defined $send_future;

            if ($returned_future) {
                eval { await $send_future; 1 }
                    or $future_error = $@;
            }

            await $send->({
                type => 'http.response.body',
                body => "err=$future_error",
                more => 0,
            });
        },
        sub {
            my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/unreadable-file")->get;
            is($response->code, 200, 'response completes cleanly (app already sent 200)');
            ok($server->is_running, 'server still running after file pre-check failure');
        },
    );

    chmod 0644, $unreadable_file;

    ok($returned_future && !length($sync_error),
        '$send returns a Future instead of throwing synchronously');
    ok(length($future_error), 'unreadable file fails the Future returned by $send');
    is($future_error, "Cannot read file: $unreadable_file\n",
        'error matches h2 parity message exactly (Connection.pm _h2_create_send file arm)');
};

subtest 'zero-length file works' => sub {
    my $empty_file = "$tempdir/empty.txt";
    open $fh, '>:raw', $empty_file or die;
    close $fh;

    with_server(
        async sub  {
        my ($scope, $receive, $send) = @_;
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                    ['content-length', 0],
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $empty_file,
            });
        },
        sub  {
        my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/empty.txt")->get;
            is($response->code, 200, 'got 200 response');
            is($response->content, '', 'empty file returns empty content');
            is(length($response->content), 0, 'content length is 0');
        }
    );
};

subtest 'file response offset past EOF sends zero bytes (chunked)' => sub {
    # PAGI spec (Www.pod, Response Body validation): an offset past the end
    # of the file SHOULD send zero bytes, not fail the response. No
    # content-length header here, so this exercises the chunked framing
    # path for a length-clamped-to-zero body.
    # The app alternates: past-EOF (empty) on the first request, then a
    # normal full-file response on the second -- so the "connection still
    # usable" assertion below actually distinguishes a desynced connection
    # (which would return the wrong bytes for request 2) from a healthy one.
    my $request_count = 0;
    with_server(
        async sub {
            my ($scope, $receive, $send) = @_;
            $request_count++;
            if ($request_count == 1) {
                await $send->({
                    type => 'http.response.start',
                    status => 200,
                    headers => [
                        ['content-type', 'text/plain'],
                    ],
                });
                await $send->({
                    type => 'http.response.body',
                    file => $test_file,
                    offset => 999_999_999,
                });
            }
            else {
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
                });
            }
        },
        sub {
            my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/test.txt")->get;
            is($response->code, 200, 'got 200 response for offset past EOF');
            is($response->content, '', 'offset past EOF returns empty content');

            # The connection must still be usable afterward -- a malformed
            # zero-length chunk terminator would desync the next response.
            my $second = $http->GET("http://127.0.0.1:$port/test.txt")->get;
            is($second->code, 200, 'connection still usable for a follow-up request');
            is($second->content, $test_content, 'follow-up request gets normal content');
        }
    );
};

subtest 'file response offset past EOF sends zero bytes (content-length)' => sub {
    # Same rule, but with an explicit Content-Length: 0 header, exercising
    # the non-chunked (sync fast-path) framing.
    with_server(
        async sub {
            my ($scope, $receive, $send) = @_;
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'text/plain'],
                    ['content-length', 0],
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $test_file,
                offset => 999_999_999,
            });
        },
        sub {
            my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/test.txt")->get;
            is($response->code, 200, 'got 200 response for offset past EOF');
            is($response->content, '', 'offset past EOF returns empty content');
        }
    );
};

# =============================================================================
# Test: Threshold boundary behavior
# =============================================================================
# These tests verify that files at exactly the threshold use sync path,
# and files just over the threshold use async path.
# Default sync_file_threshold is 64KB (65536 bytes).

subtest 'file at exact threshold uses sync path' => sub {
    # Create a file exactly at 64KB threshold
    my $threshold_content = "Y" x 65536;  # Exactly 64KB
    my $threshold_file = "$tempdir/threshold.bin";
    open my $tfh, '>:raw', $threshold_file or die;
    print $tfh $threshold_content;
    close $tfh;

    with_server(
        async sub {
            my ($scope, $receive, $send) = @_;
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'application/octet-stream'],
                    ['content-length', length($threshold_content)],
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $threshold_file,
            });
        },
        sub {
            my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/threshold.bin")->get;
            is($response->code, 200, 'got 200 response');
            is(length($response->content), 65536, 'threshold file length correct');
            is($response->content, $threshold_content, 'threshold file content matches');
        }
    );
};

subtest 'file just over threshold uses async path' => sub {
    # Create a file 1 byte over threshold
    my $over_threshold_content = "Z" x 65537;  # 64KB + 1 byte
    my $over_threshold_file = "$tempdir/over_threshold.bin";
    open my $ofh, '>:raw', $over_threshold_file or die;
    print $ofh $over_threshold_content;
    close $ofh;

    with_server(
        async sub {
            my ($scope, $receive, $send) = @_;
            await $send->({
                type => 'http.response.start',
                status => 200,
                headers => [
                    ['content-type', 'application/octet-stream'],
                    ['content-length', length($over_threshold_content)],
                ],
            });
            await $send->({
                type => 'http.response.body',
                file => $over_threshold_file,
            });
        },
        sub {
            my ($port, $server) = @_;
            my $response = $http->GET("http://127.0.0.1:$port/over_threshold.bin")->get;
            is($response->code, 200, 'got 200 response');
            is(length($response->content), 65537, 'over-threshold file length correct');
            is($response->content, $over_threshold_content, 'over-threshold file content matches');
        }
    );
};

done_testing;
