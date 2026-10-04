use strict;
use warnings;
use Test2::V0;

use PAGI::Server::Protocol::HTTP1;

# Every registered status an application is likely to send carries its
# reason phrase on the HTTP/1.1 status line, not "Unknown".
my $proto = PAGI::Server::Protocol::HTTP1->new;

sub status_line {
    my ($status) = @_;
    my ($line) = $proto->serialize_response_start($status, []) =~ /\A([^\r\n]*)/;
    return $line;
}

my %expected = (
    202 => 'Accepted',
    206 => 'Partial Content',
    303 => 'See Other',
    307 => 'Temporary Redirect',
    308 => 'Permanent Redirect',
    406 => 'Not Acceptable',
    409 => 'Conflict',
    410 => 'Gone',
    415 => 'Unsupported Media Type',
    416 => 'Range Not Satisfiable',
    422 => 'Unprocessable Content',
    426 => 'Upgrade Required',
    428 => 'Precondition Required',
    429 => 'Too Many Requests',
    451 => 'Unavailable For Legal Reasons',
    501 => 'Not Implemented',
    504 => 'Gateway Timeout',
    511 => 'Network Authentication Required',
);
for my $status (sort keys %expected) {
    is(status_line($status), "HTTP/1.1 $status $expected{$status}", "$status");
}

# Phrases that shipped stay as they were.
is(status_line(200), 'HTTP/1.1 200 OK', '200 unchanged');
is(status_line(413), 'HTTP/1.1 413 Payload Too Large', '413 unchanged');

# A code outside the registry still gets a status line.
is(status_line(599), 'HTTP/1.1 599 Unknown', 'an unregistered code');

done_testing;
