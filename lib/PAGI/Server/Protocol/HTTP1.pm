package PAGI::Server::Protocol::HTTP1;
use strict;
use warnings;
use experimental 'signatures';
use HTTP::Parser::XS qw(parse_http_request);
use URI::Escape qw(uri_unescape);

our $VERSION = '0.001';

=head1 NAME

PAGI::Server::Protocol::HTTP1 - HTTP/1.1 protocol handler

=head1 SYNOPSIS

    use PAGI::Server::Protocol::HTTP1;

    my $proto = PAGI::Server::Protocol::HTTP1->new;

    # Parse incoming request
    my ($request, $consumed) = $proto->parse_request($buffer);

    # Serialize response
    my $bytes = $proto->serialize_response_start(200, \@headers, $chunked);
    $bytes   .= $proto->serialize_response_body($chunk, $more);

=head1 DESCRIPTION

PAGI::Server::Protocol::HTTP1 isolates HTTP/1.1 wire-format parsing and
serialization from PAGI event handling. This allows clean separation of
protocol handling and future addition of HTTP/2 or HTTP/3 modules with
the same interface.

=head1 METHODS

=head2 new

    my $proto = PAGI::Server::Protocol::HTTP1->new;

Creates a new HTTP1 protocol handler.

=head2 parse_request

    my ($request_info, $bytes_consumed) = $proto->parse_request($buffer);

Parses an HTTP request from the buffer. Returns undef if the request
is incomplete. On success, returns:

    $request_info = {
        method       => 'GET',
        path         => '/foo',
        raw_path     => '/foo%20bar',
        query_string => 'a=1',
        http_version => '1.1',
        headers      => [ ['host', 'localhost'], ... ],
        content_length => 0,  # or undef if not present
        chunked      => 0,    # 1 if Transfer-Encoding: chunked
    };

=head2 serialize_response_start

    my $bytes = $proto->serialize_response_start($status, \@headers, $chunked);

Serializes the response line and headers.

=head2 serialize_response_body

    my $bytes = $proto->serialize_response_body($chunk, $more, $chunked);

Serializes a body chunk. Uses chunked encoding if $chunked is true.

=head2 serialize_trailers

    my $bytes = $proto->serialize_trailers(\@headers);

Serializes HTTP trailers.

=cut

# HTTP status code reason phrases
my %STATUS_PHRASES = (
    100 => 'Continue',
    101 => 'Switching Protocols',
    200 => 'OK',
    201 => 'Created',
    204 => 'No Content',
    301 => 'Moved Permanently',
    302 => 'Found',
    304 => 'Not Modified',
    400 => 'Bad Request',
    401 => 'Unauthorized',
    403 => 'Forbidden',
    404 => 'Not Found',
    405 => 'Method Not Allowed',
    413 => 'Payload Too Large',
    431 => 'Request Header Fields Too Large',
    500 => 'Internal Server Error',
    502 => 'Bad Gateway',
    503 => 'Service Unavailable',
);

sub new ($class, %args) {
    my $self = bless {
        max_header_size => $args{max_header_size} // 8192,
    }, $class;
    return $self;
}

sub parse_request ($self, $buffer_ref) {
    # HTTP::Parser::XS expects a scalar, not a reference
    my $buffer = ref $buffer_ref ? $$buffer_ref : $buffer_ref;

    # Check for complete headers (look for \r\n\r\n)
    my $header_end = index($buffer, "\r\n\r\n");
    return (undef, 0) if $header_end < 0;

    # Parse using HTTP::Parser::XS
    my %env;
    my $ret = parse_http_request($buffer, \%env);

    # Return undef if incomplete (-2) or error (-1)
    return (undef, 0) if $ret < 0;

    # Extract method and path
    my $method = $env{REQUEST_METHOD};
    my $raw_uri = $env{REQUEST_URI} // '/';

    # Split path and query string
    my ($raw_path, $query_string) = split(/\?/, $raw_uri, 2);
    $raw_path //= '/';
    $query_string //= '';

    # Decode path (URL-decode)
    my $path = uri_unescape($raw_path);

    # Build headers array with lowercase names
    my @headers;
    my $content_length;
    my $chunked = 0;
    my @cookie_values;

    for my $key (keys %env) {
        if ($key =~ /^HTTP_(.+)/) {
            my $header_name = lc($1);
            $header_name =~ s/_/-/g;
            my $value = $env{$key};

            # Handle Cookie header normalization
            if ($header_name eq 'cookie') {
                push @cookie_values, $value;
                next;
            }

            # Check for Transfer-Encoding: chunked
            if ($header_name eq 'transfer-encoding' && $value =~ /chunked/i) {
                $chunked = 1;
            }

            push @headers, [$header_name, $value];
        }
    }

    # Add normalized cookie header if present
    if (@cookie_values) {
        push @headers, ['cookie', join('; ', @cookie_values)];
    }

    # Add content-type and content-length from env
    if (defined $env{CONTENT_TYPE}) {
        push @headers, ['content-type', $env{CONTENT_TYPE}];
    }
    if (defined $env{CONTENT_LENGTH}) {
        push @headers, ['content-length', $env{CONTENT_LENGTH}];
        $content_length = $env{CONTENT_LENGTH} + 0;
    }

    # Determine HTTP version
    my $http_version = '1.1';
    if ($env{SERVER_PROTOCOL} && $env{SERVER_PROTOCOL} =~ m{HTTP/(\d+\.\d+)}) {
        $http_version = $1;
    }

    my $request = {
        method         => $method,
        path           => $path,
        raw_path       => $raw_path,
        query_string   => $query_string,
        http_version   => $http_version,
        headers        => \@headers,
        content_length => $content_length,
        chunked        => $chunked,
    };

    return ($request, $ret);
}

sub serialize_response_start ($self, $status, $headers, $chunked = 0) {
    my $phrase = $STATUS_PHRASES{$status} // 'Unknown';
    my $response = "HTTP/1.1 $status $phrase\r\n";

    # Add headers
    for my $header (@$headers) {
        my ($name, $value) = @$header;
        $response .= "$name: $value\r\n";
    }

    # Add Transfer-Encoding if chunked
    if ($chunked) {
        $response .= "Transfer-Encoding: chunked\r\n";
    }

    $response .= "\r\n";
    return $response;
}

sub serialize_response_body ($self, $chunk, $more, $chunked = 0) {
    return '' unless defined $chunk && length $chunk;

    if ($chunked) {
        my $len = sprintf("%x", length($chunk));
        my $body = "$len\r\n$chunk\r\n";

        # Add final chunk if no more data
        if (!$more) {
            $body .= "0\r\n\r\n";
        }

        return $body;
    } else {
        return $chunk;
    }
}

sub serialize_chunk_end ($self) {
    return "0\r\n\r\n";
}

sub serialize_trailers ($self, $headers) {
    my $trailers = '';
    for my $header (@$headers) {
        my ($name, $value) = @$header;
        $trailers .= "$name: $value\r\n";
    }
    $trailers .= "\r\n";
    return $trailers;
}

sub format_date ($self) {
    my @days = qw(Sun Mon Tue Wed Thu Fri Sat);
    my @months = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
    my @gmt = gmtime(time);
    return sprintf("%s, %02d %s %04d %02d:%02d:%02d GMT",
        $days[$gmt[6]], $gmt[3], $months[$gmt[4]], $gmt[5] + 1900,
        $gmt[2], $gmt[1], $gmt[0]);
}

=head2 parse_chunked_body

    my ($data, $bytes_consumed, $complete) = $proto->parse_chunked_body($buffer);

Parses chunked Transfer-Encoding body from the buffer. Returns:
- $data: decoded body data (may be empty string)
- $bytes_consumed: number of bytes consumed from buffer
- $complete: 1 if final chunk (0-length) was seen, 0 otherwise

Returns (undef, 0, 0) if more data is needed.

=cut

sub parse_chunked_body ($self, $buffer_ref) {
    my $buffer = ref $buffer_ref ? $$buffer_ref : $buffer_ref;
    my $data = '';
    my $total_consumed = 0;
    my $complete = 0;

    while (1) {
        # Find chunk size line
        my $crlf = index($buffer, "\r\n", $total_consumed);
        last if $crlf < 0;

        # Parse chunk size (hex)
        my $size_line = substr($buffer, $total_consumed, $crlf - $total_consumed);
        $size_line =~ s/;.*//;  # Remove chunk extensions
        my $chunk_size = hex($size_line);

        # Check if we have the full chunk + trailing CRLF
        my $chunk_start = $crlf + 2;
        my $chunk_end = $chunk_start + $chunk_size + 2;  # +2 for trailing CRLF

        if (length($buffer) < $chunk_end) {
            last;  # Need more data
        }

        # Extract chunk data
        if ($chunk_size > 0) {
            $data .= substr($buffer, $chunk_start, $chunk_size);
        }

        $total_consumed = $chunk_end;

        # Check for final chunk
        if ($chunk_size == 0) {
            $complete = 1;
            last;
        }
    }

    return ($data, $total_consumed, $complete);
}

1;

__END__

=head1 SEE ALSO

L<PAGI::Server::Connection>, L<HTTP::Parser::XS>

=head1 AUTHOR

John Napiorkowski E<lt>jjnapiork@cpan.orgE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
