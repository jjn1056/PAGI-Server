package PAGI::Server::Protocol::HTTP1;
use strict;
use warnings;

our $VERSION = '0.002014';

use URI::Escape qw(uri_unescape);
use Encode qw(decode);
use PAGI::Server ();
use PAGI::Server::EventValidator ();


=encoding utf8

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

    my $proto = PAGI::Server::Protocol::HTTP1->new(%options);

Creates a new HTTP1 protocol handler. Accepts the following options, which
bound resource use while parsing untrusted input:

=over 4

=item * C<max_header_size> - maximum size in bytes of the combined header
block. Default: 8192 (8KB). Exceeding it yields a 431 error result, as soon
as the bytes held already exceed it, before the head is complete.

=item * C<max_request_line_size> - maximum size in bytes of the request line.
Default: 8192 (8KB). Exceeding it yields a 414 error result, likewise as
soon as it is exceeded.

=item * C<max_header_count> - maximum number of header fields. Default: 100.
Exceeding it yields a 431 error result.

=item * C<max_chunk_size> - maximum size in bytes of a single chunk in a
chunked request body. Default: 10_485_760 (10MB).

=back

=head2 parse_request

    my ($request_info, $bytes_consumed) = $proto->parse_request($buffer);

Parses an HTTP request from the buffer. The first return value is one of three
things:

=over 4

=item * C<undef> when the request is incomplete (more bytes needed). The
second value is 0.

=item * an B<error descriptor> when the request is malformed or exceeds a
limit:

    { error => 400, message => 'Bad Request' }

The C<error> key is the HTTP status code to send (400, 413, 414, 431, or 501);
C<message> is a short reason. The second return value is the number of bytes
to discard.

=item * a B<request hash> on success:

    $request_info = {
        method          => 'GET',
        path            => '/foo',
        raw_path        => '/foo%20bar',
        query_string    => 'a=1',
        http_version    => '1.1',
        headers         => [ ['host', 'localhost'], ... ],
        content_length  => 0,     # or undef if no Content-Length header
        chunked         => 0,     # 1 if Transfer-Encoding: chunked
        expect_continue => 0,     # 1 if the request sent Expect: 100-continue
    };

=back

=head2 serialize_response_start

    my $bytes = $proto->serialize_response_start($status, \@headers, $chunked, $http_version);

Serializes the status line and headers. C<$chunked> (default 0) adds a
C<Transfer-Encoding: chunked> header, but only when C<$http_version> (default
C<'1.1'>) is C<'1.1'> — chunked encoding is not emitted for HTTP/1.0
responses. A default C<Server> header is added when the application does not
provide one.

=head2 serialize_response_body

    my $bytes = $proto->serialize_response_body($chunk, $more, $chunked);

Serializes a body chunk. Uses chunked encoding if $chunked is true.

=head2 serialize_chunk_end

    my $bytes = $proto->serialize_chunk_end;

Returns the terminating zero-length chunk (C<"0\r\n\r\n">) that ends a
chunked response body.

=head2 serialize_continue

    my $bytes = $proto->serialize_continue;

Returns a C<HTTP/1.1 100 Continue> interim response, sent in reply to a
request that carried C<Expect: 100-continue>.

=head2 serialize_trailers

    my $bytes = $proto->serialize_trailers(\@headers);

Serializes HTTP trailers.

=head2 format_date

    my $date = $proto->format_date;

Returns the current time formatted as an RFC 7231 IMF-fixdate string suitable
for a C<Date> header (e.g. C<"Sun, 06 Nov 1994 08:49:37 GMT">). The value is
cached and regenerated at most once per second.

=cut

# Cached Date header (regenerated at most once per second)
my $_cached_date;
my $_cached_date_time = 0;

# Cached default Server header (lazy-init to ensure VERSION is loaded)
my $_server_header;

# HTTP status code reason phrases: the IANA HTTP Status Code Registry, in
# RFC 9110's wording, except 413, which keeps the name it has always had
# here. A code outside the registry gets 'Unknown'.
my %STATUS_PHRASES = (
    100 => 'Continue',
    101 => 'Switching Protocols',
    102 => 'Processing',
    103 => 'Early Hints',
    200 => 'OK',
    201 => 'Created',
    202 => 'Accepted',
    203 => 'Non-Authoritative Information',
    204 => 'No Content',
    205 => 'Reset Content',
    206 => 'Partial Content',
    207 => 'Multi-Status',
    208 => 'Already Reported',
    226 => 'IM Used',
    300 => 'Multiple Choices',
    301 => 'Moved Permanently',
    302 => 'Found',
    303 => 'See Other',
    304 => 'Not Modified',
    305 => 'Use Proxy',
    307 => 'Temporary Redirect',
    308 => 'Permanent Redirect',
    400 => 'Bad Request',
    401 => 'Unauthorized',
    402 => 'Payment Required',
    403 => 'Forbidden',
    404 => 'Not Found',
    405 => 'Method Not Allowed',
    406 => 'Not Acceptable',
    407 => 'Proxy Authentication Required',
    408 => 'Request Timeout',
    409 => 'Conflict',
    410 => 'Gone',
    411 => 'Length Required',
    412 => 'Precondition Failed',
    413 => 'Payload Too Large',
    414 => 'URI Too Long',
    415 => 'Unsupported Media Type',
    416 => 'Range Not Satisfiable',
    417 => 'Expectation Failed',
    421 => 'Misdirected Request',
    422 => 'Unprocessable Content',
    423 => 'Locked',
    424 => 'Failed Dependency',
    425 => 'Too Early',
    426 => 'Upgrade Required',
    428 => 'Precondition Required',
    429 => 'Too Many Requests',
    431 => 'Request Header Fields Too Large',
    451 => 'Unavailable For Legal Reasons',
    500 => 'Internal Server Error',
    501 => 'Not Implemented',
    502 => 'Bad Gateway',
    503 => 'Service Unavailable',
    504 => 'Gateway Timeout',
    505 => 'HTTP Version Not Supported',
    506 => 'Variant Also Negotiates',
    507 => 'Insufficient Storage',
    508 => 'Loop Detected',
    511 => 'Network Authentication Required',
);

sub new {
    my ($class, %args) = @_;

    my $self = bless {
        max_header_size       => $args{max_header_size} // 8192,
        max_request_line_size => $args{max_request_line_size} // 8192,  # 8KB per RFC 7230
        max_header_count      => $args{max_header_count} // 100,  # Max number of headers
        max_chunk_size        => $args{max_chunk_size} // 10_485_760,  # 10MB default
    }, $class;
    return $self;
}

# Whether an absolute-form target's authority names the same host as the
# Host header, ignoring case and the scheme's default port. No Host header
# (HTTP/1.0) is no conflict.
sub _same_authority {
    my ($scheme, $authority, $host) = @_;
    return 1 unless defined $host;
    my $default_port = $scheme eq 'https' ? 443 : 80;
    my $normal = sub { my $value = lc shift; $value =~ s/:\Q$default_port\E\z//; $value };
    return $normal->($authority) eq $normal->($host);
}

# request-line = method SP request-target SP HTTP-version (RFC 9112 3), for
# HTTP/1.0 and HTTP/1.1. A method is a token; a target has no whitespace or
# control characters.
my $TOKEN = qr{[!#\$%&'*+.^_`|~0-9A-Za-z-]+};
my $REQUEST_LINE = qr{\A($TOKEN) ([^\x00-\x20\x7f]+) HTTP/1\.([01])\z};

# The value of the single Host field, or undef.
sub _host_of {
    my ($fields) = @_;
    for my $field (@$fields) { return $field->[1] if $field->[0] eq 'host' }
    return undef;
}

# A head still arriving: refused as soon as it is already past a size limit
# -- the limits bound what a connection buffers, not only what it parses --
# or uses a bare LF line ending, which this server does not accept (RFC 9112
# 2.2 allows refusing it; leniency in finding the end of a head is what
# request smuggling feeds on). Otherwise (undef, 0): wait for more bytes.
# Only an incomplete head pays for these checks.
sub _incomplete_head {
    my ($self, $buffer) = @_;
    my $length = length $buffer;
    my $line_end = index($buffer, "\r\n");
    if (($line_end < 0 ? $length : $line_end) > $self->{max_request_line_size}) {
        return ({ error => 414, message => 'URI Too Long' }, $length);
    }
    # The end of the head cannot start before the last three bytes held.
    if ($length - 3 > $self->{max_header_size}) {
        return ({ error => 431, message => 'Request Header Fields Too Large' }, $length);
    }
    for (my $lf = index($buffer, "\n"); $lf >= 0; $lf = index($buffer, "\n", $lf + 1)) {
        return ({ error => 400, message => 'Bad Request' }, $length)
            if $lf == 0 || substr($buffer, $lf - 1, 1) ne "\r";
    }
    return (undef, 0);
}

sub parse_request {
    my ($self, $buffer_ref) = @_;

    my $buffer = ref $buffer_ref ? $$buffer_ref : $buffer_ref;

    # Check for complete headers (look for \r\n\r\n)
    my $header_end = index($buffer, "\r\n\r\n");
    return $self->_incomplete_head($buffer) if $header_end < 0;

    # Check request line length (first line before \r\n)
    my $first_line_end = index($buffer, "\r\n");
    if ($first_line_end > $self->{max_request_line_size}) {
        return ({ error => 414, message => 'URI Too Long' }, $header_end + 4);
    }

    # Check max header size
    if ($header_end > $self->{max_header_size}) {
        return ({ error => 431, message => 'Request Header Fields Too Large' }, $header_end + 4);
    }

    my $ret = $header_end + 4;
    my $bad = { error => 400, message => 'Bad Request' };

    # The head as sent: the request line, then one field per line.
    my @lines = split /\r\n/, substr($buffer, 0, $header_end);
    my ($method, $raw_uri, $minor) = (shift(@lines) // '') =~ $REQUEST_LINE
        or return ($bad, $ret);
    my $http_version = "1.$minor";

    # Field lines in order, names lowercased and otherwise as sent: a name
    # that is not a token (whitespace before the colon, say) is refused, not
    # repaired (RFC 9112 5.1). An obs-fold continuation joins the previous
    # value with a space (RFC 9112 5.2).
    # (String operations rather than one regex per line: this loop runs for
    # every header of every request.)
    my @fields;
    for my $line (@lines) {
        # A field value holds no control character but HTAB (RFC 9110 5.5).
        return ($bad, $ret) if $line =~ tr/\x00-\x08\x0a-\x1f\x7f//;
        my $first = substr($line, 0, 1);
        if ($first eq ' ' || $first eq "\t") {
            return ($bad, $ret) unless @fields;
            $line =~ s/\A[ \t]+//;
            $line =~ s/[ \t]+\z//;
            $fields[-1][1] .= " $line";
            next;
        }
        my $colon = index($line, ':');
        return ($bad, $ret) if $colon < 1;
        my $name = substr($line, 0, $colon);
        return ($bad, $ret) if $name =~ tr/!#$%&'*+.^_`|~0-9A-Za-z-//c;
        $name = lc $name;
        # The value without the optional whitespace around it.
        my ($start, $end) = ($colon + 1, length $line);
        $start++ while $start < $end && (substr($line, $start, 1) eq ' ' || substr($line, $start, 1) eq "\t");
        if ($end > $start && (substr($line, $end - 1, 1) eq ' ' || substr($line, $end - 1, 1) eq "\t")) {
            # Content-Length is 1*DIGIT with nothing after it, as this server
            # has always required, even trailing whitespace.
            return ($bad, $ret) if $name eq 'content-length';
            $end-- while $end > $start && (substr($line, $end - 1, 1) eq ' ' || substr($line, $end - 1, 1) eq "\t");
        }
        push @fields, [$name, substr($line, $start, $end - $start)];
    }

    # RFC 9112 3.2: an origin server accepts origin-form, absolute-form --
    # reduced here to its path and query, and refused when its authority
    # differs from Host -- and asterisk-form for OPTIONS. Anything else is
    # 400, so path and raw_path always begin with "/" (or are "*").
    if ($raw_uri =~ m{\A(https?)://([^/?#]*)(.*)\z}si) {
        my ($scheme, $authority, $rest) = (lc $1, $2, $3);
        return ({ error => 400, message => 'Bad Request' }, $header_end + 4)
            unless _same_authority($scheme, $authority, _host_of(\@fields));
        $raw_uri = '/' . ($rest =~ s{\A/}{}r);
    }
    elsif ($raw_uri eq '*') {
        return ({ error => 400, message => 'Bad Request' }, $header_end + 4)
            unless $method eq 'OPTIONS';
    }
    elsif (substr($raw_uri, 0, 1) ne '/') {
        return ({ error => 400, message => 'Bad Request' }, $header_end + 4);
    }

    # Split path and query string
    my ($raw_path, $query_string) = split(/\?/, $raw_uri, 2);
    $raw_path //= '/';
    $query_string //= '';

    # Decode path (URL-decode, then UTF-8 decode with fallback)
    # Mojolicious-style: try UTF-8 decode, fall back to original bytes if invalid.
    # Fast path: a path with no percent-escapes and no high bytes is already its
    # own decoded form (ASCII is its own UTF-8), so skip uri_unescape and the
    # eval + Encode::decode entirely -- the common case.
    my $path;
    if ($raw_path !~ /[%\x80-\xff]/) {
        $path = $raw_path;
    }
    else {
        my $unescaped = uri_unescape($raw_path);
        $path = eval { decode('UTF-8', $unescaped, Encode::FB_CROAK) } // $unescaped;
    }

    # The scope's headers: the fields as parsed, with every Cookie joined into
    # one at the first one's place (PAGI::Spec::Www). Framing and Host are
    # read from the same fields.
    if (@fields > $self->{max_header_count}) {
        return ({ error => 431, message => 'Request Header Fields Too Large' }, $ret);
    }
    my (@headers, $cookie, @host, @content_length, @transfer_encoding);
    my $expect_continue = 0;
    for my $field (@fields) {
        my ($name, $value) = @$field;
        if ($name eq 'cookie') {
            if ($cookie) { $cookie->[1] .= "; $value" }
            else         { push @headers, $cookie = ['cookie', $value] }
            next;
        }
        if    ($name eq 'host')              { push @host, $value }
        elsif ($name eq 'content-length')    { push @content_length, $value }
        elsif ($name eq 'transfer-encoding') { push @transfer_encoding, $value }
        elsif ($name eq 'expect')            { $expect_continue = 1 if lc($value) eq '100-continue' }
        push @headers, $field;
    }

    # RFC 9112 3.2: exactly one Host on HTTP/1.1, and never two.
    return ($bad, $ret) if @host > 1 || ($http_version eq '1.1' && !@host);

    # RFC 9112 6.3: one Content-Length, 1*DIGIT.
    my $content_length;
    if (@content_length) {
        my $cl_value = $content_length[0];
        return ($bad, $ret) if @content_length > 1 || $cl_value !~ /\A[0-9]+\z/;

        # Check for unreasonably large values (>2GB indicates potential DoS)
        # Using string length check to avoid Perl's numeric conversion issues
        if (length($cl_value) > 10 || $cl_value > 2_147_483_647) {
            return ({ error => 413, message => 'Payload Too Large' }, $ret);
        }
        $content_length = $cl_value + 0;
    }

    # RFC 9112 6.1: the codings of every Transfer-Encoding field, in order;
    # chunked must be the final one.
    my $chunked = 0;
    if (@transfer_encoding) {
        my @codings = map { s/^\s+|\s+$//gr } split /,/, lc join(',', @transfer_encoding);
        if (@codings && $codings[-1] eq 'chunked') {
            $chunked = 1;
        }
        elsif (grep { $_ eq 'chunked' } @codings) {
            return ({ error => 400, message => 'chunked must be the final Transfer-Encoding' }, $ret);
        }
        else {
            return ({ error => 501, message => 'Unsupported Transfer-Encoding' }, $ret);
        }
    }

    # RFC 9112 Section 6.3.3: reject requests with both Transfer-Encoding
    # and Content-Length to prevent request smuggling (CL/TE desync)
    if ($chunked && defined $content_length) {
        return ({ error => 400, message => 'Transfer-Encoding and Content-Length are mutually exclusive' }, $ret);
    }

    my $request = {
        method          => $method,
        path            => $path,
        raw_path        => $raw_path,
        query_string    => $query_string,
        http_version    => $http_version,
        headers         => \@headers,
        content_length  => $content_length,
        chunked         => $chunked,
        expect_continue => $expect_continue,
    };

    return ($request, $ret);
}

sub serialize_response_start {
    my ($self, $status, $headers, $chunked, $http_version) = @_;
    for my $header (@$headers) {
        PAGI::Server::EventValidator::check_header_name($header->[0]);
        PAGI::Server::EventValidator::check_header_value($header->[1]);
    }
    return $self->_encode_response_start($status, $headers, $chunked, $http_version);
}

# Internal encoding for headers already checked at the app-event boundary.
# Public callers, including synthetic server responses, use the checked method
# above. Both paths share this framing implementation.
sub _encode_response_start {
    my ($self, $status, $headers, $chunked, $http_version) = @_;
    $chunked //= 0;
    $http_version //= '1.1';

    my $phrase = $STATUS_PHRASES{$status} // 'Unknown';
    my $response = "HTTP/$http_version $status $phrase\r\n";

    # Serialize headers and detect app-provided Server header in a single pass
    my $has_server = 0;
    for my $header (@$headers) {
        my ($name, $value) = @$header;
        $has_server = 1 if lc($name) eq 'server';
        $response .= "$name: $value\r\n";
    }

    # Add default Server header if app didn't provide one
    unless ($has_server) {
        $_server_header //= "Server: PAGI::Server/$PAGI::Server::VERSION\r\n";
        $response .= $_server_header;
    }

    # Add Transfer-Encoding if chunked (HTTP/1.1 only)
    if ($chunked && $http_version eq '1.1') {
        $response .= "Transfer-Encoding: chunked\r\n";
    }

    $response .= "\r\n";
    return $response;
}

sub serialize_response_body {
    my ($self, $chunk, $more, $chunked) = @_;
    $chunked //= 0;

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

sub serialize_chunk_end {
    my ($self) = @_;

    return "0\r\n\r\n";
}

sub serialize_continue {
    my ($self) = @_;

    return "HTTP/1.1 100 Continue\r\n\r\n";
}

sub serialize_trailers {
    my ($self, $headers) = @_;
    for my $header (@$headers) {
        PAGI::Server::EventValidator::check_header_name($header->[0]);
        PAGI::Server::EventValidator::check_header_value($header->[1]);
    }
    return $self->_encode_trailers($headers);
}

# Internal counterpart to _encode_response_start: validation belongs to the
# app-event boundary or to the checked public serializer above.
sub _encode_trailers {
    my ($self, $headers) = @_;

    my $trailers = '';
    for my $header (@$headers) {
        my ($name, $value) = @$header;
        $trailers .= "$name: $value\r\n";
    }
    $trailers .= "\r\n";
    return $trailers;
}

sub format_date {
    my ($self) = @_;

    my $now = time();
    if ($now != $_cached_date_time) {
        $_cached_date_time = $now;
        my @days = qw(Sun Mon Tue Wed Thu Fri Sat);
        my @months = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
        my @gmt = gmtime($now);
        $_cached_date = sprintf("%s, %02d %s %04d %02d:%02d:%02d GMT",
            $days[$gmt[6]], $gmt[3], $months[$gmt[4]], $gmt[5] + 1900,
            $gmt[2], $gmt[1], $gmt[0]);
    }
    return $_cached_date;
}

=head2 parse_chunked_body

    my ($data, $bytes_consumed, $complete) = $proto->parse_chunked_body($buffer);

Parses chunked Transfer-Encoding body from the buffer. Returns:
- $data: decoded body data (may be empty string)
- $bytes_consumed: number of bytes consumed from buffer
- $complete: 1 once the terminating chunk and the trailer section that follows
  it have been consumed, 0 otherwise

The trailer section (RFC 9112 section 7.1.2) is field lines, which may be
none, followed by a blank line; its fields are consumed as framing and are not
returned. The body ends at that blank line, so a terminating chunk whose
trailer section has not fully arrived is not yet complete.

Returns (undef, 0, 0) if more data is needed.

=cut

# The offset just past the blank line that ends a trailer section starting at
# $pos, or undef while that section is still arriving.
sub _trailer_section_end {
    my ($buffer, $pos) = @_;

    while (1) {
        my $crlf = index($buffer, "\r\n", $pos);
        return undef if $crlf < 0;
        return $crlf + 2 if $crlf == $pos;  # the blank line, and the body ends
        $pos = $crlf + 2;                   # a trailer field line
    }
}

sub parse_chunked_body {
    my ($self, $buffer_ref) = @_;

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
        $size_line =~ s/^\s+|\s+$//g;  # Trim whitespace

        # Validate chunk size is valid hex (RFC 7230 Section 4.1)
        if ($size_line eq '' || $size_line !~ /^[0-9a-fA-F]+$/) {
            return ({ error => 400, message => 'Invalid chunk size' }, 0, 0);
        }

        # Reject obviously oversized chunk sizes before hex() conversion
        # 7 hex digits = max 268MB, 8+ digits certainly exceeds any reasonable limit
        if (length($size_line) > 7) {
            return ({ error => 413, message => 'Chunk Too Large' }, 0, 0);
        }

        my $chunk_size = hex($size_line);

        # Reject chunks exceeding max_chunk_size (DoS protection)
        if ($chunk_size > $self->{max_chunk_size}) {
            return ({ error => 413, message => 'Chunk Too Large' }, 0, 0);
        }

        my $chunk_start = $crlf + 2;

        # The terminating chunk is followed by a trailer section rather than a
        # trailing CRLF: zero or more field lines, then the blank line that
        # ends the body (RFC 9112 section 7.1.2). Nothing here reads the
        # fields; the body ends where the blank line does, and not before.
        if ($chunk_size == 0) {
            my $trailer_end = _trailer_section_end($buffer, $chunk_start);
            last unless defined $trailer_end;  # Need more data
            $total_consumed = $trailer_end;
            $complete = 1;
            last;
        }

        # Check if we have the full chunk + trailing CRLF
        my $chunk_end = $chunk_start + $chunk_size + 2;  # +2 for trailing CRLF

        if (length($buffer) < $chunk_end) {
            last;  # Need more data
        }

        $data .= substr($buffer, $chunk_start, $chunk_size);
        $total_consumed = $chunk_end;
    }

    return ($data, $total_consumed, $complete);
}

1;

__END__

=head1 SEE ALSO

L<PAGI::Server::Connection>

=head1 AUTHOR

John Napiorkowski E<lt>jjnapiork@cpan.orgE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
