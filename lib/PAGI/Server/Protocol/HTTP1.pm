package PAGI::Server::Protocol::HTTP1;
use strict;
use warnings;
use experimental 'signatures';

our $VERSION = '0.001';

=head1 NAME

PAGI::Server::Protocol::HTTP1 - HTTP/1.1 protocol handler

=head1 SYNOPSIS

    use PAGI::Server::Protocol::HTTP1;

    my $proto = PAGI::Server::Protocol::HTTP1->new;

    # Parse incoming request
    my ($request, $consumed) = $proto->parse_request($buffer);

    # Serialize response
    my $bytes = $proto->serialize_response_start(200, \@headers);
    $bytes   .= $proto->serialize_response_body($chunk, $more);

=head1 DESCRIPTION

PAGI::Server::Protocol::HTTP1 isolates HTTP/1.1 wire-format parsing and
serialization from PAGI event handling. This allows clean separation of
protocol handling and future addition of HTTP/2 or HTTP/3 modules with
the same interface.

=head1 METHODS

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
    };

=head2 serialize_response_start

    my $bytes = $proto->serialize_response_start($status, \@headers);

Serializes the response line and headers.

=head2 serialize_response_body

    my $bytes = $proto->serialize_response_body($chunk, $more);

Serializes a body chunk. Uses chunked encoding if $more is true.

=head2 serialize_trailers

    my $bytes = $proto->serialize_trailers(\@headers);

Serializes HTTP trailers.

=cut

sub new ($class, %args) {
    my $self = bless {
        # TODO: Add state
    }, $class;
    return $self;
}

sub parse_request ($self, $buffer) {
    # TODO: Implement in Step 1
    # Uses HTTP::Parser::XS for parsing
    return;
}

sub serialize_response_start ($self, $status, $headers) {
    # TODO: Implement in Step 1
    return '';
}

sub serialize_response_body ($self, $chunk, $more) {
    # TODO: Implement in Step 1
    return '';
}

sub serialize_trailers ($self, $headers) {
    # TODO: Implement in Step 2
    return '';
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
