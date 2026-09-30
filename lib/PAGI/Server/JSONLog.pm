package PAGI::Server::JSONLog;
use strict;
use warnings;

use JSON::PP ();
use POSIX ();
use Time::HiRes ();

our $VERSION = '0.002014';

# Encodes single values; objects are assembled here so key order is ours.
my $JSON = JSON::PP->new->utf8->allow_nonref;

# RFC 3339, UTC, milliseconds: readable, sortable as text, and the shape ECS
# expects in @timestamp.
sub timestamp {
    my ($epoch) = @_;
    $epoch //= Time::HiRes::time();
    my $seconds = int $epoch;
    my $millis  = int(($epoch - $seconds) * 1000);
    return POSIX::strftime('%Y-%m-%dT%H:%M:%S', gmtime $seconds)
        . sprintf('.%03dZ', $millis);
}

# Log values arrive as characters, UTF-8 bytes, or raw wire bytes that are
# neither. Characters pass through; bytes that decode as UTF-8 are decoded;
# anything else keeps one code point per byte, so nothing is lost or refused.
sub text {
    my ($value) = @_;
    return undef unless defined $value;
    return $value if utf8::is_utf8($value);
    my $chars = "$value";
    utf8::decode($chars);    # leaves $chars unchanged when not valid UTF-8
    return $chars;
}

# One JSON object with keys in the order given. An arrayref value is a list of
# pairs and becomes a nested object, also in order.
sub object {
    my @pairs = @_;
    my @members;
    while (my ($key, $value) = splice @pairs, 0, 2) {
        push @members, $JSON->encode($key) . ':'
            . (ref $value eq 'ARRAY' ? object(@$value) : $JSON->encode($value));
    }
    return '{' . join(',', @members) . '}';
}

# A server diagnostic event as one line. Keys are the event's own, plus time.
sub diagnostic {
    my ($event) = @_;
    return object(
        time     => timestamp(),
        level    => $event->{level},
        category => $event->{category},
        message  => text($event->{message}),
        pid      => $event->{pid},
        (defined $event->{worker} ? (worker => $event->{worker}) : ()),
        ($event->{notes}
            ? (notes => [map { ($_->[0], text($_->[1])) } @{ $event->{notes} }])
            : ()),
    );
}

# One access-log record as a line. Wire values (path, query, headers) are raw
# bytes, so each goes through text(); status is null when none was sent.
sub access {
    my ($info) = @_;

    my %header;
    for my $pair (@{ $info->{request_headers} || [] }) {
        $header{ lc $pair->[0] } //= $pair->[1];    # first occurrence, as %{Name}i
    }
    my $status = $info->{status};

    return object(
        time       => timestamp(),
        client     => text($info->{client_ip}),
        method     => text($info->{method}),
        path       => text($info->{path}),
        query      => text($info->{query} // ''),
        protocol   => 'HTTP/' . ($info->{http_version} // '1.1'),
        status     => (defined $status && $status =~ /\A\d+\z/ ? 0 + $status : undef),
        size       => 0 + ($info->{size} // 0),
        duration   => 0 + ($info->{duration} // 0),
        referer    => text($header{referer}),
        user_agent => text($header{'user-agent'}),
        pid        => $$,
        (defined $info->{worker} ? (worker => 0 + $info->{worker}) : ()),
    );
}

1;

__END__

=head1 NAME

PAGI::Server::JSONLog - JSON log line encoding for PAGI::Server (internal)

=head1 DESCRIPTION

Internal to PAGI::Server; not a public API. Builds the one-object-per-line
records written when C<log_format> is C<json> and for the C<json> access-log
preset. See L<PAGI::Server/log_format>.

=cut
