package PAGI::Server::JSONLog;
use strict;
use warnings;

use Encode ();
use POSIX ();
use Time::HiRes ();

our $VERSION = '0.002014';

# A JSON line is written for every request, so the encoder must be the XS one:
# the pure-Perl encoder costs about thirty times a clf line. Without it, JSON
# logging is unavailable rather than slow.
my $JSON = eval { require Cpanel::JSON::XS; Cpanel::JSON::XS->new->utf8->allow_nonref };

# True when JSON lines can be written.
sub available { return $JSON ? 1 : 0 }

# RFC 3339, UTC, milliseconds: readable, sortable as text, and the shape ECS
# expects in @timestamp. Formatting the date is most of a line's cost, so the
# seconds part is kept for as long as the second lasts.
my ($_stamped_second, $_stamped_prefix) = (-1, '');

sub timestamp {
    my ($epoch) = @_;
    $epoch //= Time::HiRes::time();
    my $seconds = int $epoch;
    if ($seconds != $_stamped_second) {
        $_stamped_second = $seconds;
        $_stamped_prefix = POSIX::strftime('%Y-%m-%dT%H:%M:%S', gmtime $seconds);
    }
    return $_stamped_prefix . sprintf('.%03dZ', int(($epoch - $seconds) * 1000));
}

# Log values arrive as characters, UTF-8 bytes, or raw wire bytes that are
# neither. Bytes that are strict UTF-8 are decoded; any other bytes keep one
# code point per byte, so nothing is lost or refused. Strict matters: Perl's
# own decoder also accepts surrogates, code points above U+10FFFF and its
# extended sequences, which the encoder would write back out as bytes a log
# shipper rejects -- and a client can put them in a request path.
sub text {
    my ($value) = @_;
    return undef unless defined $value;
    return "$value" if $value =~ /\A[\x00-\x7f]*\z/;    # the common case

    unless (utf8::is_utf8($value)) {
        my $chars = eval { Encode::decode('UTF-8', my $bytes = "$value", Encode::FB_CROAK()) };
        return defined $chars ? $chars : "$value";
    }

    # A character string can still hold what UTF-8 cannot encode.
    (my $chars = $value) =~ s/[^\x{0}-\x{D7FF}\x{E000}-\x{10FFFF}]/\x{FFFD}/g;
    return $chars;
}

# Encoded '"key":' prefixes. The keys are the fixed field names and banner
# labels, so this stays small, and each is encoded once rather than per line.
my %_KEY;

# One JSON object with keys in the order given. An arrayref value is a list of
# pairs and becomes a nested object, also in order.
sub object {
    my @members;
    for (my $i = 0; $i < @_; $i += 2) {
        my ($key, $value) = @_[$i, $i + 1];
        push @members, ($_KEY{$key} //= $JSON->encode($key) . ':')
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
preset, using L<Cpanel::JSON::XS>; C<available> says whether it is installed.
See L<PAGI::Server/log_format>.

=cut
