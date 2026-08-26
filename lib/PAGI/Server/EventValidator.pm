package PAGI::Server::EventValidator;

use strict;
use warnings;

our $VERSION = '0.002009';

use Carp qw(croak);
use Encode ();

# --- Primitive shape checks (anchored; undef and refs are always false) ---

sub _is_nonneg_int {
    my ($v) = @_;
    return defined $v && !ref $v && $v =~ /\A[0-9]+\z/;
}

sub _is_bool01 {
    my ($v) = @_;
    return defined $v && !ref $v && $v =~ /\A[01]\z/;
}

sub _is_nonneg_number {
    my ($v) = @_;
    return defined $v && !ref $v && $v =~ /\A[0-9]+(?:\.[0-9]+)?\z/;
}

# --- Shared header validation (byte safety per PAGI Www.pod "Header byte safety") ---
# Names: no CR/LF/NUL and no other control characters. Values: no CR/LF/NUL.

sub check_header_value {
    my ($value) = @_;
    die "Invalid header value: contains CR, LF, or null byte\n"
        if $value =~ /[\r\n\0]/;
    return $value;
}

sub check_header_name {
    my ($name) = @_;
    die "Invalid header name: contains CR, LF, or null byte\n"
        if $name =~ /[\r\n\0]/;
    die "Invalid header name: contains control characters\n"
        if $name =~ /[[:cntrl:]]/;
    return $name;
}

sub validate_headers {
    my ($headers, $event_type) = @_;
    croak "$event_type 'headers' must be an array reference"
        unless ref $headers eq 'ARRAY';
    for my $h (@$headers) {
        croak "$event_type headers: each header must be a 2-element array reference"
            unless ref $h eq 'ARRAY' && @$h == 2;
        croak "$event_type headers: header name and value must be defined strings"
            if !defined $h->[0] || !defined $h->[1] || ref $h->[0] || ref $h->[1];
        check_header_name($h->[0]);
        check_header_value($h->[1]);
    }
    return;
}

# =============================================================================
# PAGI::Server::EventValidator - Mandatory outgoing-event validation
#
# Per main.mkdn: Servers must raise exceptions if events are missing required
# fields, event fields are of the wrong type, the type is unrecognized, or
# events arrive out of sequence.
#
# This module is the shared authority for both event shape and send-sequence
# validation. Every send path in PAGI::Server::Connection and the lifespan
# send path in PAGI::Server call it unconditionally, in every environment;
# there is no way to disable it.
# =============================================================================

# =============================================================================
# HTTP Event Validation
# =============================================================================

sub validate_http_send {
    my ($event, $opts) = @_;
    my $ext = ($opts && $opts->{extensions}) || {};
    my $type = $event->{type} // '';

    if ($type eq 'http.response.start') {
        _validate_http_response_start($event);
    }
    elsif ($type eq 'http.response.body') {
        _validate_http_response_body($event);
    }
    elsif ($type eq 'http.response.trailers') {
        _validate_http_response_trailers($event);
    }
    elsif ($type eq 'http.fullflush') {
        croak "Extension not enabled: fullflush"
            unless exists $ext->{fullflush};
    }
    else {
        croak "Unrecognized event type '$type' for http protocol";
    }
    return;
}

sub _validate_http_response_start {
    my ($event) = @_;

    # status is required (Int)
    croak "http.response.start requires 'status' field"
        unless exists $event->{status};
    croak "http.response.start 'status' must be a non-negative integer"
        unless _is_nonneg_int($event->{status});

    # headers must be a valid tuple list if present
    validate_headers($event->{headers}, 'http.response.start')
        if exists $event->{headers} && defined $event->{headers};

    # trailers must be 0 or 1 if present
    if (exists $event->{trailers} && defined $event->{trailers}) {
        croak "http.response.start 'trailers' must be 0 or 1"
            unless _is_bool01($event->{trailers});
    }
}

sub _validate_http_response_body {
    my ($event) = @_;

    # Exactly one of body, file, or fh must be present
    my $has_body = exists $event->{body};
    my $has_file = exists $event->{file};
    my $has_fh = exists $event->{fh};
    my $count = $has_body + $has_file + $has_fh;

    croak "http.response.body requires exactly one of body/file/fh (got $count)"
        unless $count <= 1;  # 0 is OK - defaults to empty body

    # offset must be a non-negative integer if present
    if (exists $event->{offset} && defined $event->{offset}) {
        croak "http.response.body 'offset' must be a non-negative integer"
            unless _is_nonneg_int($event->{offset});
    }

    # length must be a non-negative integer if present
    if (exists $event->{length} && defined $event->{length}) {
        croak "http.response.body 'length' must be a non-negative integer"
            unless _is_nonneg_int($event->{length});
    }

    # more must be 0 or 1 if present
    if (exists $event->{more} && defined $event->{more}) {
        croak "http.response.body 'more' must be 0 or 1"
            unless _is_bool01($event->{more});
    }
}

sub _validate_http_response_trailers {
    my ($event) = @_;

    # headers must be a valid tuple list if present
    validate_headers($event->{headers}, 'http.response.trailers')
        if exists $event->{headers} && defined $event->{headers};
}

# =============================================================================
# WebSocket Event Validation
# =============================================================================

sub validate_websocket_send {
    my ($event, $opts) = @_;
    my $ext = ($opts && $opts->{extensions}) || {};
    my $type = $event->{type} // '';

    if ($type eq 'websocket.accept') {
        _validate_websocket_accept($event);
    }
    elsif ($type eq 'websocket.send') {
        _validate_websocket_send_event($event);
    }
    elsif ($type eq 'websocket.close') {
        _validate_websocket_close($event);
    }
    elsif ($type eq 'websocket.keepalive') {
        _validate_websocket_keepalive($event);
    }
    elsif ($type eq 'websocket.http.response.start') {
        croak "Extension not enabled: websocket.http.response"
            unless exists $ext->{'websocket.http.response'};
        _validate_ws_denial_start($event);
    }
    elsif ($type eq 'websocket.http.response.body') {
        croak "Extension not enabled: websocket.http.response"
            unless exists $ext->{'websocket.http.response'};
        _validate_ws_denial_body($event);
    }
    else {
        croak "Unrecognized event type '$type' for websocket protocol";
    }
    return;
}

sub _validate_websocket_accept {
    my ($event) = @_;

    # headers must be a valid tuple list if present
    validate_headers($event->{headers}, 'websocket.accept')
        if exists $event->{headers} && defined $event->{headers};
}

sub _validate_websocket_send_event {
    my ($event) = @_;

    # Exactly one of bytes or text must be present and defined
    my $count = (defined $event->{bytes} ? 1 : 0) + (defined $event->{text} ? 1 : 0);

    croak "websocket.send requires exactly one of bytes/text (got $count)"
        unless $count == 1;
}

sub _validate_websocket_close {
    my ($event) = @_;

    # code must be a non-negative integer if present
    if (exists $event->{code} && defined $event->{code}) {
        croak "websocket.close 'code' must be a non-negative integer"
            unless _is_nonneg_int($event->{code});
    }
}

sub _validate_websocket_keepalive {
    my ($event) = @_;

    # interval is required (Number)
    croak "websocket.keepalive requires 'interval' field"
        unless exists $event->{interval};
    croak "websocket.keepalive 'interval' must be a non-negative number"
        unless _is_nonneg_number($event->{interval});

    # timeout must be a non-negative number if present
    if (exists $event->{timeout} && defined $event->{timeout}) {
        croak "websocket.keepalive 'timeout' must be a non-negative number"
            unless _is_nonneg_number($event->{timeout});
    }
}

sub _validate_ws_denial_start {
    my ($event) = @_;

    # status is required (Int)
    croak "websocket.http.response.start requires 'status' field"
        unless exists $event->{status};
    croak "websocket.http.response.start 'status' must be a non-negative integer"
        unless _is_nonneg_int($event->{status});

    # headers must be a valid tuple list if present
    validate_headers($event->{headers}, 'websocket.http.response.start')
        if exists $event->{headers} && defined $event->{headers};
}

sub _validate_ws_denial_body {
    my ($event) = @_;

    # more must be 0 or 1 if present
    if (exists $event->{more} && defined $event->{more}) {
        croak "websocket.http.response.body 'more' must be 0 or 1"
            unless _is_bool01($event->{more});
    }
}

# =============================================================================
# SSE Event Validation
# =============================================================================

sub validate_sse_send {
    my ($event, $opts) = @_;
    my $ext = ($opts && $opts->{extensions}) || {};
    my $type = $event->{type} // '';

    if ($type eq 'sse.start') {
        _validate_sse_start($event);
    }
    elsif ($type eq 'sse.send') {
        _validate_sse_send_event($event);
    }
    elsif ($type eq 'sse.comment') {
        _validate_sse_comment($event);
    }
    elsif ($type eq 'sse.keepalive') {
        _validate_sse_keepalive($event);
    }
    elsif ($type eq 'sse.close') {
        _validate_sse_close($event);
    }
    elsif ($type eq 'sse.http.response.start') {
        _validate_sse_decline_start($event);
    }
    elsif ($type eq 'sse.http.response.body') {
        _validate_sse_decline_body($event);
    }
    elsif ($type eq 'http.fullflush') {
        croak "Extension not enabled: fullflush"
            unless exists $ext->{fullflush};
    }
    else {
        croak "Unrecognized event type '$type' for sse protocol";
    }
    return;
}

sub _validate_sse_decline_start {
    my ($event) = @_;

    croak "sse.http.response.start requires 'status' field"
        unless exists $event->{status} && defined $event->{status};
    croak "sse.http.response.start 'status' must be a non-negative integer"
        unless _is_nonneg_int($event->{status});
    validate_headers($event->{headers}, 'sse.http.response.start')
        if exists $event->{headers} && defined $event->{headers};
}

sub _validate_sse_decline_body {
    my ($event) = @_;

    if (exists $event->{more} && defined $event->{more}) {
        croak "sse.http.response.body 'more' must be 0 or 1"
            unless _is_bool01($event->{more});
    }
}

sub _validate_sse_close {
    my ($event) = @_;

    # reason is optional and server-side only; if present it must be a string
    if (exists $event->{reason} && defined $event->{reason}) {
        croak "sse.close 'reason' must be a string"
            if ref $event->{reason};
    }
}

sub _validate_sse_start {
    my ($event) = @_;

    # status must be a non-negative integer if present
    if (exists $event->{status} && defined $event->{status}) {
        croak "sse.start 'status' must be a non-negative integer"
            unless _is_nonneg_int($event->{status});
    }

    # headers must be a valid tuple list if present
    validate_headers($event->{headers}, 'sse.start')
        if exists $event->{headers} && defined $event->{headers};
}

sub _validate_sse_send_event {
    my ($event) = @_;

    # data is required (String)
    croak "sse.send requires 'data' field"
        unless exists $event->{data};
    croak "sse.send 'data' must be a string"
        unless defined $event->{data} && !ref $event->{data};

    for my $f (qw(event id)) {
        next unless exists $event->{$f} && defined $event->{$f};
        croak "sse.send '$f' must be a string" if ref $event->{$f};
        croak "sse.send '$f' must not contain newline characters"
            if $event->{$f} =~ /[\r\n]/;
    }
    if (exists $event->{retry} && defined $event->{retry}) {
        croak "sse.send 'retry' must be a non-negative integer"
            unless _is_nonneg_int($event->{retry});
    }
}

sub _validate_sse_comment {
    my ($event) = @_;

    # comment is required (String)
    croak "sse.comment requires 'comment' field"
        unless exists $event->{comment};
    croak "sse.comment 'comment' must be a string"
        unless defined $event->{comment} && !ref $event->{comment};
}

sub _validate_sse_keepalive {
    my ($event) = @_;

    # interval is required (Number)
    croak "sse.keepalive requires 'interval' field"
        unless exists $event->{interval};
    croak "sse.keepalive 'interval' must be a non-negative number"
        unless _is_nonneg_number($event->{interval});

    # comment is optional (String); when present it must be encodable as
    # UTF-8 -- validated here, at arm time, so an unencodable comment fails
    # THIS send's Future instead of dying later inside the keepalive timer
    # tick (which would escape uncaught and unwind the event loop).
    if (exists $event->{comment} && defined $event->{comment}) {
        # Copy into a lexical before encoding: Encode::encode consumes its
        # input string in place, and encoding the caller's own $event->{comment}
        # directly would leave it mutated (or emptied) out from under the
        # caller after this validation-only check.
        my $comment = $event->{comment};
        my $ok = !ref($comment)
            && eval { Encode::encode('UTF-8', $comment, Encode::FB_CROAK); 1 };
        croak "sse.keepalive 'comment' must be a UTF-8-encodable string"
            unless $ok;
    }
}

# =============================================================================
# Lifespan Event Validation
# =============================================================================

sub validate_lifespan_send {
    my ($event) = @_;
    my $type = $event->{type} // '';

    if ($type eq 'lifespan.startup.complete' or $type eq 'lifespan.shutdown.complete') {
        # no fields beyond type
    }
    elsif ($type eq 'lifespan.startup.failed' or $type eq 'lifespan.shutdown.failed') {
        if (exists $event->{message} && defined $event->{message}) {
            croak "$type 'message' must be a string" if ref $event->{message};
        }
    }
    else {
        croak "Unrecognized event type '$type' for lifespan protocol";
    }
    return;
}

# =============================================================================
# Sequence State Machines
#
# Each advance_* function is a pure transition function: given the current
# per-connection sequence state and a (shape-already-validated) event, it
# returns the next state or croaks describing the send-order violation.
# Dispatchers run shape validation first and call these separately.
# =============================================================================

sub _http_body_is_terminal {
    my ($event) = @_;
    return 1 if defined $event->{file} || defined $event->{fh};
    my $more = $event->{more} // 0;
    return !$more;
}

sub advance_http {
    my ($state, $event) = @_;
    my $type = $event->{type} // '';

    croak "cannot send '$type': response already complete"
        if $state eq 'complete';

    if ($state eq 'initial') {
        croak "cannot send '$type' before http.response.start"
            unless $type eq 'http.response.start';
        return $event->{trailers} ? 'started_t' : 'started';
    }

    croak "cannot send duplicate http.response.start"
        if $type eq 'http.response.start';

    if ($type eq 'http.response.trailers') {
        croak "cannot send http.response.trailers: trailers were not declared or body is not complete"
            unless $state eq 'awaiting_trailers';
        return 'complete';
    }

    return $state if $type eq 'http.fullflush';

    if ($type eq 'http.response.body') {
        if ($state eq 'started') {
            return _http_body_is_terminal($event) ? 'complete' : 'started';
        }
        if ($state eq 'started_t') {
            return _http_body_is_terminal($event) ? 'awaiting_trailers' : 'started_t';
        }
    }

    # awaiting_trailers state only accepts trailers/fullflush (handled above);
    # anything else here means a body chunk arrived after completion was
    # already declared but before the required trailers were sent.
    croak "cannot send '$type' during http response phase '$state'";
}

sub advance_websocket {
    my ($state, $event) = @_;
    my $type = $event->{type} // '';

    croak "cannot send '$type' after websocket.close"
        if $state eq 'closed';
    croak "cannot send '$type': denial response already complete"
        if $state eq 'denial_complete';

    if ($state eq 'connecting') {
        return 'accepted' if $type eq 'websocket.accept';
        return 'closed' if $type eq 'websocket.close';
        return 'denial' if $type eq 'websocket.http.response.start';
        croak "cannot send '$type' before websocket.accept";
    }

    if ($state eq 'accepted') {
        return 'accepted' if $type eq 'websocket.send' || $type eq 'websocket.keepalive';
        return 'closed' if $type eq 'websocket.close';
        croak "cannot send '$type' after websocket.accept";
    }

    if ($state eq 'denial') {
        if ($type eq 'websocket.http.response.body') {
            my $more = $event->{more} // 0;
            return $more ? 'denial' : 'denial_complete';
        }
        croak "cannot send '$type' after websocket.http.response.start";
    }

    croak "cannot send '$type' in websocket state '$state'";
}

sub advance_sse {
    my ($state, $event) = @_;
    my $type = $event->{type} // '';

    if ($state eq 'closed') {
        return 'closed' if $type eq 'sse.close';
        croak "cannot send '$type' after sse.close";
    }
    croak "cannot send '$type': decline response already complete"
        if $state eq 'decline_complete';

    if ($state eq 'initial') {
        return 'streaming' if $type eq 'sse.start';
        return 'declining' if $type eq 'sse.http.response.start';
        croak "cannot send '$type' before sse.start";
    }

    if ($state eq 'streaming') {
        return 'streaming' if $type eq 'sse.send' || $type eq 'sse.comment' || $type eq 'sse.keepalive' || $type eq 'http.fullflush';
        return 'closed' if $type eq 'sse.close';
        croak "cannot decline with sse.http.response.start after sse.start"
            if $type eq 'sse.http.response.start';
        croak "cannot send duplicate sse.start"
            if $type eq 'sse.start';
        croak "cannot send '$type' after sse.start";
    }

    if ($state eq 'declining') {
        if ($type eq 'sse.http.response.body') {
            my $more = $event->{more} // 0;
            return $more ? 'declining' : 'decline_complete';
        }
        croak "cannot send '$type' after sse.http.response.start";
    }

    croak "cannot send '$type' in sse state '$state'";
}

sub advance_lifespan {
    my ($state, $event) = @_;
    my $type = $event->{type} // '';

    if ($state eq 'startup_pending') {
        return 'running' if $type eq 'lifespan.startup.complete';
        return 'finished' if $type eq 'lifespan.startup.failed';
    }
    elsif ($state eq 'shutdown_pending') {
        return 'finished' if $type eq 'lifespan.shutdown.complete' || $type eq 'lifespan.shutdown.failed';
    }

    croak "cannot send '$type' during lifespan phase '$state'";
}

1;

__END__

=head1 NAME

PAGI::Server::EventValidator - Mandatory outgoing-event validation

=head1 SYNOPSIS

    use PAGI::Server::EventValidator;

    # Shape validation (per protocol family)
    PAGI::Server::EventValidator::validate_http_send($event, \%opts);

    # Send-sequence validation (pure state-transition functions)
    my $next_state = PAGI::Server::EventValidator::advance_http($state, $event);

=head1 DESCRIPTION

This module is the shared, mandatory validator for every event a PAGI
application sends to the server. C<PAGI::Server::Connection> calls it on
every send path (HTTP/1, HTTP/2, WebSocket, SSE) and C<PAGI::Server> calls
it on the lifespan send path; validation cannot be disabled and runs in
every environment, including production. It checks that:

=over 4

=item * Required fields are present

=item * Field types are correct

=item * Mutually exclusive fields are handled properly

=item * The event type is recognized (and, for extension-gated types, the
extension is enabled)

=item * Events arrive in a legal order for their protocol family (see the
C<advance_*> functions)

=back

A malformed, mis-sequenced, unrecognized, or unadvertised-extension event
causes the corresponding C<$send> Future to fail; see
L<PAGI::Server::Connection> for how these functions are wired into each
send path.

=head1 FUNCTIONS

=head2 validate_http_send($event, $opts)

Validates HTTP send events: C<http.response.start>, C<http.response.body>,
C<http.response.trailers>, C<http.fullflush>. C<$opts> is an optional hash
reference of the form C<< { extensions => \%scope_extensions } >>;
C<http.fullflush> croaks with C<"Extension not enabled: fullflush"> unless
C<$opts-E<gt>{extensions}{fullflush}> exists. Any other event type croaks
with C<"Unrecognized event type '$type' for http protocol">.

=head2 validate_websocket_send($event, $opts)

Validates WebSocket send events: C<websocket.accept>, C<websocket.send>,
C<websocket.close>, C<websocket.keepalive>, C<websocket.http.response.start>,
C<websocket.http.response.body>. C<$opts> is an optional hash reference of
the form C<< { extensions => \%scope_extensions } >>; the two
C<websocket.http.response.*> types croak with
C<"Extension not enabled: websocket.http.response"> unless
C<$opts-E<gt>{extensions}{'websocket.http.response'}> exists. Any other
event type croaks with C<"Unrecognized event type '$type' for websocket protocol">.

=head2 validate_sse_send($event, $opts)

Validates SSE send events: C<sse.start>, C<sse.send>, C<sse.comment>,
C<sse.keepalive>, C<sse.close>, C<sse.http.response.start>,
C<sse.http.response.body>, C<http.fullflush>. C<$opts> is an optional hash
reference of the form C<< { extensions => \%scope_extensions } >>;
C<http.fullflush> croaks with C<"Extension not enabled: fullflush"> unless
C<$opts-E<gt>{extensions}{fullflush}> exists. Any other event type croaks
with C<"Unrecognized event type '$type' for sse protocol">.

C<sse.keepalive>'s C<comment> field is optional (defaults to C<''>), but
when present it is validated here, at arm time: it must be a defined
non-reference string that round-trips
C<Encode::encode('UTF-8', $comment, Encode::FB_CROAK)>, or this call croaks
with C<"sse.keepalive 'comment' must be a UTF-8-encodable string">. This
keeps an unencodable comment from surfacing later as an uncaught die inside
the keepalive timer tick.

=head2 validate_lifespan_send($event)

Validates lifespan send events: C<lifespan.startup.complete>,
C<lifespan.startup.failed>, C<lifespan.shutdown.complete>,
C<lifespan.shutdown.failed>. For the C<*.failed> types, C<message> is
optional but must be a defined non-reference string when present. Any
other event type croaks with
C<"Unrecognized event type '$type' for lifespan protocol">.

=head2 advance_http($state, $event)

Pure send-sequence transition function for the HTTP family. Does not
validate event shape; call C<validate_http_send> separately first. States:
C<initial>, C<started>, C<started_t> (trailers declared), C<awaiting_trailers>,
C<complete>. Starting state is C<initial>.

From C<initial>, C<http.response.start> advances to C<started>, or to
C<started_t> if the start event's C<trailers> field is true. From C<started>
or C<started_t>, C<http.response.body> events with a true C<more> field (and
no C<file>/C<fh>) keep the same state; a terminal body chunk (C<more> false,
absent, or a C<file>/C<fh> body) advances C<started> to C<complete> and
C<started_t> to C<awaiting_trailers>. C<http.response.trailers> only
succeeds from C<awaiting_trailers>, advancing to C<complete>.
C<http.fullflush> is legal in C<started>, C<started_t>, and
C<awaiting_trailers> and leaves the state unchanged.

Croaks: any event in C<initial> other than C<http.response.start>
(C<"cannot send '<type>' before http.response.start">); C<http.response.start>
outside C<initial> (C<"cannot send duplicate http.response.start">);
C<http.response.trailers> outside C<awaiting_trailers>
(C<"cannot send http.response.trailers: trailers were not declared or body is not complete">);
any event once C<complete> (C<"cannot send '<type>': response already complete">).

=head2 advance_websocket($state, $event)

Pure send-sequence transition function for the WebSocket family. Does not
validate event shape; call C<validate_websocket_send> separately first.
States: C<connecting>, C<accepted>, C<denial>, C<denial_complete>, C<closed>.
Starting state is C<connecting>.

From C<connecting>: C<websocket.accept> advances to C<accepted>;
C<websocket.close> advances to C<closed>; C<websocket.http.response.start>
(a denial) advances to C<denial>. From C<accepted>: C<websocket.send> and
C<websocket.keepalive> keep C<accepted>; C<websocket.close> advances to
C<closed>. From C<denial>: C<websocket.http.response.body> with a true
C<more> field keeps C<denial>; a terminal body chunk advances to
C<denial_complete>.

Croaks: C<websocket.send>/C<websocket.keepalive> before accept
(C<"cannot send '<type>' before websocket.accept">); any denial or accept
event once C<accepted> (C<"cannot send '<type>' after websocket.accept">);
any non-body event once denial has started
(C<"cannot send '<type>' after websocket.http.response.start">); any event
once C<closed> (C<"cannot send '<type>' after websocket.close">, including a
second C<websocket.close> - unlike SSE, WebSocket close is not idempotent);
any event once C<denial_complete>
(C<"cannot send '<type>': denial response already complete">).

=head2 advance_sse($state, $event)

Pure send-sequence transition function for the SSE family. Does not
validate event shape; call C<validate_sse_send> separately first. States:
C<initial>, C<streaming>, C<declining>, C<decline_complete>, C<closed>.
Starting state is C<initial>.

From C<initial>: C<sse.start> advances to C<streaming>;
C<sse.http.response.start> (a decline) advances to C<declining>. From
C<streaming>: C<sse.send>, C<sse.comment>, C<sse.keepalive>, and
C<http.fullflush> keep C<streaming>; C<sse.close> advances to C<closed>.
From C<declining>:
C<sse.http.response.body> with a true C<more> field keeps C<declining>; a
terminal body chunk advances to C<decline_complete>. From C<closed>,
C<sse.close> is idempotent and stays C<closed>.

Croaks: any event in C<initial> other than C<sse.start>/decline start
(C<"cannot send '<type>' before sse.start">); a decline start after
C<sse.start> (C<"cannot decline with sse.http.response.start after sse.start">);
a duplicate C<sse.start> (C<"cannot send duplicate sse.start">); any other
event once C<streaming> (C<"cannot send '<type>' after sse.start">); any
non-body event once declining has started
(C<"cannot send '<type>' after sse.http.response.start">); any event other
than C<sse.close> once C<closed> (C<"cannot send '<type>' after sse.close">);
any event once C<decline_complete>
(C<"cannot send '<type>': decline response already complete">).

=head2 advance_lifespan($state, $event)

Pure send-sequence transition function for the lifespan family. Does not
validate event shape; call C<validate_lifespan_send> separately first.
States: C<startup_pending>, C<running>, C<shutdown_pending>, C<finished>.
Starting state is C<startup_pending>. The server itself moves C<running> to
C<shutdown_pending> when it emits the shutdown event; this function does not
perform that transition.

From C<startup_pending>: C<lifespan.startup.complete> advances to
C<running>; C<lifespan.startup.failed> advances to C<finished>. From
C<shutdown_pending>: C<lifespan.shutdown.complete> or
C<lifespan.shutdown.failed> advances to C<finished>. Every other
C<($state, $event)> combination croaks with
C<"cannot send '<type>' during lifespan phase '<state>'">.

=head2 check_header_value($value)

Checks a single header value for byte safety: dies with a C<\n>-terminated
message if C<$value> contains a CR, LF, or null byte. Returns C<$value>
unchanged on success.

=head2 check_header_name($name)

Checks a single header name for byte safety: dies with a C<\n>-terminated
message if C<$name> contains a CR, LF, or null byte, and dies with a
separate C<\n>-terminated message if it contains any other control
character. Returns C<$name> unchanged on success.

=head2 validate_headers($headers, $event_type)

Validates a C<headers> field shared by every event shape that carries one
(C<http.response.start>, C<http.response.trailers>, C<websocket.accept>,
C<websocket.http.response.start>, C<sse.http.response.start>, C<sse.start>).
C<$event_type> is the event type string used in croak messages. Croaks
unless C<$headers> is an array reference of 2-element array references,
each holding a defined, non-reference name and value; each name and value
is then passed through C<check_header_name> and C<check_header_value>.
Returns nothing.

=head1 SEE ALSO

L<PAGI::Server>, L<PAGI::Server::Connection>

=cut
