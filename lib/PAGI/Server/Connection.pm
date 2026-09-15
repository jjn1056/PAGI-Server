package PAGI::Server::Connection;
use strict;
use warnings;

our $VERSION = '0.002013';

use Future;
use Future::AsyncAwait;
use Scalar::Util qw(weaken refaddr);
use Protocol::WebSocket::Handshake::Server;
use Protocol::WebSocket::Frame;
use Digest::SHA qw(sha1_base64);
use Encode;
use URI::Escape qw(uri_unescape);
use IO::Async::Timer::Countdown;
use IO::Async::Timer::Periodic;
use Time::HiRes qw(gettimeofday tv_interval);
use PAGI::Server::AsyncFile;
use PAGI::Server::ConnectionState;
use PAGI::Server::TransportState;
use PAGI::Server::EventValidator;


use constant FILE_CHUNK_SIZE => 65536;  # 64KB chunks for file streaming

# Per-second cache for CLF timestamp in access log (same pattern as HTTP1::format_date)
my $_cached_log_timestamp;
my $_cached_log_time = 0;

# =============================================================================
# Header Validation (CRLF Injection Prevention)
# =============================================================================
# RFC 7230 Section 3.2.6: Field values MUST NOT contain CR or LF

sub _validate_header_value { PAGI::Server::EventValidator::check_header_value($_[0]) }

sub _validate_header_name  { PAGI::Server::EventValidator::check_header_name($_[0]) }

# The one-line supplement an application exception makes for disconnect_detail
# (Www.pod: "a short human-readable supplement to the reason"): the message
# without Perl's " at FILE line N." tail and without any trailing stack lines.
# The full text still goes to the log at every site that has one.
sub _detail_from_error {
    my ($error) = @_;
    my ($first) = split /\n/, ($error // '');
    return undef unless defined $first;
    $first =~ s/ at \S+ line \d+\.?\s*\z//;
    $first =~ s/\s+\z//;
    return length $first ? $first : undef;
}

# A request body that crossed max_body_size once this scope had already
# started its response. The 413 that answers the same overrun before the
# application runs has nowhere to go here -- Www.pod "Application Left a
# Response Incomplete" forbids replacing a started response and requires it
# stay observable as truncated -- so the scope ends with body_too_large
# instead. One sentence says why, as disconnect_detail without $where and as
# the log line with the transport in it.
sub _body_limit_after_start {
    my ($limit, $where) = @_;
    return "request body exceeded max_body_size ($limit bytes) after the"
         . ' response had started' . (defined $where ? " ($where)" : '')
         . '; response truncated';
}

# =============================================================================
# HTTP/2 connection-specific header stripping (RFC 9113 section 8.2.2, design
# doc section 13.3)
# =============================================================================
# HTTP/2 forbids connection-specific header fields. An app-supplied
# connection, keep-alive, proxy-connection, transfer-encoding, or upgrade
# header -- or a te header carrying anything but the token 'trailers' --
# corrupts the response at the framing layer: the client receives only
# :status, with no body. HTTP/1.1 has no such prohibition, so this strip
# applies only to the HTTP/2 response paths that call it.
my %H2_CONNECTION_SPECIFIC_HEADER = map { $_ => 1 }
    qw(connection keep-alive proxy-connection transfer-encoding upgrade te);

# Returns a new arrayref with connection-specific header pairs removed,
# warning once per stripped occurrence (not deduplicated by name -- two
# 'keep-alive' headers warn twice). Does not mutate $headers.
#
# $in_trailers (optional, default false) selects the trailer-block variant
# of this rule: RFC 9110 section 6.6.2 forbids every connection-specific
# field from a trailer section outright, so unlike a response's HEADERS
# block there is no 'te: trailers' carve-out inside a trailer block itself
# -- that carve-out is what lets a response ADVERTISE trailers are coming,
# not something a trailer block may then contain. A trailer-borne 'te'
# tuple is therefore stripped regardless of its value.
sub _h2_strip_connection_headers {
    my $self = shift;
    my ($headers, $in_trailers) = @_;
    my $context = $in_trailers ? 'trailers' : 'response';
    my @kept;
    for my $h (@$headers) {
        my ($name, $value) = @$h;
        my $lc_name = lc $name;
        if ($H2_CONNECTION_SPECIFIC_HEADER{$lc_name}) {
            # RFC 9113 permits a response 'te' only with the exact token
            # 'trailers'; a case-insensitive VALUE compare (not a name/list
            # match) is what the token grammar calls for. OWS
            # (leading/trailing whitespace) around the token is trimmed
            # before the compare, per RFC 9110's field-value grammar -- a
            # compound value like 'trailers, gzip' is not the bare token and
            # is still stripped. This carve-out does not apply in a trailer
            # block (see $in_trailers above).
            if (!$in_trailers && $lc_name eq 'te') {
                (my $v = lc $value) =~ s/^\s+|\s+\z//g;
                if ($v eq 'trailers') {
                    # Submit the normalized token, never the original value:
                    # RFC 9113 8.2.1 forbids OWS in field values, and
                    # libnghttp2 versions disagree on how to punish one
                    # (omit the field vs corrupt the whole response).
                    push @kept, [$name, 'trailers'];
                    next;
                }
            }
            $self->_log(warn => "PAGI: connection-specific header '$name' stripped from HTTP/2 $context (RFC 9113)");
            next;
        }
        push @kept, $h;
    }
    return \@kept;
}

# =============================================================================
# HTTP/1.1 connection-specific header stripping (PAGI spec: "Over HTTP/1.1
# the server must ignore or strip application-supplied Transfer-Encoding and
# Connection -- it supplies its own -- and SHOULD log when it does")
# =============================================================================
# Deliberately narrower than the HTTP/2 six-name strip above: HTTP/1.1 has
# no prohibition on keep-alive, proxy-connection, upgrade, or te as ordinary
# application response headers, so only the two names the server itself
# always owns the framing/connection-state for -- transfer-encoding and
# connection -- are stripped here.
my %H1_CONNECTION_SPECIFIC_HEADER = map { $_ => 1 } qw(transfer-encoding connection);

# Returns a new arrayref with app-supplied transfer-encoding/connection pairs
# removed, warning once per stripped occurrence (not deduplicated by name --
# two 'connection' headers warn twice). Does not mutate $headers.
sub _h1_strip_connection_headers {
    my $self = shift;
    my ($headers) = @_;
    my @kept;
    for my $h (@$headers) {
        my ($name, $value) = @$h;
        if ($H1_CONNECTION_SPECIFIC_HEADER{lc $name}) {
            $self->_log(warn => "PAGI: connection-specific header '$name' stripped from HTTP/1.1 response");
            next;
        }
        push @kept, $h;
    }
    return \@kept;
}

# SSE client-signal check (PAGI Www.pod "SSE Connection Detection"): the
# exact media range text/event-stream, case-insensitively, with q > 0.
# A boolean signal test, not content negotiation: wildcards never signal
# SSE, and q=0 is an explicit refusal.
sub _accept_signals_sse {
    my ($headers) = @_;
    my @values;
    for my $h (@$headers) {
        push @values, $h->[1] if $h->[0] eq 'accept';
    }
    return 0 unless @values;
    for my $range (split /,/, join(',', @values)) {
        my ($type, @params) = split /;/, $range;
        $type =~ s/\A\s+|\s+\z//g;
        next unless lc($type) eq 'text/event-stream';
        my $q = 1;
        for my $p (@params) {
            $p =~ s/\A\s+|\s+\z//g;
            $q = $1 if $p =~ /\Aq\s*=\s*([0-9.]+)\z/i;
        }
        return 1 if $q > 0;
    }
    return 0;
}

# RFC 6455 Section 11.3.4: Subprotocol must be a token (no whitespace, separators)
sub _validate_subprotocol {
    my ($value) = @_;

    if ($value =~ /[\r\n\0\s]/) {
        die "Invalid subprotocol: contains CR, LF, null, or whitespace\n";
    }
    # Token characters only (roughly)
    if ($value !~ /^[\w\-\.]+$/) {
        die "Invalid subprotocol: must be alphanumeric, dash, underscore, or dot\n";
    }
    return $value;
}

=head1 NAME

PAGI::Server::Connection - Per-connection state machine

=head1 SYNOPSIS

    # Internal use by PAGI::Server
    my $conn = PAGI::Server::Connection->new(
        stream     => $stream,
        app        => $app,
        protocol   => $protocol,
        server     => $server,
        extensions => {},
    );
    $conn->start;

=head1 DESCRIPTION

PAGI::Server::Connection manages the state machine for a single client
connection. It handles:

=over 4

=item * Request parsing via Protocol::HTTP1

=item * Scope creation for the application

=item * Event queue management for $receive and $send

=item * Protocol upgrades (WebSocket, SSE)

=item * SSE over HTTP/1.1 and HTTP/2

=item * Connection lifecycle and cleanup

=back

=cut

sub new {
    my ($class, %args) = @_;

    my $self = bless {
        stream        => $args{stream},
        app           => $args{app},
        protocol      => $args{protocol},
        server        => $args{server},
        extensions    => $args{extensions} // {},
        state         => $args{state} // {},
        tls_enabled   => $args{tls_enabled} // 0,
        timeout       => $args{timeout} // 60,  # Idle timeout in seconds
        request_timeout => $args{request_timeout} // 0,  # Request stall timeout in seconds (0 = disabled, default for performance)
        ws_idle_timeout => $args{ws_idle_timeout} // 0,   # WebSocket idle timeout (0 = disabled)
        sse_idle_timeout => $args{sse_idle_timeout} // 0,  # SSE idle timeout (0 = disabled)
        max_body_size     => $args{max_body_size},  # 0 = unlimited
        # What is left of a request body the application never read, which this
        # connection consumes before it parses anything else (RFC 9112 s9.3).
        discarding_body   => undef,
        access_log        => $args{access_log},     # Filehandle for access logging
        _access_log_formatter => $args{_access_log_formatter},  # Pre-compiled format closure
        max_receive_queue => $args{max_receive_queue} // 1000,  # Max WebSocket receive queue size
        max_disconnect_receives => $args{max_disconnect_receives} // 100,  # Receives answered with a synthesized disconnect event, per scope (0 = unlimited)
        max_ws_frame_size => $args{max_ws_frame_size} // 65536,  # Max WebSocket frame size in bytes
        sync_file_threshold => $args{sync_file_threshold} // 65536,  # Threshold for sync file reads (default 64KB)
        validate_events => $args{validate_events} // 0,  # Deprecated: core event validation is mandatory; this flag is retained for compatibility and controls nothing.
        # Send-side backpressure (watermarks in bytes)
        # Defaults match Python asyncio: 64KB high, 16KB low (high/4)
        write_high_watermark => $args{write_high_watermark} // 65536,   # 64KB - pause sending above this
        write_low_watermark  => $args{write_low_watermark}  // 16384,   # 16KB - resume sending below this
        _drain_waiters       => [],   # Pending Futures for blocking backpressure (a producer awaiting buffer drain)
        _drain_fires         => [],   # arm_drain callback fires (on_drain hysteresis) -- kept separate from
                                       # _drain_waiters so teardown can resume the former without invoking the
                                       # latter (mirrors h2's stream_drain_waiters vs transport_drain_fires split)
        _drain_check_active  => 0,    # Flag to prevent redundant on_outgoing_empty setup
        tls_info      => undef,  # Populated on first request if TLS
        buffer        => '',
        closed        => 0,
        response_started => 0,
        h1_seq        => 'initial',  # Mirrors this scope's send closure $seq: the
                                    # validator state machine for http, websocket
                                    # or sse (see _create_send,
                                    # _create_websocket_send, _create_sse_send)
        response_status  => undef,  # Track response status for logging
        _response_size   => 0,      # Track response body bytes for logging
        request_start    => undef,  # Track request start time for logging
        idle_timer    => undef,  # IO::Async::Timer for idle timeout
        _served_a_request => 0,  # True once a request has completed on this connection (idle_timer reason: idle_timeout -> keepalive_timeout)
        stall_timer   => undef,  # IO::Async::Timer for request stall timeout
        ws_idle_timer => undef,  # IO::Async::Timer for WebSocket idle timeout
        sse_idle_timer => undef, # IO::Async::Timer for SSE idle timeout
        # Event queue for $receive
        receive_queue   => [],
        receive_pending => undef,
        # Track all pending receive Futures to cancel on close
        receive_futures => [],
        # Track request handling Future to prevent "lost future" warning
        request_future  => undef,
        # Idempotency guard for disconnect handling
        _disconnect_handled => 0,
        # WebSocket state
        websocket_frame   => undef,  # Protocol::WebSocket::Frame for parsing
        # How this connection's current scope ended (see _record_end)
        end_reason => undef,  # Standard token for a server-decided or server-observed abnormal end
        end_detail => undef,  # Free-text supplement to end_reason
        end_code   => undef,  # WebSocket close code the disconnect event carries (1006 by default)
        # Keepalive state (protocol-level ping/pong for WebSocket, comments for SSE)
        ws_keepalive_timer  => undef,  # Periodic timer for sending WebSocket pings
        ws_pong_timeout     => undef,  # Timeout timer for pong response
        ws_waiting_pong     => 0,      # Flag: are we waiting for a pong?
        ws_keepalive_interval => 0,    # Current keepalive interval (0 = disabled)
        ws_keepalive_timeout  => 0,    # Current pong timeout (0 = no timeout check)
        sse_keepalive_timer => undef,  # Periodic timer for sending SSE keepalive comments
        sse_keepalive_comment => '',   # Comment text to send
        # HTTP/2 state
        alpn_protocol     => $args{alpn_protocol},    # ALPN-negotiated protocol (e.g. 'h2', 'http/1.1')
        h2_protocol       => $args{h2_protocol},      # PAGI::Server::Protocol::HTTP2 instance
        h2c_enabled       => $args{h2c_enabled} // 0, # Allow h2c preface detection on cleartext
        is_h2             => 0,                        # Set during start() if HTTP/2 detected
        h2_session        => undef,                    # PAGI::Server::Protocol::HTTP2::Session
        h2_streams        => {},                       # Per-stream state for HTTP/2
        h2_peer_goaway    => 0,                        # The peer announced GOAWAY on this connection
        # Transport info (tcp or unix)
        transport_type    => $args{transport_type} // 'tcp',
        transport_path    => $args{transport_path},  # socket path for unix
        # Cached connection info (populated in start(), used by _create_scope)
        client_host       => '127.0.0.1',
        client_port       => 0,
        server_host       => '127.0.0.1',
        server_port       => 5000,
    }, $class;

    # Extract TLS info if this is a TLS connection
    if ($self->{tls_enabled}) {
        $self->_extract_tls_info;
    }

    return $self;
}

use Socket qw(IPPROTO_TCP TCP_NODELAY);

sub start {
    my ($self) = @_;

    my $stream = $self->{stream};
    weaken(my $weak_self = $self);

    # Enable TCP_NODELAY to reduce latency for small responses (TCP only)
    my $handle = $stream->write_handle // $stream->read_handle;
    if ($self->{transport_type} eq 'tcp' && $handle && $handle->can('setsockopt')) {
        eval {
            $handle->setsockopt(IPPROTO_TCP, TCP_NODELAY, 1);
        };
        # Ignore errors - not all sockets support this
    }

    # Cache connection info once (avoids per-request socket method calls)
    if ($self->{transport_type} eq 'unix') {
        # Unix socket: no peer IP/port, server is identified by path
        $self->{client_host} = undef;
        $self->{client_port} = undef;
        $self->{server_host} = $self->{transport_path};
        $self->{server_port} = undef;
    } elsif ($handle && $handle->can('peerhost')) {
        eval {
            $self->{client_host} = $handle->peerhost // '127.0.0.1';
            $self->{client_port} = $handle->peerport // 0;
            $self->{server_host} = $handle->sockhost // '127.0.0.1';
            $self->{server_port} = $handle->sockport // 5000;
        };
        # Ignore errors - keep defaults if extraction fails
    }

    # Detect HTTP/2 via ALPN negotiation
    if ($self->{alpn_protocol} && $self->{alpn_protocol} eq 'h2' && $self->{h2_protocol}) {
        $self->_init_h2_session;
    }

    # Set up idle timeout timer
    $self->_start_idle_timer;

    # Set up read handler
    $stream->configure(
        on_read => sub  {
        my ($s, $buffref, $eof) = @_;
            return 0 unless $weak_self;

            # Reset idle timer on any read activity
            $weak_self->_reset_idle_timer;

            # Reset stall timer on read activity (if handling a request)
            $weak_self->_reset_stall_timer if $weak_self->{handling_request};

            $weak_self->{buffer} .= $$buffref;
            $$buffref = '';

            if ($eof) {
                # EOF means client closed - handle disconnect and cleanup
                $weak_self->_handle_disconnect_and_close('client_closed');
                return 0;
            }

            # h2c detection: check if cleartext connection starts with HTTP/2 preface
            if ($weak_self->{h2c_enabled} && !$weak_self->{is_h2}) {
                if (length($weak_self->{buffer}) >= 24) {  # HTTP/2 preface is 24 bytes
                    if ($weak_self->{h2_protocol} && PAGI::Server::Protocol::HTTP2->detect_preface($weak_self->{buffer})) {
                        $weak_self->_init_h2_session;
                        $weak_self->{h2c_enabled} = 0;  # Detection done
                    } else {
                        $weak_self->{h2c_enabled} = 0;  # Not h2c, stop checking
                    }
                } else {
                    # Not enough data yet to determine protocol, wait for more
                    return 0;
                }
            }

            # Wrap processing in eval to prevent exceptions from crashing the event loop
            # This is critical - Protocol::WebSocket::Frame can throw exceptions for
            # oversized payloads, and other parsing code may throw as well
            eval {
                # HTTP/2: feed data to session for frame processing
                if ($weak_self->{is_h2}) {
                    $weak_self->_h2_process_data;
                    return;
                }

                # If in WebSocket mode, process WebSocket frames
                if (_ws_handshake_accepted($weak_self->{h1_seq})) {
                    $weak_self->_process_websocket_frames;
                    return;
                }

                # If we're waiting for body data, notify the receive handler
                if ($weak_self->{receive_pending} && !$weak_self->{receive_pending}->is_ready) {
                    my $f = $weak_self->{receive_pending};
                    $weak_self->{receive_pending} = undef;
                    $f->done;
                }

                $weak_self->_try_handle_request;
            };
            if (my $error = $@) {
                # Log the error and close the connection gracefully
                $self->_log(error => "PAGI connection error: $error");
                return 0 unless $weak_self;
                $weak_self->_handle_disconnect_and_close('server_error',
                    detail => _detail_from_error($error));
            }
            return 0;
        },
        on_closed => sub {
            return unless $weak_self;
            # Stream closed - handle disconnect and remove from connections hash
            $weak_self->_handle_disconnect_and_close('client_closed');
        },
        # Without these, IO::Async::Stream's own contract applies: "If an
        # error occurs when the corresponding error callback is not
        # supplied, ... the close method is called instead" -- i.e. it would
        # call close_now itself and (via on_closed above) report every
        # socket error as client_closed, indistinguishable from an ordinary
        # peer disconnect. Registering a handler here -- regardless of what
        # it returns -- is sufficient by itself to suppress that close_now
        # (maybe_invoke_event always returns a truthy value once any handler
        # exists), so there is no double-teardown risk from IO::Async's
        # side; _handle_disconnect_and_close's own _disconnect_handled guard
        # covers the (harmless, pre-existing) case where on_closed also
        # fires afterward once the socket actually finishes closing.
        on_read_error => sub {
            my ($s, $errno) = @_;
            return unless $weak_self;
            # The errno IO::Async hands us, rendered the way the operator
            # will read it in a log line (Www.pod: disconnect_detail).
            local $! = $errno;
            $weak_self->_handle_disconnect_and_close('read_error', detail => "$!");
        },
        on_write_error => sub {
            my ($s, $errno) = @_;
            return unless $weak_self;
            local $! = $errno;
            $weak_self->_handle_disconnect_and_close('write_error', detail => "$!");
        },
    );
}

# Arm the idle timeout. Called once at connection setup (before any request
# has completed on this connection) and again whenever a long-lived mode
# that removed the timer (SSE) hands the connection back to ordinary
# keep-alive request handling, or a plain request completes and the
# connection stays open awaiting the next one. The same timer instance
# serves both cases (it is only reset, never re-created, by ordinary reads);
# on_expire decides its reason token at fire time from _served_a_request,
# per the spec's split between idle_timeout (nothing has arrived yet) and
# keepalive_timeout (a request already completed on this connection).
sub _start_idle_timer {
    my ($self) = @_;

    return if $self->{idle_timer};
    return unless $self->{timeout} && $self->{timeout} > 0 && $self->{server};

    weaken(my $weak_self = $self);

    my $timer = IO::Async::Timer::Countdown->new(
        delay => $self->{timeout},
        on_expire => sub {
            return unless $weak_self;
            return if $weak_self->{closed};
            # Close idle connection
            my $reason = $weak_self->{_served_a_request} ? 'keepalive_timeout' : 'idle_timeout';
            $weak_self->_handle_disconnect_and_close($reason,
                detail => "no traffic for $weak_self->{timeout}s");
        },
    );
    $self->{idle_timer} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _reset_idle_timer {
    my ($self) = @_;

    return unless $self->{idle_timer};

    # Debounce: rescheduling the IO::Async countdown on every read is costly
    # under keep-alive load -- it re-enqueues a loop timer each time. Reset at
    # most ~20x/second; this coarsens the idle timeout by at most ~50ms, which is
    # immaterial for a multi-second idle timeout but cuts the per-request timer
    # churn dramatically under load.
    my $now = Time::HiRes::time();
    return if defined $self->{_idle_reset_at} && ($now - $self->{_idle_reset_at}) < 0.05;
    $self->{_idle_reset_at} = $now;

    $self->{idle_timer}->reset;
    $self->{idle_timer}->start unless $self->{idle_timer}->is_running;
}

sub _stop_idle_timer {
    my ($self) = @_;

    return unless $self->{idle_timer};
    $self->{idle_timer}->stop if $self->{idle_timer}->is_running;
    # Remove timer completely so _reset_idle_timer won't restart it
    # This is important for long-lived connections (WebSocket, SSE)
    if ($self->{server}) {
        $self->{server}->remove_child($self->{idle_timer});
    }
    $self->{idle_timer} = undef;
}

# =============================================================================
# HTTP/2 Session Initialization
# =============================================================================

sub _init_h2_session {
    my ($self) = @_;

    $self->{is_h2} = 1;

    weaken(my $weak_self = $self);

    $self->{h2_session} = $self->{h2_protocol}->create_session(
        on_request => sub {
            my ($stream_id, $pseudo, $headers, $has_body) = @_;
            return unless $weak_self;
            $weak_self->_h2_on_request($stream_id, $pseudo, $headers, $has_body);
        },
        on_body => sub {
            my ($stream_id, $data, $eof) = @_;
            return unless $weak_self;
            $weak_self->_h2_on_body($stream_id, $data, $eof);
        },
        on_close => sub {
            my ($stream_id, $error_code) = @_;
            return unless $weak_self;
            $weak_self->_h2_on_close($stream_id, $error_code);
        },
        on_frame_sent => sub {
            my ($stream_id, $type, $flags) = @_;
            return unless $weak_self;
            $weak_self->_h2_on_frame_sent($stream_id, $type, $flags);
        },
        on_header_overflow => sub {
            my ($stream_id) = @_;
            return unless $weak_self;
            $weak_self->_h2_on_header_overflow($stream_id);
        },
        on_nghttp2_error => sub {
            my ($lib_error_code, $message) = @_;
            return unless $weak_self;
            $weak_self->_h2_record_nghttp2_diagnostic($message);
        },
        on_invalid_frame => sub {
            my ($stream_id, $type, $lib_error_code) = @_;
            return unless $weak_self;
            $weak_self->_h2_record_nghttp2_diagnostic(sprintf(
                'nghttp2 rejected a %s frame on stream %d (%s)',
                _h2_frame_name($type), $stream_id,
                _h2_lib_error_name($lib_error_code)));
        },
        # RFC 9113 section 6.8: a peer that sends GOAWAY is shutting the
        # connection down, and may do so while streams are still open --
        # nghttp2 keeps the session readable until the last of those closes.
        # So the peer's goodbye outlives the feed that carried it, and is kept
        # for as long as nghttp2 keeps its own record of it: the connection.
        on_goaway => sub {
            return unless $weak_self;
            $weak_self->{h2_peer_goaway} = 1;
        },
    );

    # Send initial SETTINGS to client
    $self->_h2_write_pending;
}

# RFC 9113 section 6 frame types, by wire value: what an operator reads in a
# disconnect_detail. Net::HTTP2::nghttp2 exports no names for these, so the
# RFC's own are used; it does export the library error codes, so those are
# looked up by the constant name the binding publishes rather than printed as
# the ABI numbers they are.
my @H2_FRAME_NAME = qw(DATA HEADERS PRIORITY RST_STREAM SETTINGS PUSH_PROMISE
                       PING GOAWAY WINDOW_UPDATE CONTINUATION);
my %H2_LIB_ERROR_NAME;

sub _h2_frame_name { return $H2_FRAME_NAME[$_[0]] // "frame type $_[0]" }

sub _h2_lib_error_name {
    my ($code) = @_;
    %H2_LIB_ERROR_NAME = map { Net::HTTP2::nghttp2->can($_)->() => $_ } qw(
        NGHTTP2_ERR_WOULDBLOCK NGHTTP2_ERR_DEFERRED NGHTTP2_ERR_PROTO
        NGHTTP2_ERR_STREAM_CLOSING NGHTTP2_ERR_HTTP_HEADER
        NGHTTP2_ERR_CALLBACK_FAILURE NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE
    ) unless %H2_LIB_ERROR_NAME;
    return $H2_LIB_ERROR_NAME{$code} // "nghttp2 error $code";
}

# nghttp2's message is the peer's own header names and values quoted back,
# bounded only by nghttp2's 4096-byte cap on the whole message. An application
# reads it as disconnect_detail and an operator reads it in a log line, so what
# is kept is one line's worth.
my $H2_DETAIL_MAX = 256;

sub _h2_bounded_detail {
    my ($message) = @_;
    return $message if !defined $message || length($message) <= $H2_DETAIL_MAX;
    return substr($message, 0, $H2_DETAIL_MAX - 3) . '...';
}

# What this feed learned about how the session is ending: nghttp2's account of
# the peer's frames. One hashref on the connection, cleared at the start of
# every feed and released with the connection -- nghttp2 has already decided
# what to do about a violation, and this only records how to describe the
# ending. The peer's own GOAWAY is not kept here; it is a fact about the
# connection rather than about one feed (h2_peer_goaway).
sub _h2_record_nghttp2_diagnostic {
    my ($self, $message) = @_;
    $self->{h2_feed}{detail} = _h2_bounded_detail($message);
    return;
}

sub _h2_process_data {
    my ($self) = @_;
    return unless $self->{h2_session};

    if (length($self->{buffer}) > 0) {
        delete $self->{h2_feed};
        $self->_h2_session_call(sub { $self->{h2_session}->feed($self->{buffer}) });
        $self->{buffer} = '';
    }

    $self->_h2_write_pending;

    # Close connection when session is done (GOAWAY received or sent)
    if ($self->{h2_session} && !$self->{h2_session}->want_read) {
        # Which party ended the session is what the ending is named for. A peer
        # that sent GOAWAY ended it itself, and anything nghttp2 said about the
        # peer's frames on the way is context, not a verdict: nghttp2 reports a
        # header field it merely ignores through the same callback and the same
        # NGHTTP2_ERR_HTTP_HEADER code as one it rejects, so the message cannot
        # decide. With no GOAWAY from the peer it was nghttp2 that ended the
        # session, and it only does that on a violation -- its diagnostic is
        # then the rule that was broken, which nghttp2 alone knows
        # (Compliance.pod "disconnect_detail contents"). The two facts are read
        # from where each one lives: a peer that announced GOAWAY is closing and
        # stays closing, so that is the connection's, while a diagnostic
        # describes the feed it arrived in and is dropped at the next one, so it
        # cannot blame a later, unrelated close.
        my $feed = $self->{h2_feed};
        if ($feed && defined $feed->{detail} && !$self->{h2_peer_goaway}) {
            $self->_log(error => "PAGI connection error (HTTP/2): $feed->{detail}");
            return $self->_handle_disconnect_and_close('protocol_error',
                detail => $feed->{detail});
        }
        $self->_handle_disconnect_and_close;
    }
}

# nghttp2 runs this server's callbacks from inside mem_recv and mem_send and
# forbids reentering either from there (Net::HTTP2::nghttp2::Session,
# "Reentrancy"): a flush reached from a callback croaks, while queueing frames
# there is legal and nghttp2 serializes them into the same flush. So every
# session call that can run callbacks -- feed's mem_recv and the extract below
# -- is made here, and the flush its callbacks asked for is left to the
# outermost one, on its way out. A receive woken inside a session call still
# resumes its application inline (Www.pod "Callback invocation context"); it is
# only that application's flush that waits.
sub _h2_session_call {
    my ($self, $code) = @_;
    $self->{h2_session_depth}++;
    my $ok  = eval { $code->(); 1 };
    my $err = $@;
    $self->{h2_session_depth}--;
    die $err unless $ok;
    $self->_h2_write_pending if !$self->{h2_session_depth} && delete $self->{h2_flush_wanted};
    return;
}

sub _h2_write_pending {
    my ($self) = @_;
    return unless $self->{h2_session};
    return $self->{h2_flush_wanted} = 1 if $self->{h2_session_depth};
    $self->_h2_session_call(sub {
        while (1) {
            my $data = $self->{h2_session}->extract;
            last unless defined $data && length($data) > 0;
            $self->{stream}->write($data);
        }
    });
}

# =============================================================================
# HTTP/2 Stream Callbacks
# =============================================================================

# RFC 9113 section 10.5.1: "a server that receives a larger header block
# than it is willing to handle can send an HTTP 431". Fired by the protocol
# layer (PAGI::Server::Protocol::HTTP2) instead of on_request when a
# request's HEADERS block exceeds max_header_list_size -- no request state
# was ever dispatched (HTTP2.pm never called on_request for this stream),
# so there is nothing in {h2_streams} to initialize or clean up here.
# Like the plain-CONNECT-501 and content-length-413 answers below, this runs
# inside feed's mem_recv and leaves its flush to _h2_session_call.
sub _h2_on_header_overflow {
    my ($self, $stream_id) = @_;

    $self->{h2_session}->submit_response($stream_id,
        status  => 431,
        headers => [
            ['content-type', 'text/plain'],
            ['date', $self->{protocol}->format_date],
        ],
        body    => "Request Header Fields Too Large\n",
    );
    $self->_h2_write_pending;
}

sub _h2_on_request {
    my ($self, $stream_id, $pseudo, $headers, $has_body) = @_;

    # Defensive second layer (belt under the protocol layer's own
    # suspenders, PAGI::Server::Protocol::HTTP2's HEADERS-block
    # classification): a live in-flight stream must never be overwritten
    # by a duplicate dispatch. This should be unreachable now that the
    # protocol layer classifies received HEADERS blocks by category and
    # only calls on_request for NGHTTP2_HCAT_REQUEST -- it exists so a
    # future protocol-layer regression can't silently destroy an
    # in-flight request's accumulated state (body, receive_queue,
    # connection_state, seq_state).
    if ($self->{h2_streams}{$stream_id}) {
        $self->_log(warn => "PAGI::Server::Connection: ignoring duplicate HTTP/2 request dispatch for stream $stream_id (existing in-flight request would have been destroyed)");
        return;
    }

    # Detect CONNECT method
    my $is_websocket = 0;
    if (($pseudo->{':method'} // '') eq 'CONNECT') {
        if (($pseudo->{':protocol'} // '') eq 'websocket') {
            # Extended CONNECT for WebSocket (RFC 8441)
            $is_websocket = 1;
        } else {
            # Plain CONNECT not supported.
            $self->{h2_session}->submit_response($stream_id,
                status  => 501,
                headers => [
                    ['content-type', 'text/plain'],
                    ['date', $self->{protocol}->format_date],
                ],
                body    => "CONNECT method not supported\n",
            );
            $self->_h2_write_pending;
            return;
        }
    }

    # Detect SSE (Accept: text/event-stream)
    my $is_sse = 0;
    if (!$is_websocket) {
        $is_sse = _accept_signals_sse($headers);
    }

    # Initialize per-stream state
    $self->{h2_streams}{$stream_id} = {
        pseudo    => $pseudo,
        headers   => $headers,
        has_body  => $has_body,
        body      => '',
        body_complete => !$has_body,
        body_pending  => undef,   # Future for body availability
        receive_queue => [],
        response_started => 0,
        seq_state => ($is_websocket ? 'connecting' : 'initial'),
        is_websocket => $is_websocket,
        is_sse       => $is_sse,
        ws_frame     => undef,   # Protocol::WebSocket::Frame for parsing
        ws_connect_sent => 0,
        ws_disconnect_delivered => 0,   # True once the scope's single websocket.disconnect has been queued
    };

    # Check Content-Length against max_body_size limit before dispatching
    # (after stream init so _h2_on_body/_h2_on_close can find the stream)
    if ($self->{max_body_size} && $has_body) {
        for my $h (@$headers) {
            if ($h->[0] eq 'content-length') {
                if ($h->[1] > $self->{max_body_size}) {
                    $self->{h2_session}->submit_response($stream_id,
                        status  => 413,
                        headers => [
                            ['content-type', 'text/plain'],
                            ['date', $self->{protocol}->format_date],
                        ],
                        body    => "Payload Too Large\n",
                    );
                    $self->_h2_write_pending;
                    return;
                }
                last;
            }
        }
    }

    # Defer dispatch to next event loop tick to prevent re-entrant nghttp2 calls
    weaken(my $weak_self = $self);
    $self->{server}->loop->later(sub {
        return unless $weak_self;
        return if $weak_self->{closed};
        # The same boundary the dispatch tail has, for the part of dispatch
        # that runs before the application: the scope, receive and send
        # builders throw here, where there is not yet a Future to carry the
        # failure and nothing but this eval between them and $loop->run.
        eval { $weak_self->_h2_dispatch_stream($stream_id); 1 }
            or $weak_self && $weak_self->_connection_handler_error("HTTP/2 stream $stream_id", $@);
    });
}

sub _h2_on_body {
    my ($self, $stream_id, $data, $eof) = @_;

    my $stream = $self->{h2_streams}{$stream_id};
    return unless $stream;

    if ($stream->{is_websocket} && _ws_handshake_accepted($stream->{seq_state})) {
        # WebSocket: DATA frames contain raw WebSocket frames
        $self->_h2_process_ws_frames($stream_id, $stream, $data) if length($data);

        if ($eof) {
            # END_STREAM with no close handshake (bare END_STREAM): abnormal
            # closure per RFC 6455 (Www.pod "Disconnect - receive event").
            # The client went away and named nothing, so the ending record's
            # own defaults -- 1006 and 'client_closed' -- are the answer.
            $self->_h2_end_ws_stream($stream);
        }
        return;
    }

    if (length($data) > 0) {
        $stream->{body} .= $data;

        # Enforce max_body_size (0 = unlimited)
        if ($self->{max_body_size} && length($stream->{body}) > $self->{max_body_size}) {
            # Which answer this overrun gets turns on one question the send
            # machine's own state already answers: has this scope started its
            # response? A 413 is a response, and a started response can never
            # be replaced by a second one (Www.pod "Application Left a Response
            # Incomplete"). HTTP/2 cannot even carry the attempt -- nghttp2
            # refuses a second data provider on a live stream and the croak
            # takes down the whole connection. So past that point the scope is
            # ended abnormally with the same Standard Disconnect Reason and the
            # stream is truncated the way every other incomplete response on
            # this transport is: RST_STREAM INTERNAL_ERROR, the other streams
            # on the connection untouched.
            my $kind = $stream->{is_websocket} ? 'websocket'
                     : $stream->{is_sse}       ? 'sse'
                     :                           'http';
            my $detail;
            if (PAGI::Server::EventValidator::scope_started($kind, $stream->{seq_state})) {
                $detail = _body_limit_after_start($self->{max_body_size});
                # The one report an application author gets on the operator
                # side, at the level of the incomplete-response line it stands
                # in for (the dispatch wrapper's, suppressed below by the
                # recorded reason) -- a truncated response must not go silent
                # under log_level 'error'.
                $self->_log(error => _body_limit_after_start(
                    $self->{max_body_size}, "HTTP/2 stream $stream_id"));
                # Before h2_closed below: that mark is exactly what
                # _h2_reset_stream's liveness test reads. The flush waits for
                # _h2_session_call on the way out of the mem_recv this runs in.
                $self->_h2_reset_stream($stream_id, _h2_rst_error_code());
            }
            else {
                $detail = "request body exceeded $self->{max_body_size} bytes";
                $self->{h2_session}->submit_response($stream_id,
                    status  => 413,
                    headers => [
                        ['content-type', 'text/plain'],
                        ['date', $self->{protocol}->format_date],
                    ],
                    body    => 'Payload Too Large',
                );
            }
            # Mark death BEFORE the two releases below and the wake further
            # down: each can resume an awaiting async sub synchronously, and a
            # resumed app may call $send while this entry is still live in
            # h2_streams (the carve-outs key off entry existence). h2_closed
            # lets every send closure treat a doomed-but-still-present entry as
            # an absent one, per the post-close contract (design §6.2 / §21
            # item 1), mirroring _h2_on_close's own marker.
            $stream->{h2_closed} = 1;
            # No flush here — _h2_process_data flushes after feed() returns
            $self->_h2_resolve_stream_drain_waiters($stream);
            $self->_h2_resolve_stream_trailer_wait($stream);
            # Drop (don't fire) the app's on_drain fires: the stream is closing,
            # not draining, and the transport handle is going away. Also break
            # the $stream <-> transport_state cycle (the handle's measure/arm
            # closures hold $stream strongly), or the stream state leaks for the
            # life of the process once h2_streams drops its external ref.
            $stream->{transport_drain_fires} = [];
            delete $stream->{transport_state};
            # This stream is going away here too -- stop its keepalive (and,
            # for SSE, idle) timers before dropping the hash entry, the same
            # as _h2_on_close does. Those timers are add_child'ed to the
            # SERVER, not to this stream state, so without this the deleted
            # entry below leaves them running for the life of the process:
            # _h2_on_close never runs for this path (no h2-level stream close
            # event fires here), and the connection's own close-time sweep
            # only iterates h2_streams, which no longer lists this stream.
            $self->_h2_stop_ws_keepalive($stream) if $stream->{is_websocket};
            if ($stream->{is_sse}) {
                $self->_h2_stop_sse_keepalive($stream);
                $self->_h2_stop_sse_idle_timer($stream);
            }

            # Unblock a pending receive() instead of leaving it hanging
            # forever: mark the body complete, queue this scope's disconnect
            # event, drive connection_state (every scope type attaches one,
            # since B2), then wake body_pending. Order matters: the queued
            # event must land BEFORE the wake so a parked receive() sees it
            # (both h2 receive closures check the queue first on resume),
            # the $cs mark below must land BEFORE the wake too (Www.pod
            # "State Transition Order": steps 1-4 precede step 5), and all
            # of this must happen BEFORE the delete below, after which the
            # stream state is unreachable.
            $stream->{body_complete} = 1;
            if ($stream->{is_sse}) {
                push @{$stream->{receive_queue}}, {
                    type   => 'sse.disconnect',
                    reason => 'body_too_large',
                };
            } elsif ($stream->{is_websocket}) {
                # An accepted ws stream returns early at the top of this sub
                # (the accepted-handshake guard above), so is_websocket true here
                # always means pre-accept. Deliver the scope's single
                # websocket.disconnect (Www.pod "Disconnect - receive
                # event": server-detected abnormal close is code 1006 plus
                # the matching Standard Disconnect Reasons token, and
                # body_too_large is one). Pushed directly rather than
                # through _h2_ws_enqueue_disconnect: that helper also calls
                # _h2_wake_pending, and the wake for this stream happens
                # once, further down, after every release (h2_closed is
                # already set above, so a resumed producer sees a dead
                # stream). Still honor the single-delivery contract so a
                # later delivery attempt for this (about-to-be-deleted)
                # stream is a no-op.
                $stream->{ws_disconnect_delivered}++;
                push @{$stream->{receive_queue}}, {
                    type   => 'websocket.disconnect',
                    code   => 1006,
                    reason => 'body_too_large',
                };
            } else {
                push @{$stream->{receive_queue}}, { type => 'http.disconnect' };
            }
            $stream->{connection_state}->_mark_disconnected('body_too_large', $detail)
                if $stream->{connection_state};
            $self->_h2_wake_pending($stream);

            delete $self->{h2_streams}{$stream_id};
            return;
        }
    }

    if ($eof) {
        $stream->{body_complete} = 1;
    }

    $self->_h2_wake_pending($stream);
}

sub _h2_wake_pending {
    my ($self, $stream) = @_;
    if ($stream->{body_pending} && !$stream->{body_pending}->is_ready) {
        my $f = $stream->{body_pending};
        $stream->{body_pending} = undef;
        $f->done;
    }
}

# End a websocket stream from the server side: record, mark, enqueue, wake,
# in that order (Www.pod "State Transition Order": the object is terminal
# before any pending receive is resumed, because the enqueue can resume a
# parked application synchronously). Every server-decided websocket end on
# HTTP/2 goes through here.
sub _h2_end_ws_stream {
    my ($self, $stream, %end) = @_;
    $self->_record_end($stream, %end);
    $stream->{connection_state}->_mark_disconnected($self->_end_reason($stream), $self->_end_detail($stream))
        if $stream->{connection_state};
    $self->_h2_ws_enqueue_disconnect($stream, $self->_end_code($stream), $self->_end_reason($stream));
    return;
}

# The sse twin: record, mark, enqueue sse.disconnect, wake, in that same
# order. PAGI delivers exactly one sse.disconnect per scope, so the
# single-delivery latch lives here.
sub _h2_end_sse_stream {
    my ($self, $stream, %end) = @_;
    $self->_record_end($stream, %end);
    $stream->{connection_state}->_mark_disconnected($self->_end_reason($stream), $self->_end_detail($stream))
        if $stream->{connection_state};
    push @{$stream->{receive_queue}}, { type => 'sse.disconnect', reason => $self->_end_reason($stream) }
        unless $stream->{sse_disconnect_delivered}++;
    $self->_h2_wake_pending($stream);
    return;
}

# Enqueue the scope's single websocket.disconnect event (PAGI: exactly one
# per WebSocket scope). Every h2 delivery site MUST come through here.
sub _h2_ws_enqueue_disconnect {
    my ($self, $stream, $code, $reason) = @_;
    return if $stream->{ws_disconnect_delivered}++;

    # Kept as well as queued. Www.pod "Disconnect - receive event": "Once this
    # event has been delivered the scope is over, and a further receive()
    # resolves with the same websocket.disconnect again". The ending record
    # cannot stand in for it here -- a peer's Close frame names its own RFC
    # code and its own reason TEXT, while the record's vocabulary is the
    # standard reason tokens the connection object reports -- so the receive
    # fallback re-delivers this event itself. A copy is queued, so an
    # application that edits the event it received cannot alter what a later
    # reader sees.
    $stream->{ws_disconnect_event} = {
        type   => 'websocket.disconnect',
        code   => $code,
        reason => $reason,
    };
    push @{$stream->{receive_queue}}, { %{ $stream->{ws_disconnect_event} } };
    $self->_h2_wake_pending($stream);
}

# The application completed the handshake: it sent websocket.accept. The send
# state says so on its own, on either transport -- advance_websocket reaches
# 'closed' only from 'accepted' (a websocket.close before accept is out of
# sequence, Www.pod "Close - send event"), and every other state is either the
# open handshake or a refusal. $state is h1_seq or the stream's seq_state.
sub _ws_handshake_accepted {
    my ($state) = @_;
    $state //= '';
    return ($state eq 'accepted' || $state eq 'closed') ? 1 : 0;
}

=head1 METHODS

=head2 is_long_lived

    if ($conn->is_long_lived) { ... }

True while this connection carries a scope that outlives an ordinary
request/response: an SSE scope, or a WebSocket whose handshake completed.
Such a connection never goes idle, so the server closes it explicitly at
shutdown instead of waiting out the drain timeout.

=cut

sub is_long_lived {
    my ($self) = @_;
    return 1 if ($self->{scope_kind} // '') eq 'sse';
    return _ws_handshake_accepted($self->{h1_seq});
}

=head2 has_requests_in_flight

    if ($conn->has_requests_in_flight) { ... }

True while this connection is still producing a response. HTTP/1.1 answers for
the one request it can carry at a time; HTTP/2 answers for its stream table,
and counts only C<http> streams -- a WebSocket or SSE stream never finishes on
its own, so it does not make a connection worth waiting for. A graceful
shutdown closes a connection that answers false at once and leaves one that
answers true to drain.

=cut

sub has_requests_in_flight {
    my ($self) = @_;
    if ($self->{is_h2}) {
        for my $stream (values %{$self->{h2_streams} // {}}) {
            return 1 if !$stream->{is_websocket} && !$stream->{is_sse};
        }
        return 0;
    }
    return $self->{handling_request} ? 1 : 0;
}

# Did this h2 WebSocket scope reach a clean end? Two ways, and Www.pod
# "Meaning per scope" names both: the accepted socket completed a closing
# handshake -- the application sent websocket.close (send state 'closed') or
# the peer's Close frame validated (ws_peer_closed) -- or the server finished
# its output of a refusal (send state 'refusal_complete').
#
# The accept conjunct binds only the handshake half; the refusal half is
# deliberately outside it, because a refusal never accepts. This is term for
# term the h1 twin at _handle_websocket_request's tail.
#
# A close this server initiated -- a protocol violation, a queue overflow --
# is NOT here: the spec calls it abnormal, and it records its own reason
# before the frame goes out (see _h2_ws_close).
sub _h2_ws_clean_end {
    my ($stream) = @_;
    my $seq = $stream->{seq_state} // '';
    return 1 if _ws_handshake_accepted($seq) && ($seq eq 'closed' || $stream->{ws_peer_closed});
    return 1 if $seq eq 'refusal_complete';
    return 0;
}

# How this scope ended. $scope is the h1 connection ($self) or an h2 stream
# hash. First-wins: the first server-decided reason recorded (idle_timeout,
# app_abort, protocol_error, ...) is never overwritten by a later, less
# specific one (server_error, client_closed). Www.pod "Meaning per scope",
# "Agreement with disconnect events": the connection object, the disconnect
# event, and the receive fallbacks all read this one record.
sub _record_end {
    my ($self, $scope, %end) = @_;
    $scope->{end_reason} //= $end{reason} if defined $end{reason};
    $scope->{end_detail} //= $end{detail} if defined $end{detail};
    $scope->{end_code}   //= $end{code}   if defined $end{code};
    return;
}

# $default names what an unended scope means at the asking site: an sse
# stream the application closed itself defaults to 'app_closed', every other
# reader to a transport that went away under it.
sub _end_reason { my ($self, $scope, $default) = @_; return $scope->{end_reason} // $default // 'client_closed' }
sub _end_detail { my ($self, $scope) = @_; return $scope->{end_detail} }
sub _end_code   { my ($self, $scope) = @_; return $scope->{end_code} // 1006 }

# A refusal this stream carried to completion -- the one ending that delivers
# no disconnect event at all (Www.pod "Disconnect - receive event"). See
# _h2_scope_end_event, which is where that fact is turned into an answer.
sub _h2_refusal_complete {
    my ($stream) = @_;
    return (($stream->{seq_state} // '') eq 'refusal_complete') ? 1 : 0;
}

# What a receive reports once the scope has ended cleanly by the application's
# own act (Www.pod "Receiving after the scope's end"). The sse event carries
# NO reason key -- the object was marked complete with no reason, and an
# absent key is the only encoding that agrees with it -- and a refused
# WebSocket handshake reports http.disconnect, the event belonging to the HTTP
# exchange that refusal was.
sub _sse_scope_end_event  { return { type => 'sse.disconnect' } }
sub _ws_refusal_end_event { return { type => 'http.disconnect' } }

# That same answer for one HTTP/2 stream, or undef while this scope has not so
# ended. The send machine's state is the whole fact -- it reaches these states
# only through the application's own terminal event -- so a server-decided end
# (shutdown, idle timeout) or a transport that goes away is never one of them.
# Nor is an accepted WebSocket's own close: that one delivers its
# websocket.disconnect through the receive queue. The h1 twins are the
# $clean_end and $refusal_over predicates in the h1 sse and websocket receives.
sub _h2_scope_end_event {
    my ($stream) = @_;
    return _h2_refusal_complete($stream) ? _ws_refusal_end_event() : undef
        if $stream->{is_websocket};
    my $kind = $stream->{is_sse} ? 'sse' : 'http';
    return undef
        unless PAGI::Server::EventValidator::scope_send_clean($kind, $stream->{seq_state});
    return $kind eq 'sse' ? _sse_scope_end_event() : { type => 'http.disconnect' };
}

# This scope's output has reached its clean terminal event, while the
# application is still running: mark the connection object complete, then hand
# a receive parked on this scope its answer. Every clean end on either
# transport goes through this pair -- the http scope's terminal event, a
# refusal on any scope, and an sse scope's own sse.close.
#
# Www.pod "Meaning per scope" puts the end at the server finishing its output,
# not at the application's return, so marking here is what stops a client reset
# arriving in between from taking the object somewhere else: "the first to
# occur wins", and the terminal state never reopens. The marks are idempotent,
# so _h2_on_close and the h1 request tail mark the same object again for
# nothing. The wake comes after the mark, never before, because the object must
# be terminal before a pending receive resumes (Www.pod "State Transition
# Order"): Future::AsyncAwait resumes an awaiting coroutine inline off ->done
# and its first act may be to read the object. That resumption lands inside the
# application's own terminal send, which Www.pod "Callback invocation context"
# allows for an answer that send produced.
sub _h2_end_scope_output {
    my ($self, $stream) = @_;
    $stream->{connection_state}->_mark_complete if $stream->{connection_state};
    $self->_h2_wake_pending($stream);
    return;
}

sub _h1_end_scope_output {
    my ($self) = @_;
    $self->{current_connection_state}->_mark_complete
        if $self->{current_connection_state};
    $self->_wake_receive_pending;
    return;
}

# Wake a receive() parked on this HTTP/1.1 connection. The h2 twin is
# _h2_wake_pending; both are called only after the connection object has been
# driven to its terminal state (see _h2_end_scope_output for why).
sub _wake_receive_pending {
    my ($self) = @_;
    if ($self->{receive_pending} && !$self->{receive_pending}->is_ready) {
        my $f = $self->{receive_pending};
        $self->{receive_pending} = undef;
        $f->done;
    }
    return;
}

sub _h2_on_close {
    my ($self, $stream_id, $error_code) = @_;

    my $stream = $self->{h2_streams}{$stream_id};
    return unless $stream;

    # Record death at the earliest possible moment -- before any of the
    # deferred work below (including this function's own loop->later
    # delete of the h2_streams entry further down). nghttp2 has already
    # forgotten this stream by the time this callback fires, but the
    # entry stays in h2_streams for one more tick so pending futures can
    # resolve. The dispatch wrapper's liveness check may run nested
    # inside this very call (a woken receive() future can resume an
    # app's async sub synchronously), so it needs a fact that flips true
    # HERE, not one that only becomes true once the deferred delete runs.
    $stream->{h2_closed} = 1;

    # This stream is going away one way or another -- stop its keepalive
    # (and, for SSE, idle) timers so they don't fire (or leak) after the
    # stream state is reclaimed.
    $self->_h2_stop_ws_keepalive($stream) if $stream->{is_websocket};
    if ($stream->{is_sse}) {
        $self->_h2_stop_sse_keepalive($stream);
        $self->_h2_stop_sse_idle_timer($stream);
    }

    # Drive this stream's own connection_state to its terminal state exactly
    # once. Three outcomes, and the h2 error code alone does not separate
    # them -- a server-sent END_STREAM closes a stream with error code 0 just
    # as a client's clean close does:
    #
    #   1. seq_state 'complete' and no error code -- the whole response went
    #      out and the stream ended cleanly: a completion, not a disconnect.
    #   2. no error code but the response never reached 'complete' -- the
    #      stream ended early with no h2-level error, which on this side only
    #      the server can cause. (A declared-but-unsent-trailers response, or
    #      any other response left started-but-incomplete, does NOT land
    #      here today: the dispatch wrapper resets it with RST_STREAM
    #      INTERNAL_ERROR -- a NONZERO code -- so that case falls into
    #      outcome 3 below. This branch is kept for any other server-caused
    #      clean end that leaves seq_state short of 'complete'.) An early
    #      end to an unfinished response is an incomplete response per the
    #      PAGI spec, and the fault is the server's: the client did nothing
    #      and must not be blamed with 'client_closed'.
    #   3. a nonzero error code -- the peer reset the stream (CANCEL,
    #      INTERNAL_ERROR, ...): the client went away.
    #
    # Both marks are idempotent, so a stream the dispatch wrapper already
    # marked (e.g. server_error before its own RST) keeps that first reason.
    #
    # A server-initiated per-stream teardown (idle timeout, keepalive
    # timeout, ...) records its own token BEFORE driving the close, so it
    # takes precedence over the generic 'client_closed' / 'server_error'
    # fallbacks below -- in both the zero-error-code and nonzero-error-code
    # $cs marks (outcomes 2 and 3), and in the queued disconnect events
    # pushed further down. WebSocket keepalive timeout and SSE idle timeout
    # are exactly the writers of that token, and both stream types attach a
    # connection_state, so the $cs mark and the queued event agree on the
    # same reason. The dispatch wrapper's own server-decided ends (no
    # response started, a scope left incomplete) record it too, for the same
    # reason.
    #
    # Every site below that needs "what token is this scope ending with"
    # asks the ending record rather than spelling a fallback out again. The
    # zero-error-code $cs mark is the one exception -- an early end with no
    # h2 error code is the server's own doing, so its fallback is
    # 'server_error', not 'client_closed'.
    if (my $cs = $stream->{connection_state}) {
        my $clean = $stream->{is_websocket}
                  ? _h2_ws_clean_end($stream)
                  : PAGI::Server::EventValidator::scope_send_clean(
                        ($stream->{is_sse} ? 'sse' : 'http'), $stream->{seq_state});
        if ($clean && !$error_code) {
            $cs->_mark_complete;
        } elsif (!$error_code) {
            # Record the token on the stream before marking, so the events
            # queued below carry the same reason as the object (spec:
            # Agreement with disconnect events). An earlier server-decided
            # token (idle timeout, abort) still wins.
            $self->_record_end($stream, reason => 'server_error',
                detail => 'stream ended before the response completed');
            $cs->_mark_disconnected($self->_end_reason($stream),
                $self->_end_detail($stream));
        } else {
            # The peer reset the stream. Record the h2 error code as this
            # scope's detail, so first-wins keeps anything more specific the
            # server already recorded and the object is marked from the
            # record like every other ending.
            $self->_record_end($stream,
                detail => sprintf('RST_STREAM error code %d', $error_code));
            $cs->_mark_disconnected($self->_end_reason($stream),
                $self->_end_detail($stream));
        }
    }

    # Mark body complete to unblock any pending receive
    $stream->{body_complete} = 1;

    # Enqueue disconnect event. A scope that completed a refusal delivers
    # none on either protocol (_h2_refusal_complete): it ended cleanly, and
    # the stream closing afterwards does not un-end it.
    if ($stream->{is_websocket}) {
        # Close without a WebSocket close handshake (RST_STREAM, timeout, ...):
        # abnormal closure per RFC 6455. Deduped -- a no-op if the close-frame
        # or bare-END_STREAM path already delivered the scope's one disconnect,
        # and the mark above already drove the object terminal.
        $self->_h2_end_ws_stream($stream) unless _h2_refusal_complete($stream);
    } elsif ($stream->{is_sse}) {
        $self->_h2_end_sse_stream($stream) unless _h2_refusal_complete($stream);
    } else {
        push @{$stream->{receive_queue}}, { type => 'http.disconnect' };
    }

    $self->_h2_wake_pending($stream);

    # Release any producer blocked on this stream's backpressure drain — the
    # stream is closing, so it must not hang waiting for a queue that will
    # never drain.
    $self->_h2_resolve_stream_drain_waiters($stream);
    # Same for a send() parked awaiting the data callback's own terminal
    # invocation to submit its staged trailers (h2_closed carve-out: a
    # trailers send racing a disconnect resolves as a successful no-op,
    # same as every other post-close send).
    $self->_h2_resolve_stream_trailer_wait($stream);
    # Drop (don't fire) the app's on_drain fires: this is a close, not a drain.
    # Also break the $stream <-> transport_state cycle so the stream state can be
    # collected once the deferred delete below drops h2_streams' external ref.
    $stream->{transport_drain_fires} = [];
    delete $stream->{transport_state};

    # Clean up after a delay (let any pending futures resolve)
    weaken(my $weak_self = $self);
    $self->{server}->loop->later(sub {
        return unless $weak_self;
        delete $weak_self->{h2_streams}{$stream_id};

        # The last stream of a connection the shutdown sweep left to drain.
        # Same rule as _h2_process_data's post-feed check -- a session that
        # wants no more input has nothing left to do -- applied at the one
        # other moment a stream can end with no inbound bytes to trigger it.
        # nghttp2 reports want_read false once its GOAWAY is sent and no
        # stream is active, so this can only fire after the sweep's
        # announcement. _handle_disconnect_and_close names the reason itself
        # (server_shutdown, from the shutting_down flag).
        return unless $weak_self->{server} && $weak_self->{server}{shutting_down};
        return if keys %{$weak_self->{h2_streams} // {}};
        return unless $weak_self->{h2_session} && !$weak_self->{h2_session}->want_read;
        $weak_self->_handle_disconnect_and_close;
    });
}

# =============================================================================
# HTTP/2 Stream Dispatch (scope/receive/send creation)
# =============================================================================

sub _h2_dispatch_stream {
    my ($self, $stream_id) = @_;

    my $stream_state = $self->{h2_streams}{$stream_id};
    return unless $stream_state;

    my ($scope, $receive, $send);

    if ($stream_state->{is_websocket}) {
        $scope   = $self->_h2_create_websocket_scope($stream_id, $stream_state);
        $receive = $self->_h2_create_websocket_receive($stream_id, $stream_state);
        $send    = $self->_h2_create_websocket_send($stream_id, $stream_state);
    } elsif ($stream_state->{is_sse}) {
        $scope   = $self->_h2_create_sse_scope($stream_id, $stream_state);
        $receive = $self->_h2_create_sse_receive($stream_id, $stream_state);
        $send    = $self->_h2_create_sse_send($stream_id, $stream_state);
    } else {
        $scope   = $self->_h2_create_scope($stream_id, $stream_state);
        $receive = $self->_h2_create_receive($stream_id, $stream_state);
        $send    = $self->_h2_create_send($stream_id, $stream_state);
    }

    weaken(my $weak_self = $self);

    my $future = (async sub {
        eval {
            await $weak_self->{app}->($scope, $receive, $send);
        };
        my $error = $@;

        # The boundary HTTP/1.1 has had all along, now around HTTP/2's own
        # dispatch tail: everything from here on is the SERVER's code, the
        # application having had its own eval above. An exception from it used
        # to fail the adopted dispatch Future, and a failed adopted Future
        # reaches IO::Async::Notifier::invoke_error, which -- with no on_error
        # on PAGI::Server and no parent notifier -- is a bare die out of
        # $loop->run, so one connection's fault ended the process. One eval
        # frame per request; nothing per frame and nothing per receive.
        eval {
            if ($weak_self) {
                my $cs = $stream_state->{connection_state};

                # Client-already-gone carve-out: if the client reset this stream
                # (or the whole connection tore down) while the app's Future was
                # still pending, _h2_on_close already ran and drove $cs to an
                # ABNORMAL terminal state. There is then nothing left to report:
                # no synthesized 500, no incomplete-response RST, no warning
                # about a stream the client no longer cares about.
                #
                # The test is "abnormal", not merely "terminal": a clean
                # _mark_complete also leaves $cs disconnected, and it happens
                # routinely BEFORE the app returns (the send closure flushes
                # END_STREAM synchronously, so _h2_on_close -> _mark_complete
                # normally runs inside the app's final await). Keying off
                # is_connected would swallow every exception thrown after a
                # fully-delivered response. An abnormal end has a
                # disconnect_reason; a clean completion leaves it undef.
                # Every stream (http, websocket, sse) attaches a connection_state
                # today, so $cs is always defined here; the _h2_stream_alive
                # fallback below exists only as a defensive default for the
                # theoretical case of a stream with none.
                #
                # A recorded reason of 'server_error' is NOT a "scope already
                # ended" signal: it is what this very dispatch wrapper's
                # incomplete-response branches below record before resetting the
                # stream with RST_STREAM INTERNAL_ERROR, so it must still warn.
                # Every other server-decided end (idle_timeout, keepalive_timeout,
                # protocol_error, queue_overflow, server_shutdown, app_abort) marks
                # the object with its own token BEFORE the app's pending receive is
                # woken (State Transition Order), so by the time the app returns
                # the scope has already ended with a reason of its own; those ends
                # take the carve-out exactly like a client disconnect and do not
                # warn.
                #
                # That exception is a per-STREAM one. Once the CONNECTION itself
                # has ended, its h2 sweep has stamped server_error on every stream
                # it caught in flight, and a connection-level end is quiet for
                # those scopes exactly as server_shutdown and protocol_error are:
                # the ending is recorded and logged once, for the connection, and
                # an application whose connection was destroyed under it never had
                # the chance to start the response it would otherwise be accused
                # of withholding.
                my $stream_alive = $weak_self->_h2_stream_alive($stream_id);
                my $reason = $cs ? $cs->disconnect_reason : undef;
                my $connection_ended = $weak_self->{closed} || $weak_self->{_disconnect_handled};
                my $scope_already_ended =
                    $cs ? (defined($reason) && ($connection_ended || $reason ne 'server_error'))
                        : !$stream_alive;

                unless ($scope_already_ended) {
                    if (!$stream_state->{response_started}) {
                        # If the application failed, OR returned without starting a
                        # response, synthesize a 500 (only possible while no response
                        # has begun). A clean return that produced no response is a
                        # protocol error, same as a throw.
                        $self->_log(error => $error
                            ? "PAGI application error (HTTP/2 stream $stream_id): $error"
                            : "PAGI application returned without starting a response (HTTP/2 stream $stream_id)");
                        # Mark BEFORE the response settles: submitting it (below) can
                        # complete the stream synchronously once flushed, and
                        # _h2_on_close would then mark this same connection_state
                        # terminal if it got there first. _mark_disconnected is
                        # idempotent -- first mark wins -- so marking here first
                        # guarantees the app observes server_error. The synthesized
                        # 500 is this stream's response (spec section 9.1), so mark
                        # it started FIRST: _mark_disconnected fires on_disconnect
                        # callbacks synchronously, and they must observe
                        # response_started true.
                        #
                        # Record the reason on the stream first, the way every other
                        # server-decided per-stream end does (keepalive timeout, sse
                        # idle timeout, protocol close, abort). This is the fact
                        # _h2_on_close and both receive fallbacks read when they
                        # synthesize this scope's disconnect event; without it they
                        # fall back to 'client_closed' and the event contradicts the
                        # object, which Www.pod "Agreement with disconnect events"
                        # forbids.
                        my $no_response_detail = $error ? _detail_from_error($error)
                                                       : 'no response was started';
                        $weak_self->_record_end($stream_state, reason => 'server_error',
                            detail => $no_response_detail);
                        $cs->_mark_response_started if $cs;
                        $cs->_mark_disconnected('server_error', $no_response_detail) if $cs;
                        # The stream may already be gone (a zero-error close marked it
                        # server_error and the entry is doomed-but-present); nghttp2 must
                        # not be asked to answer a stream it has released.
                        eval {
                            $weak_self->{h2_session}->submit_response($stream_id,
                                status  => 500,
                                headers => [
                                    ['content-type', 'text/plain'],
                                    ['date', $weak_self->{protocol}->format_date],
                                ],
                                body    => "Internal Server Error\n",
                            );
                            $weak_self->_h2_write_pending;
                        } if $stream_alive;
                    }
                    elsif ($stream_state->{is_websocket} || $stream_state->{is_sse}) {
                        # The generic incomplete-response test below is keyed on the
                        # http state name 'complete', which neither of these scopes
                        # can reach. Ask each scope the same two questions against
                        # its own mirrored state instead (Www.pod "Meaning per
                        # scope": a stream abandoned without its own clean end).
                        #
                        # "Started" is scope_started, not the streaming branch alone:
                        # Www.pod counts a refusal's http.response.start as the
                        # scope's start, and a refusal abandoned before its terminal
                        # event is an incomplete response exactly like an abandoned
                        # stream ("Application Left a Response Incomplete" names the
                        # refusal case first).
                        my $seq_now = $stream_state->{seq_state};
                        my $kind    = $stream_state->{is_websocket} ? 'websocket' : 'sse';
                        my $started = PAGI::Server::EventValidator::scope_started($kind, $seq_now);
                        my $clean   = $stream_state->{is_websocket}
                                    ? _h2_ws_clean_end($stream_state)
                                    : PAGI::Server::EventValidator::scope_send_clean('sse', $seq_now);
                        if ($started && !$clean) {
                            $weak_self->_h2_incomplete_scope_end($stream_id, $stream_state, $error);
                        }
                        elsif ($clean) {
                            # Clean end -- a completed closing handshake, sse.close,
                            # or a completed refusal on either scope. _h2_on_close
                            # ordinarily marks this already; mark here too in case
                            # this coroutine resumed first -- idempotent.
                            $cs->_mark_complete if $cs;
                            $self->_log(error => 'PAGI application error after '
                                . _h2_scope_verb($kind, $seq_now)
                                . " ended cleanly (HTTP/2 stream $stream_id): $error")
                                if $error;
                        }
                        # else ($started false, $clean false): the application never
                        # accepted, never started a stream, and never refused. The
                        # !response_started arm above has already answered that case.
                    }
                    elsif ($cs && (($stream_state->{seq_state} // 'complete') ne 'complete')) {
                        # Response started but never finished: the app either
                        # returned cleanly without sending the terminal body/file/fh
                        # (or trailers), or threw after starting. Either way the
                        # stream is framed but unterminated -- there is no way to
                        # synthesize END_STREAM without lying about the body, so
                        # reset the stream instead. Only plain HTTP streams reach
                        # here: WebSocket/SSE are handled by their own arms above.
                        # Trailers-specific parenthetical mirrors the h1 wording (see
                        # _handle_request's incomplete branch) -- appended after the
                        # stream-id parenthetical so t/http2/24-incomplete-response.t's
                        # "...incomplete response (HTTP/2 stream $stream_id)" pin
                        # still matches as a contiguous substring.
                        my $trailers_note = (($stream_state->{seq_state} // '') eq 'awaiting_trailers')
                            ? ' (trailers were declared but never sent)' : '';
                        $self->_log(error => $error
                            ? "PAGI application error after response started (HTTP/2 stream $stream_id): $error"
                            : "PAGI application returned with an incomplete response (HTTP/2 stream $stream_id)$trailers_note");
                        # Mark BEFORE the RST. _h2_on_close fires for our own RST
                        # too (as an abnormal close, since seq_state never reached
                        # 'complete') and would mark this same connection_state
                        # 'client_closed' if it got there first. _mark_disconnected
                        # is idempotent -- first mark wins -- so marking here first
                        # guarantees the app observes server_error.
                        $cs->_mark_disconnected('server_error',
                            $error ? _detail_from_error($error)
                                    : (($stream_state->{seq_state} // '') eq 'awaiting_trailers'
                                        ? 'trailers were declared but never sent'
                                        : 'response started but never completed'));
                        $weak_self->_h2_reset_stream($stream_id, _h2_rst_error_code());
                    }
                    elsif ($error) {
                        # Response already complete; cannot send a 500 or usefully
                        # reset a finished stream. Log only.
                        $self->_log(error => "PAGI application error after response started (HTTP/2 stream $stream_id): $error");
                    }
                }
            }

            # Notify server that request completed (for max_requests tracking)
            $weak_self->{server}->_on_request_complete if $weak_self && $weak_self->{server};
            1;
        } or do {
            $self->_connection_handler_error("HTTP/2 stream $stream_id", $@);
        };
    })->();

    $self->{server}->adopt_future($future);
}

# What every connection-handler boundary does with what it caught -- the two
# HTTP/2 dispatch sites and the three HTTP/1.1 request tails -- and what the
# HTTP/1.1 read handler's eval has always done with what it catches: name the
# fault once, $where naming where the fault came from -- the literal 'HTTP/1.1'
# at all three h1 sites, "HTTP/2 stream N" at the two h2 ones -- then end this
# connection with server_error and the exception as its disconnect_detail
# (Www.pod "Standard Disconnect Reasons").
#
# The close is the default close_when_empty, not close_now: a response the
# application had already delivered in full before the server's tail failed
# still reaches the peer, exactly as it does on HTTP/1.1 where those bytes are
# already in the stream's buffer, and _close reserves close_now for app_abort.
sub _connection_handler_error {
    my ($self, $where, $error) = @_;
    $self->_log(error => "PAGI connection handler error ($where): $error");
    $self->_handle_disconnect_and_close('server_error', detail => _detail_from_error($error));
    return;
}

# The event that started this scope, for the messages that name it. A
# refusal's http.response.start counts as the scope's start (Www.pod
# "Application Left a Response Incomplete" names the refusal case first).
sub _h2_scope_verb {
    my ($kind, $seq) = @_;
    my %verb = (
        refusing  => 'http.response.start',
        websocket => 'WebSocket accept',
        sse       => 'sse.start',
    );
    return $verb{ (($seq // '') eq 'refusing') ? 'refusing' : $kind };
}

# An accepted websocket or started sse stream the application left without
# its terminal event (Www.pod "Application Left a Response Incomplete").
sub _h2_incomplete_scope_end {
    my ($self, $stream_id, $stream_state, $error) = @_;

    my $kind   = $stream_state->{is_websocket} ? 'websocket' : 'sse';
    my $seq    = $stream_state->{seq_state};
    my $verb   = _h2_scope_verb($kind, $seq);
    my $detail = $error ? _detail_from_error($error)
                        : "started with $verb but never ended cleanly";
    $self->_log(error => $error
        ? "PAGI application error after $verb (HTTP/2 stream $stream_id): $error"
        : "PAGI application returned after $verb without ending the scope cleanly (HTTP/2 stream $stream_id)");

    if ($kind eq 'websocket' && _ws_handshake_accepted($seq)) {
        # An accepted socket the application walked away from. Www.pod names
        # one wire form for both transports: "On an accepted WebSocket the
        # server sends a Close frame with code 1011 ... and then closes the
        # transport (the stream, on HTTP/2)". So this is the h1 tail's twin,
        # not an RST: _h2_ws_close queues the Close frame and sets
        # ws_eof_pending, so the data callback puts END_STREAM on that same
        # chunk -- exactly what the application's own websocket.close does.
        # An RST here would instead tell the peer nothing about why the
        # session ended. The same call ends the scope: it records this
        # server's reason and this detail, marks the object from the record,
        # and delivers the scope's one disconnect event with the code that
        # just went out.
        #
        # A refusal abandoned mid-response is NOT this case (no socket was
        # ever accepted, so there is no Close frame to send); it takes the
        # RST below, which is what the spec prescribes for an incomplete
        # response on any ordinary HTTP/2 stream.
        $self->_h2_ws_close($stream_id, code => 1011, text => '',
            reason => 'server_error', detail => $detail);
        # Runs off the application's Future; the queued frame needs a flush.
        $self->_h2_write_pending;
    }
    else {
        # The scope is ending for this server's own reason, so it records that
        # reason on the stream BEFORE the mark, the way every other
        # server-decided end does. _h2_on_close and both receive fallbacks
        # read the record when they synthesize the scope's disconnect event;
        # without it they say 'client_closed' while this object says
        # 'server_error', which Www.pod "Agreement with disconnect events"
        # forbids.
        $self->_record_end($stream_state, reason => 'server_error', detail => $detail);
        $stream_state->{connection_state}->_mark_disconnected(
            $self->_end_reason($stream_state), $self->_end_detail($stream_state))
            if $stream_state->{connection_state};
        $self->_h2_reset_stream($stream_id, _h2_rst_error_code());
    }
    return;
}

# True while nghttp2 still owns the stream. _h2_on_close marks h2_closed the
# instant it runs but defers deleting the h2_streams entry one loop turn so
# pending futures can resolve; in that window the entry exists but the stream
# is gone, and any call into nghttp2 for it must be skipped. Entry-existence
# alone is not the fact: waking a receive() parked on body_pending can resume
# an application synchronously from inside _h2_on_close, so an app can reach
# a liveness test before the deferred delete has run.
sub _h2_stream_alive {
    my ($self, $stream_id) = @_;
    my $ss = $self->{h2_session} ? $self->{h2_streams}{$stream_id} : undef;
    return ($ss && !$ss->{h2_closed}) ? 1 : 0;
}

# Reset a stream the server still owns. nghttp2 accepts a reset for a stream
# it has already released and corrupts at session teardown, so every RST_STREAM
# this server sends is submitted here, behind the liveness check.
sub _h2_reset_stream {
    my ($self, $stream_id, $code) = @_;
    return unless $self->_h2_stream_alive($stream_id);
    eval {
        $self->{h2_session}->submit_rst_stream($stream_id, $code);
        $self->_h2_write_pending;
    };
    return;
}

# nghttp2 has serialized a frame of ours: an END_STREAM on it has claimed its
# place in the output, which is the one moment a reset cannot race the peer's
# flow-control window and truncate the response behind it. RFC 9113 section
# 8.1 lets a server that has sent a complete response ask the client to abort
# the rest of its request with RST_STREAM NO_ERROR; without that, a client that
# never finishes its request body holds a finished stream open indefinitely and
# the scope that already ended never sees it close. One rule for every producer
# of an END_STREAM: submit_response, the data callback's terminal chunk,
# submit_trailer.
sub _h2_on_frame_sent {
    my ($self, $stream_id, $type, $flags) = @_;
    # END_STREAM (0x1) means end-of-stream only on HEADERS and DATA; the same
    # bit is ACK on SETTINGS and PING, which carry stream id 0.
    return unless $stream_id
        && ($type == Net::HTTP2::nghttp2::NGHTTP2_DATA()
         || $type == Net::HTTP2::nghttp2::NGHTTP2_HEADERS())
        && ($flags & Net::HTTP2::nghttp2::NGHTTP2_FLAG_END_STREAM());
    # An accepted WebSocket is the one stream whose open half is not an
    # unfinished request: RFC 8441 section 5 makes an orderly close an
    # END_STREAM from each side and reserves RST_STREAM for the exception
    # path. RFC 9113 section 8.1 offers the reset for a response the server
    # completed before the request, which a tunnel the application closed by
    # its own handshake is not. RFC 9113 section 8.5 expects the peer to send
    # its own END_STREAM once it has received one, so nothing has to abandon
    # that half. A refused handshake never accepted: it is the ordinary HTTP
    # response section 8.1 describes, and it resets like any other.
    my $stream = $self->{h2_streams}{$stream_id};
    return if $stream && $stream->{is_websocket}
        && _ws_handshake_accepted($stream->{seq_state});
    # 1 = the request half is finished and the stream is closing on its own;
    # undef = nghttp2 no longer has this stream at all.
    my $request_finished = $self->{h2_session}->get_stream_remote_close($stream_id);
    return if !defined $request_finished || $request_finished;
    $self->_h2_reset_stream($stream_id, _h2_rst_no_error_code());
    return;
}

# The RST_STREAM error codes this server sends, each named for what it means.
# All three are exported by Net::HTTP2::nghttp2.
#
#   INTERNAL_ERROR  a response left started but incomplete (RFC 9113 section 7,
#                   "an unexpected condition"; see _h2_dispatch_stream)
#   CANCEL          a stream the server no longer needs and nobody's fault
#                   (section 7; RFC 8441 section 5 names it for a WebSocket)
#   NO_ERROR        a complete response the server finished before the client
#                   finished its request (section 8.1) -- the zero code
#                   _h2_on_close reads as a clean close
sub _h2_rst_error_code    { return Net::HTTP2::nghttp2::NGHTTP2_INTERNAL_ERROR() }
sub _h2_rst_cancel_code   { return Net::HTTP2::nghttp2::NGHTTP2_CANCEL() }
sub _h2_rst_no_error_code { return Net::HTTP2::nghttp2::NGHTTP2_NO_ERROR() }

# Teardown hook for one h2 stream: reset only that stream. The object is
# already app_abort; _h2_on_close's marks are idempotent so the first reason
# wins. Mark before the RST for the same reason the incomplete-response path
# does (see _h2_dispatch_stream).
sub _h2_abort_hook {
    my ($self, $stream_id) = @_;
    weaken(my $weak_self = $self);
    return sub {
        my ($cs, $detail) = @_;
        return unless $weak_self && $weak_self->{h2_session};
        my $ss = $weak_self->{h2_streams}{$stream_id} or return;
        return if $ss->{h2_closed};
        $weak_self->_record_end($ss, reason => 'app_abort', detail => $detail);
        $weak_self->_h2_reset_stream($stream_id, _h2_rst_cancel_code());
    };
}

sub _h2_create_scope {
    my ($self, $stream_id, $stream_state) = @_;

    my $pseudo  = $stream_state->{pseudo};
    my $headers = $stream_state->{headers};

    # Parse path and query string from :path pseudo-header
    my $full_path = $pseudo->{':path'} // '/';
    my ($path, $query_string) = split(/\?/, $full_path, 2);
    $query_string //= '';

    # Decode percent-encoded path for scope (keep raw_path as-is)
    # Match HTTP/1.1 pipeline: URI::Escape + UTF-8 decode with fallback
    my $unescaped = uri_unescape($path);
    my $decoded_path = eval { decode('UTF-8', $unescaped, Encode::FB_CROAK) }
                       // $unescaped;

    my $connection_state = PAGI::Server::ConnectionState->new(
        connection => $self,
        server     => $self->{server},
        on_abort   => $self->_h2_abort_hook($stream_id),
    );
    # Store on the stream-state so the send path can mark response_started on
    # this stream's own connection object (h2 multiplexes many streams).
    $stream_state->{connection_state} = $connection_state;

    return {
        type         => 'http',
        pagi         => {
            version      => '0.5',
            spec_version => '0.6',
        },
        http_version => '2',
        method       => $pseudo->{':method'} // 'GET',
        scheme       => $pseudo->{':scheme'} // $self->_get_scheme,
        path         => $decoded_path,
        raw_path     => $path,
        query_string => $query_string,
        root_path    => '',
        headers      => $headers,
        (defined $self->{client_host}
            ? (client => [$self->{client_host}, $self->{client_port}])
            : ()
        ),
        server       => [$self->{server_host}, $self->{server_port}],
        state        => keys %{$self->{state}} ? { %{$self->{state}} } : {},
        extensions   => $self->_get_extensions_for_scope,
        'pagi.connection' => $connection_state,
        # h2 transport handle measures THIS stream's send queue (per-stream),
        # stored on the stream state rather than $self->{current_transport_state}
        # because h2 multiplexes many concurrent streams over one connection.
        'pagi.transport'  => ($stream_state->{transport_state} = $self->_h2_transport_state($stream_state)),
    };
}

sub _h2_create_receive {
    my ($self, $stream_id, $stream_state) = @_;

    weaken(my $weak_self = $self);

    # This scope's cap record and its gate (see _disconnect_receive_future),
    # closure-local so it outlives the h2_streams entry.
    my %cap = (scope => 'http', transport => "HTTP/2 stream $stream_id", count => 0);
    my $disconnect = sub {
        my ($parked) = @_;
        return Future->done({ type => 'http.disconnect' }) unless $weak_self;
        return $weak_self->_disconnect_receive_future(
            \%cap, { type => 'http.disconnect' }, $parked);
    };

    # This scope's clean end, asked ahead of the transport's state everywhere,
    # because it is the scope's ending that decides what a call reports (Www.pod
    # "Receiving after the scope's end"). The websocket and sse closures ask the
    # same predicate; the h1 twin is _create_receive's own $scope_ended.
    my $scope_ended = sub { _h2_scope_end_event($stream_state) ? 1 : 0 };

    return sub {
        return Future->done({ type => 'http.disconnect' }) unless $weak_self;
        return $disconnect->() if $weak_self->{closed};

        my $ss = $weak_self->{h2_streams}{$stream_id};
        return $disconnect->() unless $ss;

        my $future = (async sub {
            return { type => 'http.disconnect' } unless $weak_self;

            # True once this call has waited: the answer it then gets is a
            # delivery of the scope's terminal state, not a repeat request
            # for it, so the cap does not count it.
            my $parked = 0;

            my $ss = $weak_self->{h2_streams}{$stream_id};
            return await $disconnect->($parked) unless $ss;

            # Check queue first
            if (@{$ss->{receive_queue}}) {
                return shift @{$ss->{receive_queue}};
            }

            # The scope has ended: report the end, not the request body, read
            # or not (Www.pod "Receiving after the scope's end"). Asked AFTER
            # the queue, because the stream closing behind the terminal event
            # queues this scope's own http.disconnect and handing that one over
            # is a delivery -- the cap counts only what the server has to
            # invent (t/75 pins the free first delivery).
            return await $disconnect->($parked) if $scope_ended->();

            # If body is already complete, return the final body event --
            # once. A receive called after the terminal event parks in the
            # wait loop below until the stream ends, matching h1's
            # post-request receive contract (stream close queues
            # http.disconnect and wakes body_pending). Same one-shot
            # discipline as the SSE closure's sse_request_sent.
            if ($ss->{body_complete} && !$ss->{final_request_delivered}) {
                $ss->{final_request_delivered} = 1;
                my $body = $ss->{body};
                $ss->{body} = '';
                return {
                    type => 'http.request',
                    body => $body,
                    more => 0,
                };
            }

            while (1) {
                # The scope ended under this call: answered from another
                # Future while this one waited for the rest of a request body.
                return await $disconnect->($parked) if $scope_ended->();

                # This stream's close handler has already run. Its h2_streams
                # entry is still here -- dropped one turn later so pending
                # futures can resolve -- but nothing will ever wake
                # body_pending again, so parking would strand this call on a
                # Future that drop then orphans, and Spec.pod "Cancellation
                # and Disconnects" forbids leaving a receive unresolved once
                # the disconnect is known. The connection-close sweep leaves
                # body_pending READY rather than undef, so without this the
                # loop below re-awaits a resolved Future and spins.
                return await $disconnect->($parked)
                    unless $weak_self->_h2_stream_alive($stream_id);

                # Wait for body data (or, once the request has been fully
                # delivered, for the stream to end)
                if (!$ss->{body_pending}) {
                    $ss->{body_pending} = Future->new;
                }
                $parked = 1;
                await $ss->{body_pending};

                # Re-fetch stream state (may have changed). A clean end is
                # answered at the top of the loop, which never touches $ss.
                $ss = $weak_self->{h2_streams}{$stream_id};
                return await $disconnect->($parked) unless $ss;

                # Check queue after waking -- a queued event wins over the
                # body fallthrough (a close can set body_complete AND queue
                # http.disconnect on the same wake)
                if (@{$ss->{receive_queue}}) {
                    return shift @{$ss->{receive_queue}};
                }

                # The scope ended while this call was parked: same rule,
                # same order, as the head of this closure.
                return await $disconnect->($parked) if $scope_ended->();

                # Terminal event already delivered: this wake brought
                # nothing for the application -- park again rather than
                # re-synthesize the final body event.
                next if $ss->{final_request_delivered};

                my $more = $ss->{body_complete} ? 0 : 1;
                $ss->{final_request_delivered} = 1 unless $more;
                my $body = $ss->{body};
                $ss->{body} = '';
                return {
                    type => 'http.request',
                    body => $body,
                    more => $more,
                };
            }
        })->();

        return $future;
    };
}

sub _h2_create_send {
    my ($self, $stream_id, $stream_state, %opt) = @_;

    # Refusing a websocket handshake or an sse stream delegates its wire work
    # here so the response is identical to the same response on an http scope
    # (Www.pod "Refusing the handshake" / "Refusing the stream"). Nothing on
    # this transport turns on it -- HTTP/2 has no Connection header and no
    # keep-alive to suppress -- but the h1 twin, _create_send, takes the same
    # option and does act on it.
    my $is_refusal = $opt{refusal};

    weaken(my $weak_self = $self);

    # Publish the closure-local $seq where the scope's owner reads it: the
    # stream for an http scope, and for a refusal the refusing scope's own
    # send closure, which passes its own publisher.
    my $publish = $opt{on_state} // sub {
        my $ss = $weak_self ? $weak_self->{h2_streams}{$stream_id} : undef;
        $ss->{seq_state} = $_[0] if $ss;
    };

    my $status;
    my @response_headers;

    # Streaming state for deferred data provider pattern.
    # The send queue lives on per-stream state ($ss->{send_queue} /
    # $ss->{send_queue_bytes}) so the h2 transport handle can measure it;
    # $eof_pending / $streaming_started stay closure-local.
    my $eof_pending = 0;
    my $streaming_started = 0;
    # Mirrors the stream state's own starting point (see the seq_state =>
    # 'initial' initializer in _h2_on_request); the two must stay in step.
    my $seq = 'initial';
    my $is_head = (($stream_state->{pseudo}{':method'} // '') eq 'HEAD');
    # Set once, from http.response.start's own 'trailers' flag (section 6
    # below), and never changed again. $data_callback's no_end computation
    # MUST key off this rather than $ss->{seq_state}: the trailers arm
    # advances that mirror to 'complete' as soon as the app's send() call
    # is made, which can be BEFORE the data provider has actually drained
    # (deferred submit, see below) -- keying off seq_state there raced the
    # mirror update and let END_STREAM land back on the DATA frame.
    my $trailers_declared = 0;

    # Trailers-vs-data-provider handshake (design §8.3). Confirmed
    # empirically: calling submit_trailer() BEFORE the data provider has
    # actually handed nghttp2 its terminal (eof=1) chunk silently abandons
    # any DATA nghttp2 has not yet pulled through $data_callback (observed
    # under real per-stream flow control -- the still-queued tail of a
    # file/fh body never reached the wire, yet the stream closed "cleanly"
    # with the trailer -- a DEFERRED data-provider item is detached from
    # nghttp2's own outbound queue, so an early trailing HEADERS orphans
    # it). The Net::HTTP2::nghttp2 binding's own POD says submit_trailer()
    # "can be called inside" the data-provider callback OR after it
    # returns -- it does not say "at any later, unrelated time" -- so the
    # invariant this handshake actually enforces is narrower and stricter:
    # never BEFORE the provider has delivered its terminal EOF. The
    # trailers arm below submits directly ONLY once $data_eof_delivered is
    # already true (no further callback invocation will occur for this
    # stream); otherwise it stages the headers here and PARKS the send()
    # until the callback's own terminal invocation submits them.
    #
    # Contract note (flag for Task 6 / Compliance.pod): this means a
    # trailers send() can now block for as long as the peer withholds
    # flow-control window on a still-draining body -- new, unbounded-in-
    # the-app's-view blocking that a pre-Task-4 (stub) reading of the spec
    # would not have anticipated. This is arguably MORE correct, not a
    # regression: trailers now participate in the same backpressure body
    # sends already do, rather than racing ahead of undelivered DATA.
    my $data_eof_delivered = 0;
    my $pending_trailer_headers;

    # Called by $data_callback at the exact point it hands nghttp2 the
    # terminal (eof=1) chunk. If a trailers send() is waiting on this
    # (staged $pending_trailer_headers), submits it HERE -- synchronously,
    # from inside the data-provider callback, per nghttp2's own sanctioned
    # pattern -- then wakes the waiting send() inline: this provider has just
    # delivered its terminal chunk and will not be invoked again, so the
    # resumed send()'s own resume_stream is inert and its flush defers.
    my $deliver_trailer_eof = sub {
        $data_eof_delivered = 1;
        return unless defined $pending_trailer_headers;
        my $headers = $pending_trailer_headers;
        $pending_trailer_headers = undef;
        my $ss2 = $weak_self && $weak_self->{h2_streams}{$stream_id};
        # Dead-stream invariant, kept local here rather than inferred from
        # the doomed-but-still-present carve-out pattern used across this
        # file (h2_closed set, entry not yet reclaimed): don't reach
        # nghttp2 a second time on a stream id it may already be tearing
        # down. Two close paths, two mechanisms, same outcome -- there's
        # nothing left to do here either way: _h2_on_close and the 413
        # early-close branch set h2_closed on the still-present entry
        # (caught by the check below), while a whole-connection _close
        # deletes the h2_streams entry outright (so $ss2 comes back undef
        # here) and releases trailer_wait itself via its own sweep. Either
        # way, whichever close path is running has already released -- or
        # is about to release -- trailer_wait.
        return if $ss2 && $ss2->{h2_closed};
        my $ok = eval {
            $weak_self->{h2_session}->submit_trailer($stream_id, headers => $headers);
            1;
        };
        my $err = $@;
        my $f = $ss2 && delete $ss2->{trailer_wait};
        return if !$f || $f->is_ready;
        if ($ok) { $f->done(1) } else { $f->fail($err) }
    };

    # Data callback for nghttp2's streaming response.
    # Returns ($data, $eof) when data is available, or undef to defer.
    my $data_callback = sub {
        my ($cb_stream_id, $max_len) = @_;

        my $ss = $weak_self && $weak_self->{h2_streams}{$stream_id};
        return undef unless $ss;
        my $q = $ss->{send_queue} ||= [];

        if (@$q) {
            my $chunk = shift @$q;
            # Respect max_len — XS truncates without preserving remainder
            if (length($chunk) > $max_len) {
                unshift @$q, substr($chunk, $max_len);
                $chunk = substr($chunk, 0, $max_len);
            }
            $ss->{send_queue_bytes} -= length($chunk);

            # Per-stream backpressure: once this stream's queue falls below the
            # low watermark, release any producer blocked in
            # _h2_wait_for_stream_drain. This callback runs inside nghttp2's
            # extract(), so resolve on the next loop tick — completing the Future
            # resumes the awaiting producer synchronously, and it must not call
            # resume_stream/_h2_write_pending re-entrantly into nghttp2.
            if (($ss->{send_queue_bytes} // 0) < $weak_self->{write_low_watermark}
                    && $ss->{stream_drain_waiters} && @{$ss->{stream_drain_waiters}}) {
                my @waiters = splice @{$ss->{stream_drain_waiters}};
                $weak_self->{server}->loop->later(sub {
                    $_->done for grep { !$_->is_ready } @waiters;
                });
            }

            # Fire the app's on_drain hysteresis callbacks once this stream's
            # queue falls below the low watermark. Like the waiters above, this
            # runs inside nghttp2's extract(), and an on_drain callback may call
            # $send to resume its source — which would re-enter nghttp2. Splice
            # the fires out first (so they can't double-fire), then invoke them on
            # the next loop tick.
            if (($ss->{send_queue_bytes} // 0) < $weak_self->{write_low_watermark}
                    && $ss->{transport_drain_fires} && @{$ss->{transport_drain_fires}}) {
                my @fires = splice @{$ss->{transport_drain_fires}};
                $weak_self->{server}->loop->later(sub {
                    $_->() for @fires;
                });
            }

            my $eof = (!@$q && $eof_pending) ? 1 : 0;
            # Trailers declared: reserve END_STREAM for the trailing
            # HEADERS block (design §8.3) instead of letting it land on
            # this DATA frame. Has no effect unless $eof is also true
            # (Net::HTTP2::nghttp2's own contract), so it is safe to key
            # off $trailers_declared unconditionally here rather than the
            # $ss->{seq_state} mirror, which the trailers arm may have
            # already advanced past 'awaiting_trailers' by this point.
            my $no_end = $trailers_declared ? 1 : 0;
            $deliver_trailer_eof->() if $eof;
            return ($chunk, $eof, $no_end);
        }

        # Queue empty but EOF pending — signal end of stream
        if ($eof_pending) {
            my $no_end = $trailers_declared ? 1 : 0;
            $deliver_trailer_eof->();
            return ('', 1, $no_end);
        }

        # Queue empty, more data expected — defer (NGHTTP2_ERR_DEFERRED in C layer)
        return undef;
    };

    # Shared file/fh chunk pump: pushes produced chunks into this stream's
    # send queue under the per-stream watermark, then marks EOF. The producer
    # is an async sub that receives an async "emit" callback and must await it
    # per chunk; emit dies with the sentinel below if the stream vanishes
    # (client reset) so the pump stops reading without treating it as an error.
    my $STREAM_GONE = "PAGI::h2 stream gone\n";
    my $emit_chunk = async sub {
        my ($chunk) = @_;
        my $ss = $weak_self ? $weak_self->{h2_streams}{$stream_id} : undef;
        die $STREAM_GONE unless $ss && !$ss->{h2_closed} && !$weak_self->{closed};
        if (($ss->{send_queue_bytes} // 0) >= $weak_self->{write_high_watermark}) {
            await $weak_self->_h2_wait_for_stream_drain($stream_id);
            $ss = $weak_self ? $weak_self->{h2_streams}{$stream_id} : undef;
            die $STREAM_GONE unless $ss && !$ss->{h2_closed} && !$weak_self->{closed};
        }
        if (length $chunk) {
            push @{$ss->{send_queue}}, $chunk;
            $ss->{send_queue_bytes} += length $chunk;
        }
        $ss->{transport_state}->_check_watermarks if $ss->{transport_state};
        $weak_self->{h2_session}->resume_stream($stream_id);
        $weak_self->_h2_write_pending;
        return;
    };

    # Shared tail for the file/fh arms below: mark EOF pending and resume the
    # stream once the read loop finishes without error. A no-op if the stream
    # vanished (client reset) while the last chunk was in flight.
    my $finish_body_stream = sub {
        $eof_pending = 1;
        return unless $weak_self;
        return unless $weak_self->{h2_streams}{$stream_id};
        $weak_self->{h2_session}->resume_stream($stream_id);
        $weak_self->_h2_write_pending;
    };

    # Shared preamble for the file/fh body arms below (extraction, not a
    # redesign -- see the arms themselves for why each piece sits where it
    # does). Split in two because the two pieces cross the arms' own
    # eval boundary: the file arm must report an illegal-sequence error
    # (from advance_http) ahead of a misleading "File not found" from its
    # own -f/-r checks, but must NOT have submitted response headers to the
    # client before those checks pass -- so the sequence advance runs
    # BEFORE the arm's eval (a comment-preserving-only move: advance_http
    # is a pure function, and a caught vs. uncaught throw here already
    # behaved identically before this extraction, since $seq is simply
    # never reassigned on a throw either way), while the streaming-start
    # call must stay INSIDE the eval, at each arm's own correct position
    # relative to its own pre-checks, so a failure there still rolls back
    # via the shared tail below instead of leaking a submitted-but-broken
    # response.

    # Outside-eval half: snapshot+advance+mirror the sequence state ahead
    # of either arm's own validity checks. Returns the pre-advance $seq so
    # the tail helper below can roll back to it on failure.
    my $advance_seq_for_body = sub {
        my ($ss, $event) = @_;
        my $seq_before = $seq;
        $seq = PAGI::Server::EventValidator::advance_http($seq, $event);
        $publish->($seq);
        return $seq_before;
    };

    # Inside-eval half: submit the streaming response exactly once, the
    # first time either arm actually has a chunk ready to send. Each arm
    # calls this at its own correct point (after its own pre-checks, so a
    # failed check never causes headers to reach the client for a response
    # that goes on to fail).
    my $ensure_h2_streaming_started = sub {
        my ($ss) = @_;
        return if $streaming_started;
        $streaming_started = 1;
        $ss->{send_queue} //= []; $ss->{send_queue_bytes} //= 0;
        $weak_self->{h2_session}->submit_response_streaming(
            $stream_id,
            status => $status, headers => \@response_headers,
            data_callback => $data_callback,
        );
        $weak_self->_h2_write_pending;
    };

    # Shared failure-rollback-or-finish tail for the file/fh body arms:
    # given the arm's own eval result, either rolls $seq back to its
    # pre-event value and re-raises (recoverable per contract, unless the
    # stream is simply gone -- a quiet no-op), or marks the body stream's
    # EOF pending on success. Mirrors the h1 send closure's
    # advance-then-rollback pattern.
    my $finish_or_rollback_body_send = sub {
        my ($ok, $err, $ss, $seq_before) = @_;
        if (!$ok) {
            return if $err eq $STREAM_GONE;   # client reset: quiet no-op
            $seq = $seq_before;                # recoverable, per contract
            $publish->($seq);
            die $err;
        }
        $finish_body_stream->();
    };

    my $send_event = async sub {
        my ($event) = @_;
        return unless $weak_self;

        my $type = $event->{type} // '';

        # Once the machine has recorded the response complete, the machine
        # decides what happens next -- 'complete' has no idempotent case, so
        # any further send always raises -- not the stream-gone check below.
        # h2_streams entries for a finished stream are reclaimed
        # asynchronously by _h2_on_close, so by the time a post-complete send
        # arrives $ss may already be gone.
        my $already_closed = ($seq eq 'complete');

        my $ss = $weak_self->{h2_streams}{$stream_id};
        # A doomed-but-still-present entry (h2_closed set but not yet
        # deleted -- see the 413-overrun branch in _h2_on_body) is treated
        # the same as an absent one: both are post-close sends and must
        # silently no-op, not reach nghttp2 a second time on this stream id.
        return if (!$ss || $ss->{h2_closed}) && !$already_closed;

        return if $weak_self->{closed} && !$already_closed;

        # 1. Shape validation (mandatory)
        PAGI::Server::EventValidator::validate_http_send(
            $event, { extensions => $weak_self->{extensions} });

        # 2. HEAD: the server suppresses the body (PAGI Www.pod "HEAD Requests").
        # The app responds exactly as for GET; we discard payloads, never open
        # file/fh, and accept-and-discard trailers. Sequence state still
        # advances so the lifecycle (completion, post-complete raises) matches GET.
        if ($is_head && ($type eq 'http.response.body' || $type eq 'http.response.trailers')) {
            $seq = PAGI::Server::EventValidator::advance_http($seq, $event);
            $publish->($seq);
            if ($seq eq 'complete' && !$ss->{h2_head_finished}) {
                $ss->{h2_head_finished} = 1;
                $weak_self->{h2_session}->submit_response($stream_id,
                    status  => $status,
                    headers => \@response_headers,
                    body    => '',
                );
                $weak_self->_h2_write_pending;
            }
            return;
        }

        # 3. http.response.trailers (design §8.3): submits the trailing
        # HEADERS block with END_STREAM, completing a response that declared
        # trailers at start. advance-then-rollback (mirrored from
        # _create_send's chunked-framing check): the sequence machine
        # and its $ss mirror advance FIRST, then the native submit is
        # attempted (directly here, or via $deliver_trailer_eof above --
        # see the closure-top comment for why). advance_http itself may
        # croak here (trailers undeclared or body not yet complete) -- that
        # propagates unrolled-back, same as validate_http_send's shape
        # check above, since $seq was never reassigned in that case. If
        # submit_trailer throws, both are rolled back to their pre-event
        # value and the error propagates: the app's return then lands in
        # the existing incomplete-response arm (RST NGHTTP2_INTERNAL_ERROR)
        # -- the same machinery a dropped body chunk already relies on, not
        # duplicated here.
        if ($type eq 'http.response.trailers') {
            my $seq_before = $seq;
            $seq = PAGI::Server::EventValidator::advance_http($seq, $event);
            $publish->($seq);

            # Empty/absent headers still calls submit_trailer with [] --
            # trailers were declared and must still terminate the response.
            my $trailer_headers = [
                map { [_validate_header_name($_->[0]), _validate_header_value($_->[1])] }
                    @{ $event->{headers} // [] }
            ];
            # RFC 9110 6.6.2 / design 13.3 -- an app-supplied connection,
            # transfer-encoding, etc. in a trailer block corrupts it at the
            # framing layer exactly like a response header: nghttp2 rejects
            # the whole trailer HEADERS block, so the peer sees zero
            # trailing HEADERS (on_stream_close never fires) even though
            # submit_trailer below reports success. Strip before submission,
            # same as every response-header path; $in_trailers=1 drops the
            # response-only 'te: trailers' carve-out (RFC 9110 6.6.2 bans
            # connection-specific fields from trailers outright).
            $trailer_headers = $weak_self->_h2_strip_connection_headers($trailer_headers, 1);

            my $ok;
            if ($data_eof_delivered) {
                # The data provider already handed nghttp2 its terminal
                # chunk -- no further $data_callback invocation will occur
                # for this stream, so it is safe to submit directly here.
                $ok = eval {
                    $weak_self->{h2_session}->submit_trailer(
                        $stream_id, headers => $trailer_headers);
                    1;
                };
            } else {
                # The data provider has not yet delivered EOF to nghttp2
                # (still mid-transfer, possibly blocked on real per-stream
                # flow control). Stage the headers for $deliver_trailer_eof
                # to submit from inside the callback's own terminal
                # invocation, and await that -- resolving this send() any
                # earlier would let submit_trailer race ahead of not-yet-
                # extracted DATA (see the closure-top comment). This PARKS
                # the send() until the body actually finishes draining --
                # e.g. until the peer grants enough flow-control window --
                # which can be a real, unbounded wait from the app's point
                # of view (closure-top comment has the full contract note).
                $pending_trailer_headers = $trailer_headers;
                my $f = $weak_self->{server}->loop->new_future;
                $ss->{trailer_wait} = $f;
                $weak_self->{h2_session}->resume_stream($stream_id) if $streaming_started;
                $weak_self->_h2_write_pending;
                # eval wraps the await (not just a plain assignment) so a
                # failed $f (native rejection, via $deliver_trailer_eof's
                # own $f->fail) is caught into $@ and folds into the same
                # $ok/rollback handling the direct branch above uses.
                $ok = eval { await $f; 1 };
                # $f resolves DONE (not fail) on a whole-connection teardown
                # too (_close's own sweep releases trailer_wait -- see
                # _h2_resolve_stream_trailer_wait) -- the h2_closed carve-
                # out's "successful no-op" contract, same discipline as the
                # body arms' own post-drain-wait liveness re-check
                # (:1616-1619 / :1635-1638 above). Without this, a
                # connection that died while we awaited would leave
                # $weak_self->{h2_session} undef and the shared tail below
                # would call a method on it.
                return unless $weak_self && !$weak_self->{closed} && $weak_self->{h2_session};
            }
            if (!$ok) {
                my $err = $@;
                $seq = $seq_before;
                $publish->($seq);
                die $err;
            }

            # Stream-existence re-check, same discipline as every sibling
            # await site's own $ss re-fetch (e.g. $emit_chunk above): the
            # connection-liveness check at :1524 only covers the deferred
            # branch and only the connection, not this stream specifically.
            # Placed AFTER the rollback above (not folded into :1524) so a
            # deferred native failure still reaches its rollback+die even if
            # the stream looks gone by the time we get here -- only the
            # success-path tail below, which is about to call resume_stream/
            # _h2_write_pending on this stream id again, needs the guard.
            # A gone stream here is the h2_closed carve-out's ordinary
            # "successful no-op" case: the trailers already landed (ok=1)
            # before the stream went away, so returning quietly is correct,
            # not a swallowed error.
            $ss = $weak_self ? $weak_self->{h2_streams}{$stream_id} : undef;
            return if !$ss || $ss->{h2_closed};

            # Flush any still-queued DATA before the trailing HEADERS --
            # nghttp2 orders trailers after queued data on its own, but the
            # stream must be resumed if the data callback had deferred.
            $weak_self->{h2_session}->resume_stream($stream_id) if $streaming_started;
            $weak_self->_h2_write_pending;
            return;
        }

        # 4. file body: streamed through the send queue via $emit_chunk, one
        # chunk at a time, under this stream's own backpressure. The type
        # guard (not just "defined $event->{file}") keeps a nonconforming
        # http.response.start carrying a stray 'file' key from being
        # misrouted into this arm.
        if ($type eq 'http.response.body' && defined $event->{file}) {
            my $file   = $event->{file};
            my $offset = $event->{offset} // 0;
            my $length = $event->{length};

            # Snapshot+advance BEFORE the -f/-r checks (and the checks live
            # inside the eval below) so a file event arriving in an illegal
            # sequence state reports the SEQUENCE error, not a misleading
            # "File not found" -- h1 parity. A failed file send must NOT
            # mark the response complete, so on failure $seq rolls back to
            # $seq_before exactly as the h1 twin in _create_send does.
            my $seq_before = $advance_seq_for_body->($ss, $event);

            my $ok = eval {
                die "File not found: $file\n"  unless -f $file;
                die "Cannot read file: $file\n" unless -r $file;

                $ensure_h2_streaming_started->($ss);

                # Mirror h1's effective-length computation (_send_file_response)
                # so the sync-vs-async choice below sees the same number h1
                # would, including the Www.pod rule that an offset past EOF
                # clamps to zero bytes rather than failing.
                my $file_size = -s $file;
                die "Cannot stat file $file: $!\n" unless defined $file_size;
                my $effective_length = $length // ($file_size - $offset);
                $effective_length = 0 if $effective_length < 0;

                if ($weak_self->{sync_file_threshold} > 0
                    && $effective_length <= $weak_self->{sync_file_threshold}) {
                    # Small-file fast path (parity with h1's sync_file_threshold):
                    # a file at or under the threshold is read synchronously,
                    # in-process, as ONE queue chunk -- avoiding a worker-pool
                    # round trip for a body small enough that it costs more
                    # than it saves. The no-slurp rule targets large files.
                    open my $fh, '<:raw', $file or die "Cannot open file $file: $!\n";
                    seek($fh, $offset, 0) if $offset;
                    my $bytes_read = read($fh, my $data, $effective_length);
                    die "Failed to read file $file: $!\n" unless defined $bytes_read;
                    close $fh;
                    await $emit_chunk->($data);
                }
                else {
                    my $loop = $weak_self->{server}->loop;
                    await PAGI::Server::AsyncFile->read_file_chunked(
                        $loop, $file, $emit_chunk,
                        offset => $offset,
                        (defined $length ? (length => $length) : ()),
                        chunk_size => FILE_CHUNK_SIZE,
                    );
                }
                1;
            };
            $finish_or_rollback_body_send->($ok, $@, $ss, $seq_before);
            return;
        }

        # 5. fh body: streamed through the send queue via $emit_chunk, one
        # chunk at a time, under this stream's own backpressure. The
        # application owns $fh (opened before the send, closed or not by the
        # app afterward) -- the server never closes it. Mirrors the file arm
        # above; the read loop is adapted from h1's _send_fh_response. The
        # type guard mirrors the file arm's M1 fix above.
        if ($type eq 'http.response.body' && defined $event->{fh}) {
            my $fh = $event->{fh};

            my $seq_before = $advance_seq_for_body->($ss, $event);

            my $ok = eval {
                $ensure_h2_streaming_started->($ss);
                if (my $off = $event->{offset}) {
                    seek($fh, $off, 0) or die "Cannot seek: $!\n";
                }
                my $remaining = $event->{length};
                while (1) {
                    my $to_read = FILE_CHUNK_SIZE;
                    if (defined $remaining) {
                        $to_read = $remaining if $remaining < $to_read;
                        last if $to_read <= 0;
                    }
                    # The bare block scopes only the 'closed' warning
                    # suppression around read() -- a bare block is itself a
                    # one-iteration loop, so die/last/await must live outside
                    # it or 'last' would only exit the block, not this while.
                    my ($bytes_read, $chunk);
                    { no warnings 'closed';
                      $bytes_read = read($fh, $chunk, $to_read);
                    }
                    die "Failed to read filehandle: $!\n" unless defined $bytes_read;
                    last if $bytes_read == 0;
                    await $emit_chunk->($chunk);
                    $remaining -= $bytes_read if defined $remaining;
                }
                1;
            };
            $finish_or_rollback_body_send->($ok, $@, $ss, $seq_before);
            return;
        }

        # 6. Sequence enforcement (start / plain body / fullflush). advance_http
        # is deliberately called from four places in this sub — the HEAD
        # block above, the file arm, the fh arm, and here — each has
        # different pre/post-state needs; do not consolidate them.
        $seq = PAGI::Server::EventValidator::advance_http($seq, $event);
        $publish->($seq);

        if ($type eq 'http.response.start') {
            $ss->{response_started} = 1;
            $ss->{connection_state}->_mark_response_started if $ss->{connection_state};
            $trailers_declared = 1 if $event->{trailers};

            $status = $event->{status} // 200;
            @response_headers = map {
                [_validate_header_name($_->[0]), _validate_header_value($_->[1])]
            } @{$event->{headers} // []};
            # RFC 9113 8.2.2 / design 13.3 — strips app-supplied connection,
            # transfer-encoding, etc. before this list reaches nghttp2 (also
            # covers the HEAD path below, which submits this same array).
            @response_headers = @{ $weak_self->_h2_strip_connection_headers(\@response_headers) };
            # Server-supplied Date header (HTTP/1.1 parity) — add if the app didn't.
            unless (grep { lc($_->[0]) eq 'date' } @response_headers) {
                push @response_headers, ['date', $weak_self->{protocol}->format_date];
            }
        }
        elsif ($type eq 'http.response.body') {
            my $body = $event->{body} // '';
            my $more = $event->{more} // 0;

            if ($more) {
                if (!$streaming_started) {
                    # First streaming chunk — submit with data callback
                    $streaming_started = 1;
                    $ss->{send_queue}       //= [];
                    $ss->{send_queue_bytes} //= 0;
                    if (length $body) {
                        push @{$ss->{send_queue}}, $body;
                        $ss->{send_queue_bytes} += length $body;
                    }
                    # Synchronous: we're in the app's send path (not nghttp2's
                    # extract), so on_high_water can fire here to tell the app to
                    # pause its source.
                    $ss->{transport_state}->_check_watermarks if $ss->{transport_state};
                    $weak_self->{h2_session}->submit_response_streaming(
                        $stream_id,
                        status        => $status,
                        headers       => \@response_headers,
                        data_callback => $data_callback,
                    );
                    $weak_self->_h2_write_pending;
                } else {
                    # Subsequent chunk — backpressure check then push and resume.
                    # Bound on THIS stream's send queue (per-stream), not the
                    # shared TCP buffer which is meaningless across multiplexed
                    # streams.
                    if (($ss->{send_queue_bytes} // 0) >= $weak_self->{write_high_watermark}) {
                        await $weak_self->_h2_wait_for_stream_drain($stream_id);
                        return unless $weak_self;
                        return if $weak_self->{closed};
                        return unless $weak_self->{h2_streams}{$stream_id};
                    }
                    if (length $body) {
                        push @{$ss->{send_queue}}, $body;
                        $ss->{send_queue_bytes} += length $body;
                    }
                    # Synchronous — app send path, not nghttp2 extract.
                    $ss->{transport_state}->_check_watermarks if $ss->{transport_state};
                    $weak_self->{h2_session}->resume_stream($stream_id);
                    $weak_self->_h2_write_pending;
                }
            } else {
                if ($streaming_started) {
                    # Final chunk on an already-streaming response. Bound on THIS
                    # stream's send queue (per-stream), not the shared TCP buffer.
                    if (($ss->{send_queue_bytes} // 0) >= $weak_self->{write_high_watermark}) {
                        await $weak_self->_h2_wait_for_stream_drain($stream_id);
                        return unless $weak_self;
                        return if $weak_self->{closed};
                        return unless $weak_self->{h2_streams}{$stream_id};
                    }
                    $eof_pending = 1;
                    if (length $body) {
                        push @{$ss->{send_queue}}, $body;
                        $ss->{send_queue_bytes} += length $body;
                    }
                    # Synchronous — app send path, not nghttp2 extract.
                    $ss->{transport_state}->_check_watermarks if $ss->{transport_state};
                    $weak_self->{h2_session}->resume_stream($stream_id);
                    $weak_self->_h2_write_pending;
                } elsif ($seq eq 'awaiting_trailers') {
                    # Trailers were declared and this is the (single-shot)
                    # terminal body event: a plain submit_response would set
                    # END_STREAM immediately, ending the stream before the
                    # trailers arrive -- design §8.3 forbids that. Route
                    # through the streaming path instead, mirroring the
                    # file/fh arms' $streaming_started idiom; the data
                    # callback above already reserves END_STREAM for the
                    # trailing HEADERS block once it observes
                    # 'awaiting_trailers'.
                    $streaming_started = 1;
                    $ss->{send_queue}       //= [];
                    $ss->{send_queue_bytes} //= 0;
                    $eof_pending = 1;
                    if (length $body) {
                        push @{$ss->{send_queue}}, $body;
                        $ss->{send_queue_bytes} += length $body;
                    }
                    $ss->{transport_state}->_check_watermarks if $ss->{transport_state};
                    $weak_self->{h2_session}->submit_response_streaming(
                        $stream_id,
                        status        => $status,
                        headers       => \@response_headers,
                        data_callback => $data_callback,
                    );
                    $weak_self->_h2_write_pending;
                } else {
                    # Non-streaming: single response (unchanged one-shot path)
                    $weak_self->{h2_session}->submit_response($stream_id,
                        status  => $status,
                        headers => \@response_headers,
                        body    => $body,
                    );
                    $weak_self->_h2_write_pending;
                }
            }
        }
        elsif ($type eq 'http.fullflush') {
            # Hand any pending frames to the session's write path (design §8.4).
            $weak_self->{h2_session}->resume_stream($stream_id) if $streaming_started;
            $weak_self->_h2_write_pending;
        }
    };

    # One site for every terminal event this machine produces -- body,
    # trailers, a HEAD response's suppressed body, and the refusal the
    # WebSocket and SSE scopes delegate here. Its own state is the whole fact,
    # so a chunk with more => 1 never reaches it and a rolled-back send fails
    # the Future first. The wire rule for the same moment is _h2_on_frame_sent.
    return sub {
        my ($event) = @_;
        return $send_event->($event)->on_done(sub {
            return unless $weak_self
                && PAGI::Server::EventValidator::scope_send_clean('http', $seq);
            my $ss = $weak_self->{h2_streams}{$stream_id} or return;
            $weak_self->_h2_end_scope_output($ss);
        });
    };
}

# =============================================================================
# HTTP/2 WebSocket over HTTP/2 (RFC 8441)
# =============================================================================

sub _h2_create_websocket_scope {
    my ($self, $stream_id, $stream_state) = @_;

    my $pseudo  = $stream_state->{pseudo};
    my $headers = $stream_state->{headers};

    my $full_path = $pseudo->{':path'} // '/';
    my ($path, $query_string) = split(/\?/, $full_path, 2);
    $query_string //= '';

    # Match HTTP/1.1 pipeline: URI::Escape + UTF-8 decode with fallback
    my $unescaped = uri_unescape($path);
    my $decoded_path = eval { decode('UTF-8', $unescaped, Encode::FB_CROAK) }
                       // $unescaped;

    # Extract subprotocols from headers
    my @subprotocols;
    for my $header (@$headers) {
        my ($name, $value) = @$header;
        if ($name eq 'sec-websocket-protocol') {
            push @subprotocols, map { s/^\s+|\s+$//gr } split /,/, $value;
        }
    }

    my $connection_state = PAGI::Server::ConnectionState->new(
        connection => $self,
        server     => $self->{server},
        on_abort   => $self->_h2_abort_hook($stream_id),
    );
    $stream_state->{connection_state} = $connection_state;

    return {
        type         => 'websocket',
        pagi         => {
            version      => '0.5',
            spec_version => '0.6',
        },
        http_version => '2',
        scheme       => $self->_get_ws_scheme,
        path         => $decoded_path,
        raw_path     => $path,
        query_string => $query_string,
        root_path    => '',
        headers      => $headers,
        (defined $self->{client_host}
            ? (client => [$self->{client_host}, $self->{client_port}])
            : ()
        ),
        server       => [$self->{server_host}, $self->{server_port}],
        subprotocols => \@subprotocols,
        state        => keys %{$self->{state}} ? { %{$self->{state}} } : {},
        extensions   => do {
            my %ext = %{$self->_get_extensions_for_scope};
            # fullflush has no validate_websocket_send arm; advertising it here
            # would lie to the app (design 13.2).
            delete $ext{fullflush};
            \%ext;
        },
        # max_frame_size: omitted when unenforced (max_ws_frame_size 0/undef
        # means unlimited, per Protocol::WebSocket::Frame's max_payload_size
        # semantics -- a server that does not enforce a cap must not
        # advertise one). max_receive_queue has no unlimited mode (a hard,
        # always-enforced cap), so it is always present.
        ($self->{max_ws_frame_size}
            ? (max_frame_size => $self->{max_ws_frame_size})
            : ()
        ),
        max_receive_queue => $self->{max_receive_queue},
        'pagi.connection' => $connection_state,
        # Per-stream outbound flow-control handle. Like the h2 sse/streaming
        # scopes, it measures THIS stream's send queue (h2 multiplexes many
        # streams over one connection, so the shared TCP buffer is
        # meaningless per stream). Gives WebSocket-over-h2 the same
        # pagi.transport surface HTTP/1.1 WebSocket already provides.
        'pagi.transport'  => ($stream_state->{transport_state} = $self->_h2_transport_state($stream_state)),
    };
}

sub _h2_create_websocket_receive {
    my ($self, $stream_id, $stream_state) = @_;

    weaken(my $weak_self = $self);

    # Fallback disconnect for a receive() that resolves after this stream is
    # gone or the connection is closed: 1006/'client_closed' by default (RFC
    # 6455 abnormal closure with the token for a bare transport drop, per
    # Www.pod "Disconnect - receive event"), but prefers the stream's own
    # ending record when its state is still reachable in h2_streams -- a
    # server-initiated teardown (idle timeout, keepalive timeout, protocol
    # close, ...) records its reason there before tearing the stream down, so
    # a receive() racing that teardown still reports why.
    my $fallback_disconnect = sub {
        # This scope already delivered its disconnect: a further receive()
        # resolves with that same event, not with a fresh reading of the
        # ending record, which cannot spell a peer's close code and reason
        # text (see _h2_ws_enqueue_disconnect). Read from the stream state
        # this closure captured -- the same hash _h2_dispatch_stream took out
        # of h2_streams -- so the event is still reachable after the deferred
        # delete drops the h2_streams entry a turn past the close.
        return { %{ $stream_state->{ws_disconnect_event} } }
            if $stream_state && $stream_state->{ws_disconnect_event};

        return { type => 'websocket.disconnect', code => 1006, reason => 'client_closed' }
            unless $weak_self;
        my $scope = $weak_self->{h2_streams}{$stream_id} // $weak_self;
        return {
            type   => 'websocket.disconnect',
            code   => $weak_self->_end_code($scope),
            reason => $weak_self->_end_reason($scope),
        };
    };

    # This scope's cap record and its gate (see _disconnect_receive_future),
    # closure-local so it outlives the h2_streams entry.
    my %cap = (scope => 'websocket', transport => "HTTP/2 stream $stream_id", count => 0);
    my $disconnect = sub {
        my ($parked) = @_;
        return Future->done($fallback_disconnect->()) unless $weak_self;
        return $weak_self->_disconnect_receive_future(\%cap, $fallback_disconnect, $parked);
    };

    # The answer for a receive made after this scope's clean end, which on a
    # websocket scope is a completed refusal. It goes through the same gate,
    # so the cap counts it exactly like every other synthesized answer.
    my $scope_end = sub {
        my ($parked) = @_;
        my $end = _h2_scope_end_event($stream_state) or return undef;
        return Future->done($end) unless $weak_self;
        return $weak_self->_disconnect_receive_future(\%cap, $end, $parked);
    };

    return sub {
        return Future->done($fallback_disconnect->())
            unless $weak_self;

        # A completed refusal is this scope's clean end: it delivers no
        # websocket.disconnect of its own (Www.pod "Disconnect - receive
        # event"), and a receive made after it reports the end with the
        # http.disconnect of the HTTP exchange that refusal was (Www.pod
        # "Receiving after the scope's end"). Read from $stream_state rather
        # than h2_streams: the entry is reclaimed a tick after the stream
        # closes, and the scope-level fact outlives it. The h1 twin answers
        # the same way, and so does the sse side of both.
        if (my $f = $scope_end->()) { return $f }

        return $disconnect->()
            if $weak_self->{closed};

        my $ss = $weak_self->{h2_streams}{$stream_id};
        return $disconnect->()
            unless $ss;

        my $future = (async sub {
            return $fallback_disconnect->()
                unless $weak_self;

            # True once this call has waited: the answer it then gets is a
            # delivery of the scope's terminal state, not a repeat request
            # for it, so the cap does not count it.
            my $parked = 0;

            my $ss = $weak_self->{h2_streams}{$stream_id};
            return await $disconnect->($parked)
                unless $ss;

            # Check queue first
            if (@{$ss->{receive_queue}}) {
                return shift @{$ss->{receive_queue}};
            }

            # First call returns websocket.connect
            if (!$ss->{ws_connect_sent}) {
                $ss->{ws_connect_sent} = 1;
                return { type => 'websocket.connect' };
            }

            # Wait for events
            while (1) {
                # The scope ended cleanly under this call: the refusal was
                # sent from another Future while this one waited. Asked ahead
                # of the queue, because it is the scope's ending, not the
                # transport's, that decides what this call reports.
                if (my $f = $scope_end->($parked)) { return await $f }

                if (@{$ss->{receive_queue}}) {
                    return shift @{$ss->{receive_queue}};
                }

                if ($weak_self->{closed}) {
                    return await $disconnect->($parked);
                }

                # Nothing left for this call to wait for, on either count:
                # this stream's close handler has already run (see the twin in
                # _h2_create_receive for why parking here would strand the
                # call), or the scope has already delivered its disconnect --
                # "once this event has been delivered the scope is over, and a
                # further receive() resolves with the same websocket.disconnect
                # again" (Www.pod "Disconnect - receive event"), and a peer that
                # half-closes with END_STREAM and never resets holds the stream
                # itself open for as long as it likes. A completed refusal never
                # reaches here: the test at the top of this loop answers it
                # first, with the scope's end.
                return await $disconnect->($parked)
                    if !$weak_self->_h2_stream_alive($stream_id)
                    || $ss->{ws_disconnect_delivered};

                if (!$ss->{body_pending}) {
                    $ss->{body_pending} = Future->new;
                }
                $parked = 1;
                await $ss->{body_pending};

                $ss = $weak_self->{h2_streams}{$stream_id};
                # A clean end is answered at the top of this loop, which never
                # touches $ss, so the entry going away does not take the
                # scope's own ending with it.
                return await $disconnect->($parked)
                    unless $ss || _h2_scope_end_event($stream_state);
            }
        })->();

        return $future;
    };
}

sub _h2_create_websocket_send {
    my ($self, $stream_id, $stream_state) = @_;

    weaken(my $weak_self = $self);
    my $seq = 'connecting';

    # Refusing the handshake goes out through the ordinary HTTP send path;
    # that closure publishes its http state straight into this scope's.
    my $refusal_send;

    # Data callback for nghttp2's streaming response (the same pull-based
    # data-provider model _h2_create_send/_h2_create_sse_send already use).
    # Pulls raw WS frame bytes from the per-stream queue -- app messages,
    # protocol replies (pong, close-echo), and this stream's own keepalive
    # ping are all pushed there in call order, so FIFO ordering on the wire
    # is preserved exactly as it was under direct submit_data calls.
    #
    # $ss->{ws_eof_pending} lives on the stream state (not a closure-local,
    # unlike the http streaming callback's $eof_pending) because it is set
    # from other subs entirely -- _h2_ws_close and the close-frame arm of
    # _h2_process_ws_frames -- not just from this closure. When set, it
    # merges END_STREAM onto the LAST queued chunk (the close frame itself)
    # rather than emitting a separate empty terminal frame, matching the
    # previous submit_data($id, $close_frame_bytes, 1) behavior exactly.
    my $data_callback = sub {
        my ($cb_stream_id, $max_len) = @_;

        my $ss = $weak_self && $weak_self->{h2_streams}{$stream_id};
        return undef unless $ss;
        my $q = $ss->{send_queue} ||= [];

        if (@$q) {
            my $chunk = shift @$q;
            # Respect max_len — XS truncates without preserving remainder
            if (length($chunk) > $max_len) {
                unshift @$q, substr($chunk, $max_len);
                $chunk = substr($chunk, 0, $max_len);
            }
            $ss->{send_queue_bytes} -= length($chunk);

            # Per-stream backpressure: once this stream's queue falls below the
            # low watermark, release any producer blocked in
            # _h2_wait_for_stream_drain. This runs inside nghttp2's extract(), so
            # resolve on the next loop tick — completing the Future resumes the
            # awaiting producer synchronously, and it must not re-enter nghttp2.
            if (($ss->{send_queue_bytes} // 0) < $weak_self->{write_low_watermark}
                    && $ss->{stream_drain_waiters} && @{$ss->{stream_drain_waiters}}) {
                my @waiters = splice @{$ss->{stream_drain_waiters}};
                $weak_self->{server}->loop->later(sub {
                    $_->done for grep { !$_->is_ready } @waiters;
                });
            }

            # Fire the app's on_drain hysteresis callbacks once this stream's
            # queue falls below the low watermark. Deferred for the same reason:
            # an on_drain callback may call $send, which would re-enter nghttp2.
            if (($ss->{send_queue_bytes} // 0) < $weak_self->{write_low_watermark}
                    && $ss->{transport_drain_fires} && @{$ss->{transport_drain_fires}}) {
                my @fires = splice @{$ss->{transport_drain_fires}};
                $weak_self->{server}->loop->later(sub {
                    $_->() for @fires;
                });
            }

            my $eof = (!@$q && $ss->{ws_eof_pending}) ? 1 : 0;
            return ($chunk, $eof);
        }

        # Queue empty but EOF pending (a close frame already delivered as
        # the terminal chunk above) — signal end of stream.
        return ('', 1) if $ss->{ws_eof_pending};

        # Queue empty, more data expected — defer (NGHTTP2_ERR_DEFERRED in C layer)
        return undef;
    };

    return async sub {
        my ($event) = @_;
        return unless $weak_self;

        my $type = $event->{type} // '';

        # Once the machine has recorded the connection closed or the refusal
        # response complete, the machine decides what happens next -- both
        # 'closed' and 'refusal_complete' have no idempotent case, so any
        # further send always raises -- not the stream-gone check below.
        # h2_streams entries for a finished stream are reclaimed
        # asynchronously by _h2_on_close (websocket.close itself triggers
        # this via END_STREAM, same mechanism as the h2 HTTP/SSE closures),
        # so by the time a post-close send arrives $ss may already be gone.
        my $already_closed = ($seq eq 'closed' || $seq eq 'refusal_complete');

        my $ss = $weak_self->{h2_streams}{$stream_id};
        # A doomed-but-still-present entry (h2_closed set but not yet
        # deleted -- see the 413-overrun branch in _h2_on_body) is treated
        # the same as an absent one: both are post-close sends and must
        # silently no-op, not reach nghttp2 a second time on this stream id.
        return if (!$ss || $ss->{h2_closed}) && !$already_closed;

        return if $weak_self->{closed} && !$already_closed;

        PAGI::Server::EventValidator::validate_websocket_send(
            $event, { extensions => $weak_self->{extensions} });
        my $seq_before = $seq;
        $seq = PAGI::Server::EventValidator::advance_websocket($seq, $event);
        $ss->{seq_state} = $seq if $ss;

        if ($type =~ /^http\.response\./) {
            # Refusing the handshake (Www.pod): the wire response is identical
            # to the same response on an http scope, so that path writes it.
            # advance_websocket has already rejected an HTTP event after
            # websocket.accept. See the h1 twin in _create_websocket_send for
            # why the delegate's own http state is the fact about completion.
            $refusal_send //= $weak_self->_h2_create_send($stream_id, $ss,
                refusal  => 1,
                on_state => sub {
                    return unless $weak_self;
                    $seq = $_[0] eq 'complete' ? 'refusal_complete' : 'refusing';
                    my $s = $weak_self->{h2_streams}{$stream_id};
                    $s->{seq_state} = $seq if $s;
                },
            );
            my $ok = eval { await $refusal_send->($event); 1 };
            my $error = $@;
            die $error unless $ok;
            return;
        }

        if ($type eq 'websocket.accept') {
            # A duplicate accept is already rejected by advance_websocket.

            # HTTP/2 WebSocket: respond with 200 (not 101). Header building is
            # advance-then-rollback, like the h1 twin:
            # a subprotocol or header that fails byte validation must leave no
            # state behind, and the send state is now the only record of
            # whether the handshake completed.
            my @headers;
            my $built = eval {
                if (my $subprotocol = $event->{subprotocol}) {
                    $subprotocol = _validate_subprotocol($subprotocol);
                    push @headers, ['sec-websocket-protocol', $subprotocol];
                }
                if (my $extra = $event->{headers}) {
                    push @headers, map {
                        [_validate_header_name($_->[0]), _validate_header_value($_->[1])]
                    } @$extra;
                }
                # RFC 9113 8.2.2 / design 13.3 — strip app-supplied connection,
                # transfer-encoding, etc. before submission.
                @headers = @{ $weak_self->_h2_strip_connection_headers(\@headers) };
                1;
            };
            unless ($built) {
                my $error = $@;
                $seq = $seq_before;
                $ss->{seq_state} = $seq if $ss;
                die $error;
            }

            $ss->{response_started} = 1;
            $ss->{ws_frame} = Protocol::WebSocket::Frame->new(
                max_payload_size => $weak_self->{max_ws_frame_size},
            );

            # Submit 200 response with a pull-based data provider (same
            # model as h2 streaming/SSE): frames are pushed onto
            # $ss->{send_queue} and pulled by $data_callback as nghttp2's
            # per-stream flow-control window allows.
            $ss->{send_queue}       //= [];
            $ss->{send_queue_bytes} //= 0;
            $weak_self->{h2_session}->submit_response_streaming($stream_id,
                status        => 200,
                headers       => \@headers,
                data_callback => $data_callback,
            );
            $weak_self->_h2_write_pending;

            # Process any data that arrived before accept. This arm runs
            # outside a feed, so the drain's own output (a pong, a close echo)
            # is flushed here -- nothing else will write it.
            if (length($ss->{body}) > 0) {
                my $buffered = $ss->{body};
                $ss->{body} = '';
                $weak_self->_h2_process_ws_frames($stream_id, $ss, $buffered);
                $weak_self->_h2_write_pending;
            }
        }
        elsif ($type eq 'websocket.send') {
            return unless _ws_handshake_accepted($ss->{seq_state});

            my $frame;
            if (defined $event->{text}) {
                $frame = Protocol::WebSocket::Frame->new(
                    buffer => $event->{text},
                    type   => 'text',
                );
            }
            elsif (defined $event->{bytes}) {
                $frame = Protocol::WebSocket::Frame->new(
                    buffer => $event->{bytes},
                    type   => 'binary',
                );
            }
            else {
                return;
            }

            my $bytes = $frame->to_bytes;

            # Per-stream backpressure: bound on THIS stream's queue, not the
            # shared TCP buffer (meaningless across multiplexed h2 streams).
            if (($ss->{send_queue_bytes} // 0) >= $weak_self->{write_high_watermark}) {
                await $weak_self->_h2_wait_for_stream_drain($stream_id);
                return unless $weak_self;
                return if $weak_self->{closed};
                # Refetch (same idiom as emit_chunk, :1374-1375): a stream
                # can close while this send was parked.
                $ss = $weak_self->{h2_streams}{$stream_id};
                return unless $ss;
                return if $ss->{h2_closed};
                # This send can wake AFTER this stream's close frame was
                # already queued (window opened below the low watermark
                # while ws_eof_pending was set) -- pushing app data now would
                # land it BEHIND the close frame: Close would ship without
                # END_STREAM and a Text/Binary frame would follow it,
                # violating RFC 6455 5.5.1. Same post-close no-op contract
                # as the top-of-closure $already_closed check.
                return if $ss->{ws_eof_pending};
            }

            push @{$ss->{send_queue}}, $bytes;
            $ss->{send_queue_bytes} = ($ss->{send_queue_bytes} // 0) + length $bytes;
            # Synchronous — app send path, not nghttp2 extract — so on_high_water
            # may fire here to tell the app to pause its source.
            $ss->{transport_state}->_check_watermarks if $ss->{transport_state};
            $weak_self->{h2_session}->resume_stream($stream_id);
            $weak_self->_h2_write_pending;
        }
        elsif ($type eq 'websocket.close') {
            # A close before accept is already rejected by advance_websocket,
            # so the handshake is known complete here.

            # Close frame + END_STREAM, and release this stream's keepalive
            # (_h2_ws_close does both). Runs outside feed(), so flush here.
            $weak_self->_h2_ws_close($stream_id,
                code => $event->{code} // 1000, text => $event->{reason} // '');
            $weak_self->_h2_write_pending;
        }
        elsif ($type eq 'websocket.keepalive') {
            return unless _ws_handshake_accepted($ss->{seq_state});

            my $interval = $event->{interval} // 0;
            my $timeout  = $event->{timeout};

            if ($interval > 0) {
                $weak_self->_h2_start_ws_keepalive($stream_id, $ss, $interval, $timeout);
            }
            else {
                $weak_self->_h2_stop_ws_keepalive($ss);
            }
        }

        return;
    };
}

# =============================================================================
# HTTP/2 SSE (Server-Sent Events over HTTP/2)
# =============================================================================

sub _h2_create_sse_scope {
    my ($self, $stream_id, $stream_state) = @_;

    my $pseudo  = $stream_state->{pseudo};
    my $headers = $stream_state->{headers};

    my $full_path = $pseudo->{':path'} // '/';
    my ($path, $query_string) = split(/\?/, $full_path, 2);
    $query_string //= '';

    # Match HTTP/1.1 pipeline: URI::Escape + UTF-8 decode with fallback
    my $unescaped = uri_unescape($path);
    my $decoded_path = eval { decode('UTF-8', $unescaped, Encode::FB_CROAK) }
                       // $unescaped;

    my $connection_state = PAGI::Server::ConnectionState->new(
        connection => $self,
        server     => $self->{server},
        on_abort   => $self->_h2_abort_hook($stream_id),
    );
    $stream_state->{connection_state} = $connection_state;

    return {
        type         => 'sse',
        pagi         => {
            version      => '0.5',
            spec_version => '0.6',
        },
        http_version => '2',
        method       => $pseudo->{':method'} // 'GET',
        scheme       => $pseudo->{':scheme'} // $self->_get_scheme,
        path         => $decoded_path,
        raw_path     => $path,
        query_string => $query_string,
        root_path    => '',
        headers      => $headers,
        (defined $self->{client_host}
            ? (client => [$self->{client_host}, $self->{client_port}])
            : ()
        ),
        server       => [$self->{server_host}, $self->{server_port}],
        state        => keys %{$self->{state}} ? { %{$self->{state}} } : {},
        extensions   => $self->_get_extensions_for_scope,
        'pagi.connection' => $connection_state,
        # Per-stream outbound flow-control handle. Like the h2 streaming scope,
        # it measures THIS stream's send queue (h2 multiplexes many streams over
        # one connection, so the shared TCP buffer is meaningless per stream).
        'pagi.transport'  => ($stream_state->{transport_state} = $self->_h2_transport_state($stream_state)),
    };
}

sub _h2_create_sse_receive {
    my ($self, $stream_id, $stream_state) = @_;

    weaken(my $weak_self = $self);

    # This fallback must name whatever the stream's connection_state was
    # marked with, not a hardcoded 'client_closed'. The stream may already be
    # gone by the time a parked receive resumes; the connection's own ending
    # record answers then, because a connection-level end records its reason
    # onto every stream still open before any of this is asked.
    my $sse_disconnect = sub {
        return { type => 'sse.disconnect', reason => 'client_closed' } unless $weak_self;
        return {
            type   => 'sse.disconnect',
            reason => $weak_self->_end_reason($weak_self->{h2_streams}{$stream_id} // $weak_self),
        };
    };

    # This scope's cap record and its gate (see _disconnect_receive_future),
    # closure-local so it outlives the h2_streams entry.
    my %cap = (scope => 'sse', transport => "HTTP/2 stream $stream_id", count => 0);
    my $disconnect = sub {
        my ($parked) = @_;
        return Future->done($sse_disconnect->()) unless $weak_self;
        return $weak_self->_disconnect_receive_future(\%cap, $sse_disconnect, $parked);
    };

    # The answer for a receive made after this scope's clean end -- the
    # application's own sse.close, or a completed refusal. It goes through the
    # same gate, so the cap counts it exactly like every other synthesized
    # answer.
    my $scope_end = sub {
        my ($parked) = @_;
        my $end = _h2_scope_end_event($stream_state) or return undef;
        return Future->done($end) unless $weak_self;
        return $weak_self->_disconnect_receive_future(\%cap, $end, $parked);
    };

    return sub {
        return Future->done($sse_disconnect->()) unless $weak_self;

        # A clean end the application itself produced -- its own sse.close, or
        # a completed refusal -- delivers no sse.disconnect of its own, and a
        # receive made after it reports the end with a reasonless
        # sse.disconnect (Www.pod "Receiving after the scope's end"). See the
        # twin in _h2_create_websocket_receive for why this is read from
        # $stream_state, which outlives the h2_streams entry.
        if (my $f = $scope_end->()) { return $f }

        return $disconnect->() if $weak_self->{closed};

        my $ss = $weak_self->{h2_streams}{$stream_id};
        return $disconnect->() unless $ss;

        my $future = (async sub {
            return $sse_disconnect->() unless $weak_self;

            # True once this call has waited: the answer it then gets is a
            # delivery of the scope's terminal state, not a repeat request
            # for it, so the cap does not count it.
            my $parked = 0;

            my $ss = $weak_self->{h2_streams}{$stream_id};
            return await $disconnect->($parked) unless $ss;

            # Check queue first
            if (@{$ss->{receive_queue}}) {
                return shift @{$ss->{receive_queue}};
            }

            # First call returns sse.request once the full body has arrived.
            # A POST body still streaming across DATA frames must not be
            # truncated behind a truthful more=>0 -- wait for body_complete
            # (set on END_STREAM or stream close, see _h2_on_body/_h2_on_close)
            # before delivering it as a single terminal event. This is the
            # smaller change relative to reworking sse.request into a
            # truthful multi-chunk stream: the wire shape here was already
            # one-shot, so completing that contract fixes the dispatch-timing
            # bug (design section 11.1) without touching the event shape.
            if (!$ss->{sse_request_sent}) {
                while (!$ss->{body_complete}) {
                    # The scope ended cleanly under this call -- the
                    # application's own sse.close, sent from another Future
                    # while this one waits for the request body. The end is
                    # what this call reports, rather than a truncated body or
                    # a reason this scope never had. Same rule, same order, as
                    # the wait loop below and as the head of this closure.
                    if (my $f = $scope_end->($parked)) { return await $f }

                    if (@{$ss->{receive_queue}}) {
                        return shift @{$ss->{receive_queue}};
                    }

                    if ($weak_self->{closed}) {
                        return await $disconnect->($parked);
                    }

                    # The stream's close handler has already run; see the twin
                    # in the disconnect-wait loop below. A clean end never
                    # reaches here: the park at the top of this loop answers
                    # it first.
                    return await $disconnect->($parked)
                        if !$weak_self->_h2_stream_alive($stream_id);

                    if (!$ss->{body_pending}) {
                        $ss->{body_pending} = Future->new;
                    }
                    $parked = 1;
                    await $ss->{body_pending};

                    $ss = $weak_self->{h2_streams}{$stream_id};
                    # A clean end is answered at the top of this loop, which
                    # never touches $ss, so the entry going away does not take
                    # the scope's own ending with it.
                    return await $disconnect->($parked)
                        unless $ss || _h2_scope_end_event($stream_state);
                }

                # body_complete can flip true on the very wake that also
                # queued a terminal event -- e.g. _h2_on_close sets
                # body_complete AND pushes sse.disconnect before waking
                # body_pending. That queued event must win over delivering
                # a (possibly truncated) body: the loop's own queue check
                # only runs at the top of an iteration, so a queue entry
                # that arrives on the wake that also satisfies the while
                # condition is never seen there.
                if (@{$ss->{receive_queue}}) {
                    return shift @{$ss->{receive_queue}};
                }

                $ss->{sse_request_sent} = 1;
                return {
                    type => 'sse.request',
                    body => $ss->{body},
                    more => 0,
                };
            }

            # Wait for disconnect
            while (1) {
                # A call already parked when the scope ends cleanly is
                # answered with that end. Asked ahead of the queue, because the
                # stream closing after the end still queues the transport's own
                # disconnect, and it is the scope's ending, not the
                # transport's, that decides what this call reports.
                if (my $f = $scope_end->($parked)) { return await $f }

                if (@{$ss->{receive_queue}}) {
                    return shift @{$ss->{receive_queue}};
                }

                if ($weak_self->{closed}) {
                    return await $disconnect->($parked);
                }

                # Nothing left for this call to wait for, on either count:
                # this stream's close handler has already run (see the twin in
                # _h2_create_receive for why parking here would strand the
                # call), or the scope has already delivered its disconnect --
                # "once this event has been delivered the scope is over, and a
                # further receive() resolves with the same sse.disconnect
                # again" (Www.pod "SSE Disconnect - receive event"). The two
                # are not the same moment: a server-decided end (the idle
                # timeout, a shutdown) delivers the event through
                # _h2_end_sse_stream and only marks the stream ending, leaving
                # the final END_STREAM to the data callback, which emits it
                # once the stream's send queue has drained -- so a peer that
                # stops reading holds the stream open for as long as it likes
                # after the scope is over.
                return await $disconnect->($parked)
                    if !$weak_self->_h2_stream_alive($stream_id)
                    || $ss->{sse_disconnect_delivered};

                if (!$ss->{body_pending}) {
                    $ss->{body_pending} = Future->new;
                }
                $parked = 1;
                await $ss->{body_pending};

                $ss = $weak_self->{h2_streams}{$stream_id};
                # A clean end is answered at the top of this loop, which never
                # touches $ss, so the entry going away does not take the
                # scope's own ending with it.
                return await $disconnect->($parked)
                    unless $ss || _h2_scope_end_event($stream_state);
            }
        })->();

        return $future;
    };
}

sub _h2_create_sse_send {
    my ($self, $stream_id, $stream_state) = @_;

    weaken(my $weak_self = $self);
    my $seq = 'initial';

    # Streaming state for the data-provider pull pattern. The send queue lives on
    # per-stream state ($ss->{send_queue} / $ss->{send_queue_bytes}) so the
    # pagi.transport handle can measure THIS stream's backlog. $streaming_started
    # stays closure-local.
    my $streaming_started = 0;

    # Refusing the stream goes out through the ordinary HTTP send path;
    # that closure publishes its http state straight into this scope's.
    my $refusal_send;

    # Data callback for nghttp2's streaming response. Pulls from the per-stream
    # queue; SSE responses stay open, so this never signals EOF (returns eof=0),
    # or undef to defer when the queue is empty.
    my $data_callback = sub {
        my ($cb_stream_id, $max_len) = @_;

        my $ss = $weak_self && $weak_self->{h2_streams}{$stream_id};
        return undef unless $ss;
        my $q = $ss->{send_queue} ||= [];

        if (@$q) {
            my $chunk = shift @$q;
            # Respect max_len — XS truncates without preserving remainder
            if (length($chunk) > $max_len) {
                unshift @$q, substr($chunk, $max_len);
                $chunk = substr($chunk, 0, $max_len);
            }
            $ss->{send_queue_bytes} -= length($chunk);

            # Per-stream backpressure: once this stream's queue falls below the
            # low watermark, release any producer blocked in
            # _h2_wait_for_stream_drain. This runs inside nghttp2's extract(), so
            # resolve on the next loop tick — completing the Future resumes the
            # awaiting producer synchronously, and it must not re-enter nghttp2.
            if (($ss->{send_queue_bytes} // 0) < $weak_self->{write_low_watermark}
                    && $ss->{stream_drain_waiters} && @{$ss->{stream_drain_waiters}}) {
                my @waiters = splice @{$ss->{stream_drain_waiters}};
                $weak_self->{server}->loop->later(sub {
                    $_->done for grep { !$_->is_ready } @waiters;
                });
            }

            # Fire the app's on_drain hysteresis callbacks once this stream's
            # queue falls below the low watermark. Deferred for the same reason:
            # an on_drain callback may call $send, which would re-enter nghttp2.
            if (($ss->{send_queue_bytes} // 0) < $weak_self->{write_low_watermark}
                    && $ss->{transport_drain_fires} && @{$ss->{transport_drain_fires}}) {
                my @fires = splice @{$ss->{transport_drain_fires}};
                $weak_self->{server}->loop->later(sub {
                    $_->() for @fires;
                });
            }

            return ($chunk, 0);  # SSE streams never EOF via data_callback
        }

        # Queue empty. If the application closed this stream (sse.close), emit a
        # final empty DATA frame with END_STREAM to terminate it; otherwise defer.
        return ('', 1) if ($ss->{seq_state} // '') eq 'closed'
                       || $ss->{sse_server_ending};


        # Queue empty — defer (NGHTTP2_ERR_DEFERRED in the C layer)
        return undef;
    };

    return async sub {
        my ($event) = @_;
        return unless $weak_self;

        my $type = $event->{type} // '';

        # Once the machine has already recorded this stream as closed --
        # either an app-initiated sse.close or a completed refusal response
        # -- the machine, not the stream-gone check below, decides what
        # happens next: idempotent no-op for a repeat sse.close, croak for
        # anything else (refusal_complete has no idempotent case; it croaks
        # unconditionally). h2_streams entries for a closed stream are
        # reclaimed asynchronously by _h2_on_close, so by the time a
        # post-close send arrives $ss may already be gone.
        my $already_closed = ($seq eq 'closed' || $seq eq 'refusal_complete');

        my $ss = $weak_self->{h2_streams}{$stream_id};
        # A doomed-but-still-present entry (h2_closed set but not yet
        # deleted -- see the 413-overrun branch in _h2_on_body) is treated
        # the same as an absent one: both are post-close sends and must
        # silently no-op, not reach nghttp2 a second time on this stream id.
        return if (!$ss || $ss->{h2_closed}) && !$already_closed;

        return if $weak_self->{closed} && !$already_closed;

        # Reset THIS stream's SSE idle timer on send activity (skip once fully closed)
        $weak_self->_h2_reset_sse_idle_timer($ss) unless $already_closed;

        # Mandatory event validation and sequencing (PAGI spec compliance).
        PAGI::Server::EventValidator::validate_sse_send(
            $event, { extensions => $weak_self->{extensions} });
        $seq = PAGI::Server::EventValidator::advance_sse($seq, $event);
        $ss->{seq_state} = $seq if $ss;

        if ($type =~ /^http\.response\./) {
            # Refusing the stream (Www.pod): identical on the wire to the same
            # response on an http scope, so that path writes it. The stream's
            # keepalive and idle timers cannot be armed here -- advance_sse
            # never admits sse.keepalive before sse.start -- so there is
            # nothing to release beyond what _h2_on_close already does.
            $refusal_send //= $weak_self->_h2_create_send($stream_id, $ss,
                refusal  => 1,
                on_state => sub {
                    return unless $weak_self;
                    $seq = $_[0] eq 'complete' ? 'refusal_complete' : 'refusing';
                    my $s = $weak_self->{h2_streams}{$stream_id};
                    $s->{seq_state} = $seq if $s;
                },
            );
            my $ok = eval { await $refusal_send->($event); 1 };
            my $error = $@;
            die $error unless $ok;
            return;
        }

        if ($type eq 'sse.start') {
            return if $ss->{response_started};
            $ss->{response_started} = 1;
            my $status = $event->{status} // 200;
            my $headers = $event->{headers} // [];

            # Ensure Content-Type is text/event-stream
            my $has_content_type = 0;
            for my $h (@$headers) {
                if (lc($h->[0]) eq 'content-type') {
                    $has_content_type = 1;
                    last;
                }
            }

            my @final_headers;
            for my $h (@$headers) {
                push @final_headers, [_validate_header_name($h->[0]), _validate_header_value($h->[1])];
            }
            # RFC 9113 8.2.2 / design 13.3 — strip app-supplied connection,
            # transfer-encoding, etc. before submission.
            @final_headers = @{ $weak_self->_h2_strip_connection_headers(\@final_headers) };
            if (!$has_content_type) {
                push @final_headers, ['content-type', 'text/event-stream'];
            }
            # Cache-Control and Date: server-supplied only when the app didn't
            # supply them (design doc section 11.4).
            unless (grep { lc($_->[0]) eq 'cache-control' } @final_headers) {
                push @final_headers, ['cache-control', 'no-cache'];
            }
            # Server-supplied Date header (HTTP/1.1 parity) — the h1 SSE path adds
            # this too; add it unless the app supplied one.
            unless (grep { lc($_->[0]) eq 'date' } @final_headers) {
                push @final_headers, ['date', $weak_self->{protocol}->format_date];
            }

            $streaming_started = 1;
            $ss->{send_queue}       //= [];
            $ss->{send_queue_bytes} //= 0;
            $weak_self->{h2_session}->submit_response_streaming(
                $stream_id,
                status        => $status,
                headers       => \@final_headers,
                data_callback => $data_callback,
            );
            $weak_self->_h2_write_pending;

            # Protocol-specific keepalive writer (HTTP/2 DATA frames), scoped to
            # THIS stream (design section 11.3): a second multiplexed SSE stream
            # must not steal or replace this one's writer. Keepalive bytes are
            # counted in the per-stream backlog so buffered_amount stays
            # accurate, but they do not poke the watermark callbacks — a server
            # heartbeat is not an application send.
            $ss->{sse_ka_writer} = sub {
                my ($text) = @_;
                return unless $weak_self;
                return if $weak_self->{closed};
                my $ss = $weak_self->{h2_streams}{$stream_id} or return;
                # PAGI Www.pod "Send SSE": encode to UTF-8 exactly once, at
                # the wire boundary — all queue-length math below is on the
                # resulting BYTE string.
                my $bytes = eval { Encode::encode('UTF-8', $text, Encode::FB_CROAK) };
                die "sse payload is not encodable as UTF-8: $@" unless defined $bytes;
                push @{$ss->{send_queue} ||= []}, $bytes;
                $ss->{send_queue_bytes} = ($ss->{send_queue_bytes} // 0) + length $bytes;
                $weak_self->{h2_session}->resume_stream($stream_id);
                $weak_self->_h2_write_pending;
            };

            # Start THIS stream's SSE idle timer if configured
            $weak_self->_h2_start_sse_idle_timer($stream_id, $ss);
        }
        elsif ($type eq 'sse.send') {
            return unless $ss->{response_started};

            # Per-stream backpressure: bound on THIS stream's queue, not the
            # shared TCP buffer (meaningless across multiplexed h2 streams).
            if (($ss->{send_queue_bytes} // 0) >= $weak_self->{write_high_watermark}) {
                await $weak_self->_h2_wait_for_stream_drain($stream_id);
                return unless $weak_self;
                return if $weak_self->{closed};
                return unless $weak_self->{h2_streams}{$stream_id};
            }

            my $sse_data = _format_sse_event($event);
            # PAGI Www.pod "Send SSE": encode to UTF-8 exactly once, at the
            # wire boundary — a failed encode fails this send's Future.
            my $bytes = eval { Encode::encode('UTF-8', $sse_data, Encode::FB_CROAK) };
            die "sse payload is not encodable as UTF-8: $@" unless defined $bytes;
            push @{$ss->{send_queue} ||= []}, $bytes;
            $ss->{send_queue_bytes} = ($ss->{send_queue_bytes} // 0) + length $bytes;
            # Synchronous — app send path, not nghttp2 extract — so on_high_water
            # may fire here to tell the app to pause its source.
            $ss->{transport_state}->_check_watermarks if $ss->{transport_state};
            $weak_self->{h2_session}->resume_stream($stream_id);
            $weak_self->_h2_write_pending;
        }
        elsif ($type eq 'sse.comment') {
            return unless $ss->{response_started};

            my $comment = _format_sse_comment($event);
            my $bytes = eval { Encode::encode('UTF-8', $comment, Encode::FB_CROAK) };
            die "sse payload is not encodable as UTF-8: $@" unless defined $bytes;
            push @{$ss->{send_queue} ||= []}, $bytes;
            $ss->{send_queue_bytes} = ($ss->{send_queue_bytes} // 0) + length $bytes;
            $ss->{transport_state}->_check_watermarks if $ss->{transport_state};
            $weak_self->{h2_session}->resume_stream($stream_id);
            $weak_self->_h2_write_pending;
        }
        elsif ($type eq 'sse.keepalive') {
            my $interval = $event->{interval} // 0;
            my $comment = $event->{comment};

            if ($interval > 0) {
                $weak_self->_h2_start_sse_keepalive($stream_id, $ss, $interval, $comment);
            }
            else {
                $weak_self->_h2_stop_sse_keepalive($ss);
            }
        }
        elsif ($type eq 'sse.close') {
            # A repeat sse.close after the stream's h2_streams entry has
            # already been reclaimed (see the already-closed comment above)
            # is the idempotent no-op advance_sse just allowed; nothing left
            # to do.
            return unless $ss;

            # This stream is ending -- stop its keepalive and idle timers so
            # neither fires (or leaks) after the stream is reclaimed.
            $weak_self->_h2_stop_sse_keepalive($ss);
            $weak_self->_h2_stop_sse_idle_timer($ss);

            # sse.close is this scope's clean terminal event -- END_STREAM
            # is queued just below -- so it ends the scope here, before the
            # flush, and the answer a parked receive gets is this end rather
            # than whatever the stream closing under the flush would hand it.
            $weak_self->_h2_end_scope_output($ss);

            # End THIS HTTP/2 stream now: flush remaining queued events, then the
            # data_callback emits a final END_STREAM frame. `reason` is
            # server-side only and is never written to the wire.
            # seq_state is already 'closed' -- the mirror above runs before
            # this arm, so the data_callback's END_STREAM decision and
            # _h2_on_close's per-scope clean computation both see it even if
            # resuming the stream closes it synchronously.
            $weak_self->{h2_session}->resume_stream($stream_id);
            $weak_self->_h2_write_pending;
        }
        elsif ($type eq 'http.fullflush') {
            # Hand any pending frames to the session's write path (design §8.4).
            $weak_self->{h2_session}->resume_stream($stream_id) if $streaming_started;
            $weak_self->_h2_write_pending;
        }

        return;
    };
}

# Parse one h2 stream's inbound WebSocket frames and act on them. Protocol
# replies queued below (a pong, a close echo) are not flushed by this sub: from
# _h2_on_body it runs inside feed's mem_recv and _h2_process_data flushes once
# feed returns; from the websocket.accept arm's drain of frames that arrived
# before the handshake it runs outside feed, and the reply waits for the next
# flush.
sub _h2_process_ws_frames {
    my ($self, $stream_id, $stream, $data) = @_;

    my $frame = $stream->{ws_frame};
    return unless $frame;

    $frame->append($data);

    while (defined(my $bytes = $frame->next_bytes)) {
        my $opcode = $frame->opcode;

        # RFC 6455 Section 5.2: RSV1-3 MUST be 0 unless extension defines
        # meaning. PAGI doesn't support compression extensions, so RSV must
        # always be 0. Same enforcement as h1's _process_websocket_frames
        # (Www.pod: transport-agnostic framing enforcement, and RFC 8441's
        # "identical to HTTP/1.1" claim).
        my $rsv = $frame->rsv;
        if ($rsv && ref($rsv) eq 'ARRAY') {
            if (grep { $_ } @$rsv) {
                # Server-initiated protocol close (Www.pod: RFC code +
                # 'protocol_error'); _h2_ws_close ends the scope.
                $self->_h2_ws_close($stream_id, code => 1002,
                    text => 'RSV bits must be 0', reason => 'protocol_error');
                return;
            }
        }

        # RFC 6455 Section 5.2: Opcodes 3-7 and 11-15 (0xB-0xF) are reserved.
        # Must fail connection with 1002 Protocol Error.
        if (($opcode >= 3 && $opcode <= 7) || ($opcode >= 11 && $opcode <= 15)) {
            # Server-initiated protocol close (Www.pod: RFC code +
            # 'protocol_error'); _h2_ws_close ends the scope.
            $self->_h2_ws_close($stream_id, code => 1002,
                text => 'Reserved opcode', reason => 'protocol_error');
            return;
        }

        # RFC 6455 Section 5.5: Control frames (close/ping/pong) MUST have
        # payload length <= 125 bytes.
        if (($opcode == 8 || $opcode == 9 || $opcode == 10) && length($bytes) > 125) {
            # Server-initiated protocol close (Www.pod: RFC code +
            # 'protocol_error'); _h2_ws_close ends the scope.
            $self->_h2_ws_close($stream_id, code => 1002,
                text => 'Control frame too large', reason => 'protocol_error');
            return;
        }

        if ($opcode == 1) {
            # Text frame
            my $text = eval { Encode::decode('UTF-8', $bytes, Encode::FB_CROAK) };
            unless (defined $text) {
                # Server-initiated protocol close (Www.pod: RFC code +
                # 'protocol_error'); _h2_ws_close ends the scope.
                $self->_h2_ws_close($stream_id, code => 1007,
                    text => 'Invalid UTF-8', reason => 'protocol_error');
                return;
            }
            # Check queue limit before adding (DoS protection) -- same cap
            # and pairing h1 enforces (Www.pod pins 1008 <-> queue_overflow).
            if (@{$stream->{receive_queue}} >= $self->{max_receive_queue}) {
                # Www.pod pins 1008 <-> queue_overflow; _h2_ws_close ends the
                # scope. The peer gets the short frame text, the application
                # the cap that was hit.
                $self->_h2_ws_close($stream_id, code => 1008,
                    text   => 'Message queue overflow',
                    reason => 'queue_overflow',
                    detail => "inbound message queue at $self->{max_receive_queue}");
                return;
            }
            push @{$stream->{receive_queue}}, {
                type => 'websocket.receive',
                text => $text,
            };
        }
        elsif ($opcode == 2) {
            # Binary frame
            # Check queue limit before adding (DoS protection) -- same cap
            # and pairing h1 enforces (Www.pod pins 1008 <-> queue_overflow).
            if (@{$stream->{receive_queue}} >= $self->{max_receive_queue}) {
                # Www.pod pins 1008 <-> queue_overflow; _h2_ws_close ends the
                # scope. The peer gets the short frame text, the application
                # the cap that was hit.
                $self->_h2_ws_close($stream_id, code => 1008,
                    text   => 'Message queue overflow',
                    reason => 'queue_overflow',
                    detail => "inbound message queue at $self->{max_receive_queue}");
                return;
            }
            push @{$stream->{receive_queue}}, {
                type  => 'websocket.receive',
                bytes => $bytes,
            };
        }
        elsif ($opcode == 8) {
            # Close frame -- the WS session on this stream is ending, one way
            # or another (a validation failure below closes it too), so stop
            # this stream's keepalive up front.
            $self->_h2_stop_ws_keepalive($stream);

            my ($code, $reason) = (1005, '');

            # RFC 6455 Section 5.5.1: Close frame payload is 0 or >=2 bytes
            if (length($bytes) == 1) {
                # Server-initiated protocol close (Www.pod: RFC code +
                # 'protocol_error'); _h2_ws_close ends the scope.
                $self->_h2_ws_close($stream_id, code => 1002,
                    text => 'Invalid close frame', reason => 'protocol_error');
                return;
            }

            if (length($bytes) >= 2) {
                $code = unpack('n', substr($bytes, 0, 2));
                $reason = substr($bytes, 2) // '';

                # RFC 6455 Section 7.4.1: Validate close code
                my $valid_code = 0;
                if ($code == 1000 || $code == 1001 || $code == 1002 || $code == 1003) {
                    $valid_code = 1;
                }
                elsif ($code >= 1007 && $code <= 1011) {
                    $valid_code = 1;
                }
                elsif ($code >= 3000 && $code <= 4999) {
                    $valid_code = 1;
                }
                unless ($valid_code) {
                    # Server-initiated protocol close (Www.pod: RFC code +
                    # 'protocol_error'); _h2_ws_close ends the scope.
                    $self->_h2_ws_close($stream_id, code => 1002,
                        text => 'Invalid close code', reason => 'protocol_error');
                    return;
                }

                # RFC 6455: Close reason must be valid UTF-8
                if (length($reason) > 0) {
                    my $reason_copy = $reason;
                    my $decoded = eval { Encode::decode('UTF-8', $reason_copy, Encode::FB_CROAK) };
                    unless (defined $decoded) {
                        # Server-initiated protocol close (Www.pod: RFC code +
                        # 'protocol_error'); _h2_ws_close ends the scope.
                        $self->_h2_ws_close($stream_id, code => 1007,
                            text   => 'Invalid UTF-8 in close reason',
                            reason => 'protocol_error');
                        return;
                    }
                }
            }

            # Send close frame back + END_STREAM. Queued (not submit_data)
            # so the data_callback's own eof_pending merge puts END_STREAM on
            # this exact chunk, same as the previous submit_data(..., 1) did.
            # Not an application send — no transport_state->_check_watermarks.
            my $close_frame = Protocol::WebSocket::Frame->new(
                type   => 'close',
                buffer => pack('n', $code) . $reason,
            );
            my $close_bytes = $close_frame->to_bytes;
            push @{$stream->{send_queue} ||= []}, $close_bytes;
            $stream->{send_queue_bytes} = ($stream->{send_queue_bytes} // 0) + length $close_bytes;
            $stream->{ws_eof_pending} = 1;
            $self->{h2_session}->resume_stream($stream_id);
            # No flush here — see this sub's note on who writes it

            # A completed closing handshake is a clean end regardless of the
            # peer's close code (Www.pod "Meaning per scope"). Receive-side,
            # so the send state machine never sees it. Marked BEFORE
            # _h2_ws_enqueue_disconnect: that call can wake a parked receive()
            # and resume the app synchronously (Future::AsyncAwait resumes
            # inline off ->done), and the resumed app may return and reach
            # _h2_dispatch_stream's post-await check before this line would
            # otherwise have run.
            $stream->{ws_peer_closed} = 1;

            # Peer's Close frame: its own code (1005 default when the frame
            # carried none) and reason text (Www.pod "Disconnect - receive event").
            $self->_h2_ws_enqueue_disconnect($stream, $code, $reason);
        }
        elsif ($opcode == 9) {
            # Ping — respond with pong. Queued, not an application send.
            my $pong = Protocol::WebSocket::Frame->new(
                type   => 'pong',
                buffer => $bytes,
            );
            my $pong_bytes = $pong->to_bytes;
            push @{$stream->{send_queue} ||= []}, $pong_bytes;
            $stream->{send_queue_bytes} = ($stream->{send_queue_bytes} // 0) + length $pong_bytes;
            $self->{h2_session}->resume_stream($stream_id);
            # No flush here — see this sub's note on who writes it
        }
        elsif ($opcode == 10) {
            # Pong — clear this stream's keepalive wait flag (response to
            # our own ping); cancel only the per-stream pong-timeout
            # Countdown, not the periodic ping timer itself.
            $self->_h2_cancel_ws_pong_timeout($stream);
        }
    }

    $self->_h2_wake_pending($stream);
}

# Send a Close frame with END_STREAM on one h2 WebSocket stream. The WS
# session on that stream is over once this returns, so this is also where
# the stream's keepalive is released -- every closure path funnels through
# here, and releasing it at the funnel is what keeps a ping timer from
# outliving its stream (it would otherwise keep pinging a half-closed
# stream for the life of the connection).
#
# The Close frame is queued, never flushed here. Reached from inside feed's
# mem_recv the flush is _h2_process_data's, once feed returns; reached from
# outside feed the caller must call _h2_write_pending itself.
sub _h2_ws_close {
    my ($self, $stream_id, %a) = @_;      # code, text, reason, detail

    my $ss = $self->{h2_streams}{$stream_id};
    return unless $ss;

    # Idempotency guard: a stream already closing (a Close frame already
    # queued, ws_eof_pending true) must not queue a second one. Without
    # this, a burst of several frames that each independently warrant
    # closure -- e.g. more inbound messages than max_receive_queue allows,
    # already buffered in the parser before the first violation's `return`
    # unwinds -- re-enters this sub once per remaining buffered frame (each
    # later call to _h2_process_ws_frames resumes parsing where the last
    # one left off) and would otherwise queue one Close frame per frame
    # instead of the one the wire is supposed to see. The ending below still
    # runs on a re-entry: recording, marking and enqueueing are each
    # first-wins, so a repeat is a no-op, and the first frame to reach here
    # after an application's own websocket.close still ends the scope.
    unless ($ss->{ws_eof_pending}) {
        $self->_h2_stop_ws_keepalive($ss);

        # Queued (not submit_data) so the data_callback's own eof_pending merge
        # puts END_STREAM on this exact chunk, same as the previous
        # submit_data(..., 1) did.
        my $frame = Protocol::WebSocket::Frame->new(
            type   => 'close',
            buffer => pack('n', $a{code}) . ($a{text} // ''),
        );
        my $bytes = $frame->to_bytes;
        push @{$ss->{send_queue} ||= []}, $bytes;
        $ss->{send_queue_bytes} = ($ss->{send_queue_bytes} // 0) + length $bytes;
        $ss->{ws_eof_pending} = 1;
        $self->{h2_session}->resume_stream($stream_id)
            if $self->_h2_stream_alive($stream_id);
    }

    # A server-detected protocol violation, queue overflow or abandoned scope
    # is an abnormal end (Www.pod "Meaning per scope"), so the closes that
    # come from this server name themselves here, after the frame is queued
    # and before anything can wake the application. The words going to the
    # peer double as this stream's disconnect detail unless the caller gives
    # one of its own. The application's own websocket.close names nothing,
    # because its clean end is already recorded as the send state 'closed'.
    return unless defined $a{reason};
    my $detail = $a{detail}
              // ((defined $a{text} && length $a{text}) ? $a{text} : undef);
    $self->_h2_end_ws_stream($ss, reason => $a{reason}, code => $a{code},
        (defined $detail ? (detail => $detail) : ()));
    return;
}

# HTTP/2 per-stream WebSocket keepalive (design section 10.2 -- the h2
# analogue of _start_ws_keepalive/_stop_ws_keepalive). h1 shares one TCP
# stream per connection, so its keepalive state and timers live on $self;
# h2 multiplexes many WebSocket streams per connection, so this state and
# these timers live on the per-stream hash ($ss, aka $self->{h2_streams}
# {$stream_id}) and the ping is delivered as an h2 DATA frame via the
# stream's own send queue + data-provider callback, same as every other
# ws frame on this stream, rather than a raw stream write.
sub _h2_start_ws_keepalive {
    my ($self, $stream_id, $ss, $interval, $timeout) = @_;

    # Last event wins: stop whatever was running before applying new settings.
    $self->_h2_stop_ws_keepalive($ss);

    return unless $interval && $interval > 0;
    return unless $self->{server};

    $ss->{ws_ka_interval} = $interval;
    $ss->{ws_ka_timeout}  = $timeout // 0;

    weaken(my $weak_self = $self);
    weaken(my $weak_ss   = $ss);

    my $timer = IO::Async::Timer::Periodic->new(
        interval => $interval,
        on_tick  => sub {
            return unless $weak_self;
            return if $weak_self->{closed};
            return unless $weak_ss;
            return unless _ws_handshake_accepted($weak_ss->{seq_state});

            # Queued, not an application send (no transport_state watermark
            # poke) -- this timer callback runs outside feed(), so resume
            # AND flush explicitly, unlike the in-feed() queue pushes above.
            my $ping = Protocol::WebSocket::Frame->new(
                type   => 'ping',
                buffer => '',
            );
            my $ping_bytes = $ping->to_bytes;
            push @{$weak_ss->{send_queue} ||= []}, $ping_bytes;
            $weak_ss->{send_queue_bytes} = ($weak_ss->{send_queue_bytes} // 0) + length $ping_bytes;
            $weak_self->{h2_session}->resume_stream($stream_id);
            $weak_self->_h2_write_pending;

            # Start pong timeout if configured
            if ($weak_ss->{ws_ka_timeout} > 0) {
                $weak_ss->{ws_ka_waiting_pong} = 1;
                $weak_self->_h2_start_ws_pong_timeout($stream_id, $weak_ss);
            }
        },
    );

    $ss->{ws_ka_timer} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _h2_start_ws_pong_timeout {
    my ($self, $stream_id, $ss) = @_;

    # Don't start another timeout if one is running
    return if $ss->{ws_ka_pong_timer};
    return unless $ss->{ws_ka_timeout} > 0;
    return unless $self->{server};

    weaken(my $weak_self = $self);
    weaken(my $weak_ss   = $ss);

    my $timer = IO::Async::Timer::Countdown->new(
        delay => $ss->{ws_ka_timeout},
        on_expire => sub {
            return unless $weak_self;
            return if $weak_self->{closed};
            return unless $weak_ss;

            if ($weak_ss->{ws_ka_waiting_pong}) {
                # No pong received within timeout - close only THIS stream.
                if ($weak_self->{server} && $weak_self->{server}->can('_log')) {
                    $weak_self->{server}->_log(warn =>
                        "HTTP/2 WebSocket stream $stream_id keepalive timeout - no pong received within $weak_ss->{ws_ka_timeout}s");
                }

                $weak_self->_h2_stop_ws_keepalive($weak_ss);

                # RFC 6455 section 7.4.1: 1006 MUST NOT be set as the status
                # code of a Close control frame -- it means "the connection
                # dropped with no close handshake". So a keepalive timeout
                # does not send a Close frame at all: it tears the stream
                # down with RST_STREAM, the HTTP/2 analogue of h1 dropping
                # the transport. The app still sees 1006/'keepalive_timeout'.
                #
                # Order dependency: the ending MUST precede the flush. The
                # flush is the only call here that can synchronously drive
                # on_stream_close (which would otherwise end the scope with
                # the generic 1006/'client_closed'), and recording, marking
                # and enqueueing are each first-wins, so ending first is what
                # makes 'keepalive_timeout' the reason the app observes.
                # Do not hoist the reset above this call.
                $weak_self->_h2_end_ws_stream($weak_ss, reason => 'keepalive_timeout',
                    code => 1006, detail => "no pong within $weak_ss->{ws_ka_timeout}s");
                # _h2_reset_stream flushes: this runs outside feed() (async
                # timer callback), so the RST needs an explicit write.
                $weak_self->_h2_reset_stream($stream_id, _h2_rst_cancel_code());
            }
        },
    );

    $ss->{ws_ka_pong_timer} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _h2_cancel_ws_pong_timeout {
    my ($self, $ss) = @_;
    return unless $ss;

    $ss->{ws_ka_waiting_pong} = 0;

    return unless $ss->{ws_ka_pong_timer};
    $ss->{ws_ka_pong_timer}->stop if $ss->{ws_ka_pong_timer}->is_running;
    if ($self->{server}) {
        $self->{server}->remove_child($ss->{ws_ka_pong_timer});
    }
    $ss->{ws_ka_pong_timer} = undef;
}

sub _h2_stop_ws_keepalive {
    my ($self, $ss) = @_;
    return unless $ss;

    # Stop pong timeout first
    $self->_h2_cancel_ws_pong_timeout($ss);

    return unless $ss->{ws_ka_timer};
    $ss->{ws_ka_timer}->stop if $ss->{ws_ka_timer}->is_running;
    if ($self->{server}) {
        $self->{server}->remove_child($ss->{ws_ka_timer});
    }
    $ss->{ws_ka_timer}     = undef;
    $ss->{ws_ka_interval}  = 0;
    $ss->{ws_ka_timeout}   = 0;
}

# HTTP/2 per-stream SSE keepalive and idle timeout (design section 11.3 --
# the h2 analogue of _start_sse_keepalive/_stop_sse_keepalive and
# _start_sse_idle_timer/_reset_sse_idle_timer/_stop_sse_idle_timer). HTTP/1
# carries only one long-lived scope per connection, so its keepalive/idle
# state and timers stay on $self; HTTP/2 multiplexes many SSE streams per
# connection, so this state and these timers live on the per-stream hash
# ($ss, aka $self->{h2_streams}{$stream_id}) instead -- starting or updating
# keepalive on one stream must never stop, replace, or redirect another
# stream's timer or writer.
sub _h2_start_sse_keepalive {
    my ($self, $stream_id, $ss, $interval, $comment) = @_;

    # Last event wins: stop whatever was running before applying new settings.
    $self->_h2_stop_sse_keepalive($ss);

    return unless $interval && $interval > 0;
    return unless $self->{server};

    $ss->{sse_ka_interval} = $interval;
    $ss->{sse_ka_comment}  = $comment // '';

    weaken(my $weak_self = $self);
    weaken(my $weak_ss   = $ss);

    my $timer = IO::Async::Timer::Periodic->new(
        interval => $interval,
        on_tick  => sub {
            return unless $weak_self;
            return if $weak_self->{closed};
            return unless $weak_ss;

            my $text = $weak_ss->{sse_ka_comment};
            $text = ":$text" unless $text =~ /^:/;
            my $formatted = "$text\n\n";

            if (my $writer = $weak_ss->{sse_ka_writer}) {
                $writer->($formatted);
            }
        },
    );

    $ss->{sse_ka_timer} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _h2_stop_sse_keepalive {
    my ($self, $ss) = @_;
    return unless $ss;

    return unless $ss->{sse_ka_timer};
    $ss->{sse_ka_timer}->stop if $ss->{sse_ka_timer}->is_running;
    if ($self->{server}) {
        $self->{server}->remove_child($ss->{sse_ka_timer});
    }
    $ss->{sse_ka_timer}    = undef;
    $ss->{sse_ka_interval} = 0;
    $ss->{sse_ka_comment}  = '';
}

# Per-stream SSE idle timeout: resets on send activity (design section
# 11.3's "idle timer and activity reset"). On expiry, only THIS stream ends
# -- mirroring an app-initiated sse.close server-side -- so a stalled SSE
# stream cannot take down sibling streams multiplexed on the same h2
# connection.
sub _h2_start_sse_idle_timer {
    my ($self, $stream_id, $ss) = @_;

    return unless $self->{sse_idle_timeout} && $self->{sse_idle_timeout} > 0;
    return unless $self->{server};
    return if $ss->{sse_idle_timer};

    weaken(my $weak_self = $self);
    weaken(my $weak_ss   = $ss);

    my $timer = IO::Async::Timer::Countdown->new(
        delay => $self->{sse_idle_timeout},
        on_expire => sub {
            return unless $weak_self;
            return if $weak_self->{closed};
            return unless $weak_ss;
            return if ($weak_ss->{seq_state} // '') eq 'closed'
                   || $weak_ss->{sse_server_ending};   # already ending some other way


            if ($weak_self->{server} && $weak_self->{server}->can('_log')) {
                $weak_self->{server}->_log(warn =>
                    "HTTP/2 SSE stream $stream_id idle timeout ($weak_self->{sse_idle_timeout}s) - closing stream");
            }

            $weak_self->_h2_stop_sse_keepalive($weak_ss);
            $weak_self->_h2_stop_sse_idle_timer($weak_ss);

            # End THIS stream only, the same way an app-initiated sse.close
            # does: mark it closing so the data_callback emits the final
            # END_STREAM frame once the queue drains -- sibling streams on
            # this connection are unaffected. A server-decided end, not an
            # application sse.close: the send state machine never saw an
            # event, so this is its own fact. It gates exactly what the app
            # close gates -- the data_callback emitting the final END_STREAM
            # frame once the queue drains, and this timer's own
            # already-ending guard.
            $weak_ss->{sse_server_ending} = 1;

            # Record, mark and deliver the ending here, before the flush
            # below: _h2_write_pending can resolve a parked send and resume
            # the application inline, and Www.pod "State Transition Order"
            # requires the object to be terminal with this scope's own token
            # before anything wakes the application. Leaving the mark to
            # _h2_on_close would let an abort() landing in that window name
            # the object something the queued event does not.
            $weak_self->_h2_end_sse_stream($weak_ss, reason => 'idle_timeout',
                detail => "no traffic for $weak_self->{sse_idle_timeout}s");

            $weak_self->{h2_session}->resume_stream($stream_id);
            $weak_self->_h2_write_pending;
        },
    );

    $ss->{sse_idle_timer} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _h2_reset_sse_idle_timer {
    my ($self, $ss) = @_;
    return unless $ss && $ss->{sse_idle_timer};
    $ss->{sse_idle_timer}->reset;
    $ss->{sse_idle_timer}->start unless $ss->{sse_idle_timer}->is_running;
}

sub _h2_stop_sse_idle_timer {
    my ($self, $ss) = @_;
    return unless $ss && $ss->{sse_idle_timer};
    $ss->{sse_idle_timer}->stop if $ss->{sse_idle_timer}->is_running;
    if ($self->{server}) {
        $self->{server}->remove_child($ss->{sse_idle_timer});
    }
    $ss->{sse_idle_timer} = undef;
}

# Request stall timeout - closes connection if no I/O activity during request processing
sub _start_stall_timer {
    my ($self) = @_;

    return unless $self->{request_timeout} && $self->{request_timeout} > 0;
    return unless $self->{server};
    return if $self->{stall_timer};  # Already running

    weaken(my $weak_self = $self);

    my $timer = IO::Async::Timer::Countdown->new(
        delay => $self->{request_timeout},
        on_expire => sub {
            return unless $weak_self;
            return if $weak_self->{closed};
            # Log the timeout
            if ($weak_self->{server} && $weak_self->{server}->can('_log')) {
                $weak_self->{server}->_log(warn =>
                    "Request stall timeout ($weak_self->{request_timeout}s) - closing connection");
            }
            $weak_self->_handle_disconnect_and_close('client_timeout',
                detail => "request stalled for $weak_self->{request_timeout}s");
        },
    );
    $self->{stall_timer} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _reset_stall_timer {
    my ($self) = @_;

    return unless $self->{stall_timer};
    $self->{stall_timer}->reset;
    $self->{stall_timer}->start unless $self->{stall_timer}->is_running;
}

sub _stop_stall_timer {
    my ($self) = @_;

    return unless $self->{stall_timer};
    $self->{stall_timer}->stop if $self->{stall_timer}->is_running;
    if ($self->{server}) {
        $self->{server}->remove_child($self->{stall_timer});
    }
    $self->{stall_timer} = undef;
}

# WebSocket idle timeout - closes connection if no activity
sub _start_ws_idle_timer {
    my ($self) = @_;

    return unless $self->{ws_idle_timeout} && $self->{ws_idle_timeout} > 0;
    return unless $self->{server};
    return if $self->{ws_idle_timer};

    weaken(my $weak_self = $self);

    my $timer = IO::Async::Timer::Countdown->new(
        delay => $self->{ws_idle_timeout},
        on_expire => sub {
            return unless $weak_self;
            return if $weak_self->{closed};
            if ($weak_self->{server} && $weak_self->{server}->can('_log')) {
                $weak_self->{server}->_log(warn =>
                    "WebSocket idle timeout ($weak_self->{ws_idle_timeout}s) - closing connection");
            }
            $weak_self->_handle_disconnect_and_close('idle_timeout',
                detail => "no traffic for $weak_self->{ws_idle_timeout}s");
        },
    );
    $self->{ws_idle_timer} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _reset_ws_idle_timer {
    my ($self) = @_;

    return unless $self->{ws_idle_timer};
    $self->{ws_idle_timer}->reset;
    $self->{ws_idle_timer}->start unless $self->{ws_idle_timer}->is_running;
}

sub _stop_ws_idle_timer {
    my ($self) = @_;

    return unless $self->{ws_idle_timer};
    $self->{ws_idle_timer}->stop if $self->{ws_idle_timer}->is_running;
    if ($self->{server}) {
        $self->{server}->remove_child($self->{ws_idle_timer});
    }
    $self->{ws_idle_timer} = undef;
}

# SSE idle timeout - closes connection if no activity
sub _start_sse_idle_timer {
    my ($self) = @_;

    return unless $self->{sse_idle_timeout} && $self->{sse_idle_timeout} > 0;
    return unless $self->{server};
    return if $self->{sse_idle_timer};

    weaken(my $weak_self = $self);

    my $timer = IO::Async::Timer::Countdown->new(
        delay => $self->{sse_idle_timeout},
        on_expire => sub {
            return unless $weak_self;
            return if $weak_self->{closed};
            if ($weak_self->{server} && $weak_self->{server}->can('_log')) {
                $weak_self->{server}->_log(warn =>
                    "SSE idle timeout ($weak_self->{sse_idle_timeout}s) - closing connection");
            }
            $weak_self->_record_end($weak_self, reason => 'idle_timeout');
            $weak_self->_handle_disconnect_and_close('idle_timeout',
                detail => "no traffic for $weak_self->{sse_idle_timeout}s");
        },
    );
    $self->{sse_idle_timer} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _reset_sse_idle_timer {
    my ($self) = @_;

    return unless $self->{sse_idle_timer};
    $self->{sse_idle_timer}->reset;
    $self->{sse_idle_timer}->start unless $self->{sse_idle_timer}->is_running;
}

sub _stop_sse_idle_timer {
    my ($self) = @_;

    return unless $self->{sse_idle_timer};
    $self->{sse_idle_timer}->stop if $self->{sse_idle_timer}->is_running;
    if ($self->{server}) {
        $self->{server}->remove_child($self->{sse_idle_timer});
    }
    $self->{sse_idle_timer} = undef;
}

# ============================================================================
# Send-side backpressure support
# ============================================================================
#
# Prevents unbounded memory growth when apps send faster than slow clients
# can receive. Uses watermark-based flow control:
# - High watermark (default 1MB): pause sending when buffer exceeds this
# - Low watermark (default 256KB): resume sending when buffer drops below this
#
# The $send->() Future will block (await) when high watermark is exceeded,
# and resolve when buffer drains below low watermark.

sub _get_write_buffer_size {
    my ($self) = @_;

    return 0 unless $self->{stream};

    # Access IO::Async::Stream's internal write queue
    # IO::Async doesn't expose a public API for buffer size, so we access internals
    my $queue = $self->{stream}{writequeue} // [];
    my $total = 0;

    for my $writer (@$queue) {
        my $data = $writer->data;
        if (defined $data && !ref $data) {
            $total += length($data);
        }
    }

    return $total;
}

# HTTP/1.1: the transport handle reads the shared TCP write buffer. One
# connection is one stream here, so the IO::Async write queue is the per-stream
# backlog. The connection is held weakly so the handle never keeps it alive.
# arm_drain parks the $fire callback in _drain_fires, deliberately separate
# from _drain_waiters (blocking producer Futures, see _wait_for_drain) --
# teardown (_cancel_drain_waiters) resumes the latter but drops the former
# unfired, the same split h2 keeps between stream_drain_waiters and
# transport_drain_fires: the connection going away is not a drain.
sub _h1_transport_state {
    my ($self) = @_;
    weaken(my $w = $self);
    return PAGI::Server::TransportState->new(
        measure   => sub { $w ? $w->_get_write_buffer_size : 0 },
        high      => sub { $w ? $w->{write_high_watermark} : undef },
        low       => sub { $w ? $w->{write_low_watermark}  : undef },
        arm_drain => sub {
            my $fire = shift;
            return unless $w;
            push @{$w->{_drain_fires}}, $fire;
            $w->_setup_drain_detection;
        },
    );
}

# HTTP/2: the transport handle reads this stream's send queue, not the shared
# TCP write buffer — under h2, N streams multiplex one connection, so that
# buffer is the whole connection's backlog (meaningless per stream). $ss is the
# per-stream state hashref; it's held directly (it is the stream's own state),
# while $self is weakened so the handle never keeps the connection alive.
# arm_drain parks the $fire callback on the stream; the data_callback pull fires
# it (deferred) when the queue crosses below the low watermark. Kept separate
# from stream_drain_waiters: those are Futures for blocking backpressure, these
# are the on_drain hysteresis fires.
sub _h2_transport_state {
    my ($self, $ss) = @_;
    weaken(my $w = $self);
    return PAGI::Server::TransportState->new(
        measure   => sub { $ss->{send_queue_bytes} // 0 },
        high      => sub { $w ? $w->{write_high_watermark} : undef },
        low       => sub { $w ? $w->{write_low_watermark}  : undef },
        arm_drain => sub { my $fire = shift; push @{$ss->{transport_drain_fires}}, $fire },
    );
}

# Notify the current transport-state handle after an application write so its
# backpressure callbacks (on_high_water/on_drain) can fire on a watermark cross.
sub _notify_transport_write {
    my ($self) = @_;
    my $ts = $self->{current_transport_state};
    $ts->_check_watermarks if $ts;
}

sub _check_drain_waiters {
    my ($self) = @_;

    return unless @{$self->{_drain_waiters}} || @{$self->{_drain_fires}};
    return unless $self->{stream};

    my $buffered = $self->_get_write_buffer_size;

    # Resolve/fire everyone once we've drained below low watermark: blocking
    # producer Futures resolve, and arm_drain's on_drain hysteresis callbacks
    # fire -- this IS an actual drain, unlike teardown (_cancel_drain_waiters).
    if ($buffered < $self->{write_low_watermark}) {
        my @waiters = splice @{$self->{_drain_waiters}};
        for my $f (@waiters) {
            $f->done unless $f->is_ready;
        }
        my @fires = splice @{$self->{_drain_fires}};
        $_->() for @fires;
        # Disable drain checking until next high watermark hit
        $self->{_drain_check_active} = 0;
    }
}

sub _setup_drain_detection {
    my ($self) = @_;

    # Avoid redundant setup
    return if $self->{_drain_check_active};
    $self->{_drain_check_active} = 1;

    weaken(my $weak_self = $self);

    # Primary mechanism: check when write queue empties
    # This guarantees we notice drain even for fast-draining connections
    # Store previous handler to chain if needed
    my $prev_on_empty = $self->{_prev_on_outgoing_empty};

    $self->{stream}->configure(
        on_outgoing_empty => sub {
            return unless $weak_self;
            $weak_self->_check_drain_waiters;
            # Call previous handler if any
            $prev_on_empty->(@_) if $prev_on_empty;
        },
    );
}

sub _wait_for_drain {
    my ($self) = @_;

    # Fast path: already below low watermark
    my $buffered = $self->_get_write_buffer_size;
    if ($buffered < $self->{write_low_watermark}) {
        return Future->done;
    }

    # Create Future to be resolved when drained
    my $f = $self->{server}->loop->new_future;
    push @{$self->{_drain_waiters}}, $f;

    # Ensure drain detection is active
    $self->_setup_drain_detection;

    return $f;
}

sub _cancel_drain_waiters {
    my ($self, $reason) = @_;
    $reason //= 'connection closed';

    my @waiters = splice @{$self->{_drain_waiters}};
    for my $f (@waiters) {
        # Resolve (not fail) - app should check connection state after await
        $f->done unless $f->is_ready;
    }
    # Drop (don't fire) the app's on_drain fires: the connection is going
    # away, not draining -- matches h2's teardown handling of
    # transport_drain_fires. The blocking waiters above still resume (so no
    # coroutine leak), but on_drain is a hysteresis signal for a buffer that
    # actually fell back below the low mark, which never happened here.
    $self->{_drain_fires} = [];
    $self->{_drain_check_active} = 0;
}

# HTTP/2 per-stream backpressure: the h2 analogue of _wait_for_drain. Resolves
# when this stream's send queue falls below the low watermark. Each multiplexed
# stream is bounded independently, so a quiet TCP buffer can't let one stream's
# queue grow without limit.
sub _h2_wait_for_stream_drain {
    my ($self, $stream_id) = @_;

    my $ss = $self->{h2_streams}{$stream_id} or return Future->done;

    # Fast path: already below low watermark
    if (($ss->{send_queue_bytes} // 0) < $self->{write_low_watermark}) {
        return Future->done;
    }

    # Create Future to be resolved when this stream's queue drains (in the
    # data_callback pull) or when the stream is torn down.
    my $f = $self->{server}->loop->new_future;
    push @{$ss->{stream_drain_waiters} //= []}, $f;

    return $f;
}

# Release any producer blocked on _h2_wait_for_stream_drain for a stream that
# is being torn down (close/RST/connection shutdown). Resolve, never fail - the
# producer rechecks connection/stream state after the await. Every caller marks
# the stream h2_closed first, so a producer resumed inline here fails that
# recheck and stops without reaching nghttp2.
sub _h2_resolve_stream_drain_waiters {
    my ($self, $ss) = @_;
    return unless $ss && $ss->{stream_drain_waiters};
    $_->done for grep { !$_->is_ready } splice @{$ss->{stream_drain_waiters}};
}

# Release a send() parked in the http.response.trailers arm awaiting
# $deliver_trailer_eof (the data callback's own terminal invocation) for a
# stream that is being torn down before that ever happens. Resolve, never
# fail -- same h2_closed carve-out contract as every other post-close send
# (design §6.2 / §21 item 1): a trailers send racing a disconnect is a
# successful no-op, not an error the app must handle. Resolved inline: the
# parked send() rechecks the connection and this stream's h2_closed the moment
# it wakes, which every caller has already set.
sub _h2_resolve_stream_trailer_wait {
    my ($self, $ss) = @_;
    return unless $ss;
    my $f = delete $ss->{trailer_wait};
    $f->done if $f && !$f->is_ready;
}

# ============================================================================

# WebSocket keepalive - sends protocol-level ping frames (RFC 6455)
sub _start_ws_keepalive {
    my ($self, $interval, $timeout) = @_;

    # Stop existing timers first
    $self->_stop_ws_keepalive;

    return unless $interval && $interval > 0;
    return unless $self->{server};

    $self->{ws_keepalive_interval} = $interval;
    $self->{ws_keepalive_timeout} = $timeout // 0;

    weaken(my $weak_self = $self);

    my $timer = IO::Async::Timer::Periodic->new(
        interval => $interval,
        on_tick  => sub {
            return unless $weak_self;
            return if $weak_self->{closed};
            return unless _ws_handshake_accepted($weak_self->{h1_seq});

            # Send ping frame
            my $ping = Protocol::WebSocket::Frame->new(
                type   => 'ping',
                buffer => '',
            );
            $weak_self->{stream}->write($ping->to_bytes);

            # Start pong timeout if configured
            if ($weak_self->{ws_keepalive_timeout} > 0) {
                $weak_self->{ws_waiting_pong} = 1;
                $weak_self->_start_ws_pong_timeout;
            }
        },
    );

    $self->{ws_keepalive_timer} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _start_ws_pong_timeout {
    my ($self) = @_;

    # Don't start another timeout if one is running
    return if $self->{ws_pong_timeout};
    return unless $self->{ws_keepalive_timeout} > 0;
    return unless $self->{server};

    weaken(my $weak_self = $self);

    my $timer = IO::Async::Timer::Countdown->new(
        delay => $self->{ws_keepalive_timeout},
        on_expire => sub {
            return unless $weak_self;
            return if $weak_self->{closed};

            if ($weak_self->{ws_waiting_pong}) {
                # No pong received within timeout - close connection
                if ($weak_self->{server} && $weak_self->{server}->can('_log')) {
                    $weak_self->{server}->_log(warn =>
                        "WebSocket keepalive timeout - no pong received within $weak_self->{ws_keepalive_timeout}s");
                }
                $weak_self->_handle_disconnect_and_close('keepalive_timeout',
                    detail => "no pong within $weak_self->{ws_keepalive_timeout}s");
            }
        },
    );

    $self->{ws_pong_timeout} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _cancel_ws_pong_timeout {
    my ($self) = @_;

    $self->{ws_waiting_pong} = 0;

    return unless $self->{ws_pong_timeout};
    $self->{ws_pong_timeout}->stop if $self->{ws_pong_timeout}->is_running;
    if ($self->{server}) {
        $self->{server}->remove_child($self->{ws_pong_timeout});
    }
    $self->{ws_pong_timeout} = undef;
}

sub _stop_ws_keepalive {
    my ($self) = @_;

    # Stop pong timeout first
    $self->_cancel_ws_pong_timeout;

    return unless $self->{ws_keepalive_timer};
    $self->{ws_keepalive_timer}->stop if $self->{ws_keepalive_timer}->is_running;
    if ($self->{server}) {
        $self->{server}->remove_child($self->{ws_keepalive_timer});
    }
    $self->{ws_keepalive_timer} = undef;
    $self->{ws_keepalive_interval} = 0;
    $self->{ws_keepalive_timeout} = 0;
}

# SSE keepalive - sends comment lines to prevent proxy timeouts
sub _start_sse_keepalive {
    my ($self, $interval, $comment) = @_;

    # Stop existing timer first
    $self->_stop_sse_keepalive;

    return unless $interval && $interval > 0;
    return unless $self->{server};

    $self->{sse_keepalive_comment} = $comment // '';

    weaken(my $weak_self = $self);

    my $timer = IO::Async::Timer::Periodic->new(
        interval => $interval,
        on_tick  => sub {
            return unless $weak_self;
            return if $weak_self->{closed};

            my $text = $weak_self->{sse_keepalive_comment};
            $text = ":$text" unless $text =~ /^:/;
            my $formatted = "$text\n\n";

            if (my $writer = $weak_self->{sse_keepalive_writer}) {
                $writer->($formatted);
            }
        },
    );

    $self->{sse_keepalive_timer} = $timer;
    $self->{server}->add_child($timer);
    $timer->start;
}

sub _stop_sse_keepalive {
    my ($self) = @_;

    return unless $self->{sse_keepalive_timer};
    $self->{sse_keepalive_timer}->stop if $self->{sse_keepalive_timer}->is_running;
    if ($self->{server}) {
        $self->{server}->remove_child($self->{sse_keepalive_timer});
    }
    $self->{sse_keepalive_timer} = undef;
    $self->{sse_keepalive_comment} = '';
}

sub _try_handle_request {
    my ($self) = @_;

    return if $self->{closed};
    return if $self->{handling_request};

    # An unread request body is consumed here, ahead of everything else: until
    # it is gone the bytes in the buffer are that body, not a request.
    return if $self->_discard_unread_body;

    # Try to parse a request from the buffer
    my ($request, $consumed) = $self->{protocol}->parse_request($self->{buffer});

    return unless $request;

    # Remove consumed bytes from buffer
    substr($self->{buffer}, 0, $consumed) = '';

    # Handle parse errors (malformed request, header too large)
    if ($request->{error}) {
        # Mark connection as disconnected with protocol_error reason (PAGI spec compliance).
        # The parse message going to the client on the next line is also what
        # the operator needs (Www.pod: disconnect_detail).
        $self->_handle_disconnect('protocol_error', $request->{message});
        $self->_send_error_response($request->{error}, $request->{message});
        $self->_close;
        return;
    }

    # Check Content-Length against max_body_size limit (0 = unlimited)
    if ($self->{max_body_size} && defined $request->{content_length}) {
        if ($request->{content_length} > $self->{max_body_size}) {
            $self->_handle_disconnect('body_too_large',
                "declared body of $request->{content_length} bytes"
                    . " exceeded $self->{max_body_size}");
            $self->_send_error_response(413, 'Payload Too Large');
            $self->_close;
            return;
        }
    }

    # Check if this is a WebSocket upgrade request
    my $is_websocket = $self->_is_websocket_upgrade($request);

    # Check if this is an SSE request
    my $is_sse = !$is_websocket && $self->_is_sse_request($request);

    # Handle the request - store the Future to prevent "lost future" warning
    $self->{handling_request} = 1;
    $self->{request_start} = [gettimeofday];
    $self->{current_request} = $request;  # Store for access logging

    # Recorded for anything downstream that needs the scope kind without
    # re-deriving it from the request.
    $self->{scope_kind} = $is_websocket ? 'websocket' : $is_sse ? 'sse' : 'http';

    if ($is_websocket) {
        $self->{request_future} = $self->_handle_websocket_request($request);
    } elsif ($is_sse) {
        $self->{request_future} = $self->_handle_sse_request($request);
    } else {
        # Start stall timer for HTTP requests (WebSocket/SSE have their own handling)
        $self->_start_stall_timer;
        $self->{request_future} = $self->_handle_request($request);
    }

    # Use adopt_future for proper error tracking instead of retain
    # This ensures errors are propagated to the server's error handling
    $self->{server}->adopt_future($self->{request_future});
}

sub _is_websocket_upgrade {
    my ($self, $request) = @_;

    # Check for WebSocket upgrade headers
    my $has_upgrade = 0;
    my $has_connection_upgrade = 0;
    my $has_ws_key = 0;

    for my $header (@{$request->{headers}}) {
        my ($name, $value) = @$header;
        if ($name eq 'upgrade' && lc($value) eq 'websocket') {
            $has_upgrade = 1;
        }
        elsif ($name eq 'connection') {
            # Connection header can have multiple values
            $has_connection_upgrade = 1 if lc($value) =~ /upgrade/;
        }
        elsif ($name eq 'sec-websocket-key') {
            $has_ws_key = 1;
        }
    }

    return $has_upgrade && $has_connection_upgrade && $has_ws_key;
}

sub _is_sse_request {
    my ($self, $request) = @_;

    # SSE detection per spec (see _accept_signals_sse above).
    # Request has not been upgraded to WebSocket (already checked).
    # Note: SSE works with any HTTP method (GET, POST, etc.) to support
    # modern patterns like htmx 4 and datastar using fetch-event-source

    return _accept_signals_sse($request->{headers});
}

async sub _handle_request {
    my ($self, $request) = @_;

    my $scope = $self->_create_scope($request);
    my $receive = $self->_create_receive($request);
    my $send = $self->_create_send($request);

    eval {
        await $self->{app}->($scope, $receive, $send);
    };
    my $error = $@;

    # The boundary HTTP/2's dispatch tail has, for the reason HTTP/1.1 has
    # needed it all along: Future::AsyncAwait runs this sub synchronously only
    # until the application suspends, and every application that does I/O
    # suspends. From there this tail resumes on the event loop's own stack,
    # outside the read handler's eval, and an exception from it failed the
    # adopted {request_future} -- which reaches IO::Async::Notifier::invoke_error
    # and, with no on_error on PAGI::Server, is a bare die out of $loop->run.
    #
    # Caught the read handler's own way ($@ after the eval) rather than with
    # `eval { ...; 1 } or do { ... }`: this tail returns early, and a return
    # inside an eval BLOCK leaves the eval rather than the sub, which would
    # make the or-block fire on every early return with $@ empty.
    eval {
        if ($error) {
            # Delivery defines completion (Www.pod:1156-1165): if the terminal
            # response event already went out, an exception thrown afterward is
            # not a disconnect. Fire on_complete (never on_disconnect), leave
            # disconnect_reason() undef, still warn (SHOULD log), and still
            # close (MAY close) -- mirrors _h2_dispatch_stream's own log-only
            # branch for an error after the response is already complete: that
            # stream's connection_state is likewise marked complete before the
            # exception is even observed there.
            if ($self->{response_started} && (($self->{h1_seq} // '') eq 'complete')) {
                $self->_log(error => "PAGI application error (after response complete): $error");
                $self->_write_access_log;
                $self->{server}->_on_request_complete if $self->{server};
                if (my $conn_state = $self->{current_connection_state}) {
                    $conn_state->_mark_complete;
                }
                $self->_close;
                return;
            }

            # Handle application error - always close connection after exception
            # If response already started, we can't send error page (3.17)
            if ($self->{response_started}) {
                $self->_flush_pending_headers;   # don't lose a started response's headers
                $self->_log(error => "PAGI application error (after response started): $error");
            } else {
                $self->_send_error_response(500, "Internal Server Error");
                $self->_log(error => "PAGI application error: $error");
            }
            # Always close connection after exception (3.2) - don't try keep-alive
            $self->_end_scope('server_error', detail => _detail_from_error($error));
            return;
        }

        # The application returned without starting a response. An incomplete
        # response is a protocol error: if the client is still connected, synthesize
        # a 500; either way do not keep-alive a connection on which no response was
        # written (that would hang the client, which is waiting for a response). A
        # response that was started but not completed is handled by the body-framing
        # and keep-alive logic below.
        if (!$self->{response_started}) {
            unless ($self->{closed}) {
                $self->_log(error => "PAGI application returned without starting a response");
                $self->_send_error_response(500, "Internal Server Error");
            }
            $self->_end_scope('server_error', detail => 'no response was started');
            return;
        }

        # The application resolved with a started but incomplete response
        # (terminal body/file/fh — or promised trailers — never sent). Per the
        # PAGI spec this is an abnormal end: never synthesize the terminal
        # framing, never keep the connection alive, and report server_error
        # through on_disconnect — a truncated response must be observable as
        # truncated.
        if ($self->{response_started} && ($self->{h1_seq} // 'complete') ne 'complete') {
            $self->_flush_pending_headers;   # headers may still be buffered; no terminator follows
            unless ($self->{closed}) {
                warn(($self->{h1_seq} // '') eq 'awaiting_trailers'
                    ? "PAGI application returned with an incomplete response (trailers were declared but never sent)\n"
                    : "PAGI application returned with an incomplete response\n");
            }
            $self->_end_scope('server_error',
                detail => (($self->{h1_seq} // '') eq 'awaiting_trailers')
                    ? 'trailers were declared but never sent'
                    : 'response started but never completed');
            return;
        }

        # Flush any headers buffered by response.start that were never paired with a
        # body write (a started-but-bodyless response).
        $self->_flush_pending_headers;

        # Write access log entry
        $self->_write_access_log;

        # Notify server that request completed (for max_requests tracking)
        $self->{server}->_on_request_complete if $self->{server};

        # Stop stall timer - request completed successfully
        $self->_stop_stall_timer;

        # Request finished cleanly: fire on_complete (not on_disconnect) on the
        # HTTP connection-state object. Must happen on BOTH the keep-alive and
        # close paths, and before the keep-alive branch clears the state below.
        # Once marked complete, the non-keep-alive _handle_disconnect_and_close
        # call below no-ops the state transition, so on_disconnect never fires for
        # a completed request.
        if (my $conn_state = $self->{current_connection_state}) {
            $conn_state->_mark_complete;
        }

        # A request has now completed on this connection: the idle timer's next
        # expiry (if the connection stays open awaiting another request) reports
        # keepalive_timeout rather than idle_timeout.
        $self->{_served_a_request} = 1;

        # Determine if we should keep the connection alive
        my $keep_alive = $self->_should_keep_alive($request);

        if ($keep_alive) {
            # Reset for next request
            $self->{handling_request} = 0;
            $self->{response_started} = 0;
            $self->{h1_seq} = 'initial';
            $self->{_resp_pending} = undef;
            $self->{response_status} = undef;
            $self->{_response_size} = 0;
            $self->{request_start} = undef;
            $self->{current_request} = undef;
            $self->{request_future} = undef;
            $self->{current_connection_state} = undef;  # Clear for next request
            $self->{current_transport_state}  = undef;  # New request gets a fresh handle

            # Whatever the application left unread of the request body is this
            # connection's to discard before it parses anything else.
            $self->_begin_body_discard($request);

            # Check if there's more data in the buffer (pipelining)
            if (length($self->{buffer}) > 0) {
                $self->_try_handle_request;
            }
        } else {
            # Not keeping alive - close connection
            $self->_handle_disconnect_and_close('request_complete');
        }
    };
    $self->_connection_handler_error('HTTP/1.1', $@) if $@;
}

sub _should_keep_alive {
    my ($self, $request) = @_;

    my $http_version = $request->{http_version} // '1.1';

    # Check for Connection header
    my $connection_header;
    for my $header (@{$request->{headers}}) {
        if ($header->[0] eq 'connection') {
            $connection_header = lc($header->[1]);
            last;
        }
    }

    # HTTP/1.1: keep-alive by default unless Connection: close
    if ($http_version eq '1.1') {
        return 0 if $connection_header && $connection_header =~ /close/;
        return 1;
    }

    # HTTP/1.0: close by default unless Connection: keep-alive
    if ($http_version eq '1.0') {
        return 1 if $connection_header && $connection_header =~ /keep-alive/;
        return 0;
    }

    # Unknown version: close connection
    return 0;
}

# What this request still owes, from what the receive path recorded on it:
# { remaining => N }, { chunked => 1, discarded => N }, or nothing at all.
sub _begin_body_discard {
    my ($self, $request) = @_;

    # Content under an expectation the server never answered can neither be
    # read nor waited out: a client that sent Expect: 100-continue MAY send it
    # anyway without a response, and MAY hold it back forever (RFC 9110
    # s10.1.1), so nothing here can tell a body from the next request. The
    # server closes instead, which is the intent RFC 9110 s10.1.1 asks a final
    # response before the whole content to indicate; RFC 9112 s9.3 allows it.
    # A request that declared no content has none of this ambiguity -- there
    # is nothing to hold back or send unbidden -- so the close only applies
    # when the request actually declared a body.
    if ($request->{expect_continue} && !$request->{continue_sent}
        && ($request->{chunked} || ($request->{content_length} // 0) > 0)) {
        $self->_handle_disconnect_and_close('request_complete');
        return;
    }

    if ($request->{chunked}) {
        return if $request->{body_complete};
        $self->{discarding_body} =
            { chunked => 1, discarded => $request->{body_bytes_read} // 0 };
        return;
    }

    my $remaining = ($request->{content_length} // 0)
                  - ($request->{body_bytes_read} // 0);
    $self->{discarding_body} = { remaining => $remaining } if $remaining > 0;
    return;
}

# Consume what has arrived of an owed body, through the same buffer and the same
# chunk parser the receive path reads it with. True while no request may parse.
sub _discard_unread_body {
    my ($self) = @_;

    my $discard = $self->{discarding_body} or return 0;

    if (!$discard->{chunked}) {
        # A declared length over the bound is answered 413 before dispatch.
        my $take = length($self->{buffer});
        $take = $discard->{remaining} if $discard->{remaining} < $take;
        substr($self->{buffer}, 0, $take) = '';
        $discard->{remaining} -= $take;
        return 1 if $discard->{remaining} > 0;
        $self->{discarding_body} = undef;
        return 0;
    }

    my ($data, $consumed, $complete)
        = $self->{protocol}->parse_chunked_body($self->{buffer});

    # Over a complete response there is no exchange left to answer 400 on, and
    # nothing behind unreadable framing can be trusted.
    if (ref($data) eq 'HASH' && $data->{error}) {
        $self->{discarding_body} = undef;
        $self->_handle_disconnect_and_close('protocol_error',
            detail => $data->{message} // 'Bad Request');
        return 1;
    }

    substr($self->{buffer}, 0, $consumed) = '' if $consumed;
    # Counted in wire bytes taken off the buffer, not decoded chunk data:
    # chunk framing and trailers are what the client actually sent, and the
    # bound below must hold on that, not on what survives decoding.
    $discard->{discarded} += $consumed;

    # The bound is max_body_size (0 = unlimited), and crossing it ends the
    # connection, not the scope, which already ended cleanly with its response.
    if ($self->{max_body_size} && $discard->{discarded} > $self->{max_body_size}) {
        $self->{discarding_body} = undef;
        my $detail = "unread request body exceeded max_body_size"
                   . " ($self->{max_body_size} bytes)";
        $self->_log(debug => "HTTP/1.1 connection closed: $detail");
        $self->_handle_disconnect_and_close('body_too_large', detail => $detail);
        return 1;
    }

    return 1 unless $complete;
    $self->{discarding_body} = undef;
    return 0;
}

sub _create_scope {
    my ($self, $request) = @_;

    # Create connection state object for disconnect tracking
    # Uses lazy Future creation - Future only allocated if disconnect_future() is called
    my $connection_state = PAGI::Server::ConnectionState->new(
        connection => $self,
        server     => $self->{server},
        on_abort   => $self->_h1_abort_hook,
    );
    $self->{current_connection_state} = $connection_state;

    my $scope = {
        type         => 'http',
        pagi         => {
            version      => '0.5',
            spec_version => '0.6',
        },
        http_version => $request->{http_version},
        method       => $request->{method},
        scheme       => $self->_get_scheme,
        path         => $request->{path},
        raw_path     => $request->{raw_path},
        query_string => $request->{query_string},
        root_path    => '',
        headers      => $request->{headers},
        (defined $self->{client_host}
            ? (client => [$self->{client_host}, $self->{client_port}])
            : ()
        ),
        server       => [$self->{server_host}, $self->{server_port}],
        # Optimized: avoid hash copy when state is empty (common case)
        state        => keys %{$self->{state}} ? { %{$self->{state}} } : {},
        extensions   => $self->_get_extensions_for_scope,
        # Connection state for non-destructive disconnect detection
        'pagi.connection' => $connection_state,
        # Outbound flow-control introspection (buffered_amount, watermarks,
        # on_high_water/on_drain). Stashed on the connection too, so the send
        # path can poke _check_watermarks after each write.
        'pagi.transport'  => ($self->{current_transport_state} = $self->_h1_transport_state),
    };

    return $scope;
}

# Shared by _create_receive (http.request) and _create_sse_receive
# (sse.request): parses one attempt's worth of a chunked Transfer-Encoding
# request body out of $self->{buffer}, waiting for more bytes if none are
# available yet. Returns the next event for the caller's receive() closure
# to return directly -- a body event (event_type/body/more), the caller's
# disconnect event (via $make_disconnect on a protocol error, an oversized
# body, or the connection closing), or a truthful more=>1 placeholder when
# a receive_pending wake produced no parseable chunk yet.
#
# $body_complete_ref/$bytes_read_ref are scalar refs into the caller's own
# closure-local state so it persists across receive() calls; $event_type
# is 'http.request' or 'sse.request'; $make_disconnect is a coderef taking
# "this call had already parked" and returning a Future of this scope's
# disconnect event, so the caller's max_disconnect_receives cap covers the
# disconnects synthesized in here too (see _disconnect_receive_future).
#
# $clean_end/$make_scope_end are the caller's clean-end predicate and its
# matching answer, in the same shape. An sse scope carries a request body
# (Www.pod's sse.request: POST, htmx, datastar), so the application can end
# that scope with sse.close, or finish a refusal, while a call in here is
# still waiting for the next chunk; Www.pod "Close SSE - send event" is
# unconditional that such a call, "pending or later", resolves with the
# scope's end. Both park sites therefore ask before waiting again. The http
# scope's own ending -- its terminal response event, sent without reading the
# rest of the body -- is the same shape and passes the same pair.
async sub _read_chunked_body {
    my ($self, $event_type, $make_disconnect, $body_complete_ref, $bytes_read_ref,
        $clean_end, $make_scope_end) = @_;

    my $parked = 0;

    # Wait for data if buffer is empty
    while (length($self->{buffer}) == 0 && !$self->{closed}) {
        # The scope ended cleanly under this call. Asked at the top of the
        # loop, so it covers both the wait about to start and the wake that
        # brought no chunk with it.
        return await $make_scope_end->($parked) if $clean_end && $clean_end->();

        if (!$self->{receive_pending}) {
            $self->{receive_pending} = Future->new;
        }
        $parked = 1;
        await $self->{receive_pending};
        $self->{receive_pending} = undef;

        # Check queue after waiting
        if (@{$self->{receive_queue}}) {
            return shift @{$self->{receive_queue}};
        }
    }

    if ($self->{closed} && length($self->{buffer}) == 0) {
        return await $make_disconnect->($parked);
    }

    # Try to parse chunked data
    my ($data, $consumed, $complete) = $self->{protocol}->parse_chunked_body($self->{buffer});

    # Check for parse error (invalid chunk size)
    if (ref($data) eq 'HASH' && $data->{error}) {
        $self->_handle_disconnect('protocol_error', $data->{message} // 'Bad Request');
        $self->_send_error_response($data->{error}, $data->{message} // 'Bad Request');
        $self->_close;
        return await $make_disconnect->($parked);
    }

    if ($consumed > 0) {
        substr($self->{buffer}, 0, $consumed) = '';

        # Track total wire bytes read for max_body_size check -- $consumed is
        # what came off the buffer (chunk framing and trailers included), not
        # the decoded chunk data, so framing overhead is bound the same as
        # payload.
        $$bytes_read_ref += $consumed;

        # Check max_body_size for chunked requests (0 = unlimited)
        if ($self->{max_body_size} && $$bytes_read_ref > $self->{max_body_size}) {
            # The same split as the HTTP/2 guard, asked of this scope's own
            # mirrored send state: a 413 only while no response has started,
            # and past that point the abnormal end plus a close with no
            # terminal framing, which is this transport's truncation (Www.pod
            # "Application Left a Response Incomplete"). _send_error_response
            # already refuses to write over a started response, so the branch
            # is what makes the ending say why rather than what stops the
            # write.
            my $detail;
            if (PAGI::Server::EventValidator::scope_started(
                    $self->{scope_kind} // 'http', $self->{h1_seq})) {
                $detail = _body_limit_after_start($self->{max_body_size});
                $self->_log(error => _body_limit_after_start(
                    $self->{max_body_size}, 'HTTP/1.1'));
            }
            else {
                $detail = "request body exceeded $self->{max_body_size} bytes";
                $self->_send_error_response(413, 'Payload Too Large');
            }
            $self->_handle_disconnect('body_too_large', $detail);
            $self->_close;
            return await $make_disconnect->($parked);
        }

        if ($complete) {
            $$body_complete_ref = 1;
        }

        return {
            type => $event_type,
            body => $data // '',
            more => $complete ? 0 : 1,
        };
    }

    # Need more data - wait for it
    return await $make_scope_end->($parked) if $clean_end && $clean_end->();

    if (!$self->{receive_pending}) {
        $self->{receive_pending} = Future->new;
    }
    $parked = 1;
    await $self->{receive_pending};
    $self->{receive_pending} = undef;

    # Recursive call to re-process - but we can't use __SUB__ in nested async
    # Just return disconnect if closed
    if ($self->{closed}) {
        return await $make_disconnect->($parked);
    }
    # The scope ended cleanly while this call waited for the rest of a chunk:
    # the end is what it reports, rather than the truthful-but-endless more=>1
    # placeholder below.
    return await $make_scope_end->($parked) if $clean_end && $clean_end->();

    # This shouldn't happen often - caller should retry
    return { type => $event_type, body => '', more => 1 };
}

sub _create_receive {
    my ($self, $request) = @_;

    my $content_length = $request->{content_length};
    my $is_chunked = $request->{chunked} // 0;
    my $expect_continue = $request->{expect_continue} // 0;

    # What this closure consumes of the body, and whether it let the client
    # send it, live on the request record rather than in here: the request tail
    # asks the same questions after the application returns, to decide what of
    # the body is still coming (RFC 9112 s9.3). See _begin_body_discard.
    $request->{body_complete}   = 0;
    $request->{body_bytes_read} = 0;
    $request->{continue_sent}   = 0;
    my $chunk_size = 65536;  # 64KB chunks for large bodies

    # For requests without Content-Length and not chunked, treat as no body
    my $has_body = defined($content_length) && $content_length > 0 || $is_chunked;

    weaken(my $weak_self = $self);

    # This scope's cap record and its gate. See _disconnect_receive_future.
    my %cap = (scope => 'http', transport => 'HTTP/1.1', count => 0);
    my $disconnect = sub {
        my ($parked) = @_;
        return Future->done({ type => 'http.disconnect' }) unless $weak_self;
        return $weak_self->_disconnect_receive_future(
            \%cap, { type => 'http.disconnect' }, $parked);
    };

    # This scope's clean end, read off the send machine like the h2 twin.
    # Every site that would otherwise park asks it first.
    my $scope_ended = sub {
        return 0 unless $weak_self;
        return PAGI::Server::EventValidator::scope_send_clean('http', $weak_self->{h1_seq});
    };

    # Return a wrapper that tracks the Future from the async receive
    return sub {
        return Future->done({ type => 'http.disconnect' }) unless $weak_self;
        return $disconnect->() if $weak_self->{closed};

        # The actual async implementation
        my $future = (async sub {
            return { type => 'http.disconnect' } unless $weak_self;

            # True once this call has waited: the answer it then gets is a
            # delivery of the scope's terminal state, not a repeat request
            # for it, so the cap does not count it.
            my $parked = 0;

            if ($weak_self->{closed}) {
                return await $disconnect->($parked);
            }

            # Check queue first - events from disconnect handler
            if (@{$weak_self->{receive_queue}}) {
                return shift @{$weak_self->{receive_queue}};
            }

            # http.disconnect is the answer to a receive after a completed
            # response (Www.pod "Disconnected Client"), so resolve now rather
            # than park until the transport happens to close -- on a keep-alive
            # connection that could be the next request's lifetime away. Asked
            # ahead of the request body, read or not: the scope ended at the
            # terminal response event.
            if ($scope_ended->()) {
                return await $disconnect->($parked);
            }

            # If body is already complete, wait for disconnect
            if ($request->{body_complete}) {
                if (!$weak_self->{receive_pending}) {
                    $weak_self->{receive_pending} = Future->new;
                }

                if ($weak_self->{closed}) {
                    $weak_self->{receive_pending} = undef;
                    return await $disconnect->($parked);
                }

                $parked = 1;
                my $result = await $weak_self->{receive_pending};
                # receive_pending may be completed with a value (disconnect event)
                # or just done() as a signal
                return $result if ref $result eq 'HASH';
                # If no value, check queue
                if (@{$weak_self->{receive_queue}}) {
                    return shift @{$weak_self->{receive_queue}};
                }
                return await $disconnect->($parked);
            }

            # For requests without body, return empty body immediately
            if (!$has_body) {
                $request->{body_complete} = 1;
                return {
                    type => 'http.request',
                    body => '',
                    more => 0,
                };
            }

            # Send 100 Continue if client expects it (before reading body)
            if ($expect_continue && !$request->{continue_sent}) {
                $request->{continue_sent} = 1;
                $weak_self->{stream}->write($weak_self->{protocol}->serialize_continue);
            }

            # Handle chunked Transfer-Encoding
            if ($is_chunked) {
                return await $weak_self->_read_chunked_body(
                    'http.request',
                    $disconnect,
                    \$request->{body_complete},
                    \$request->{body_bytes_read},
                    $scope_ended,
                    $disconnect,
                );
            }

            # Handle Content-Length based body reading
            my $remaining = $content_length - $request->{body_bytes_read};

            if ($remaining <= 0) {
                $request->{body_complete} = 1;
                return {
                    type => 'http.request',
                    body => '',
                    more => 0,
                };
            }

            # Wait for data if buffer is empty
            while (length($weak_self->{buffer}) == 0 && !$weak_self->{closed}) {
                if (!$weak_self->{receive_pending}) {
                    $weak_self->{receive_pending} = Future->new;
                }
                $parked = 1;
                await $weak_self->{receive_pending};
                $weak_self->{receive_pending} = undef;

                # Check queue after waiting
                if (@{$weak_self->{receive_queue}}) {
                    return shift @{$weak_self->{receive_queue}};
                }

                # The scope ended under this call. Same rule, same order, as
                # the head of this closure.
                return await $disconnect->($parked) if $scope_ended->();
            }

            # Return disconnect if closed while waiting
            if ($weak_self->{closed} && length($weak_self->{buffer}) == 0) {
                return await $disconnect->($parked);
            }

            # Read up to chunk_size or remaining bytes, whichever is smaller
            my $to_read = $remaining < $chunk_size ? $remaining : $chunk_size;
            $to_read = length($weak_self->{buffer}) if length($weak_self->{buffer}) < $to_read;

            my $body = substr($weak_self->{buffer}, 0, $to_read, '');
            $request->{body_bytes_read} += length($body);

            # Check if we've read all the body
            my $more = ($request->{body_bytes_read} < $content_length) ? 1 : 0;

            if (!$more) {
                $request->{body_complete} = 1;
            }

            return {
                type => 'http.request',
                body => $body,
                more => $more,
            };
        })->();

        # Track this Future so we can cancel it on close
        push @{$weak_self->{receive_futures}}, $future;

        # Clean up completed futures from the list
        @{$weak_self->{receive_futures}} = grep { !$_->is_ready } @{$weak_self->{receive_futures}};

        return $future;
    };
}

sub _create_send {
    my ($self, $request, %opt) = @_;

    my $chunked = 0;
    my $expects_trailers = 0;
    my $seq = 'initial';
    # Refusing a websocket handshake or an sse stream is an ordinary HTTP
    # response and delegates its wire work here, so that on the wire it is
    # identical to the same response on an http scope (Www.pod "Refusing the
    # handshake" / "Refusing the stream"). A refusal closes the connection
    # and carries no keep-alive.
    my $is_refusal = $opt{refusal};
    my $is_head_request = ($request->{method} // '') eq 'HEAD';
    my $http_version = $request->{http_version} // '1.1';
    my $is_http10 = ($http_version eq '1.0');

    # Check if HTTP/1.0 client requested keep-alive
    my $client_wants_keepalive = 0;
    if ($is_http10) {
        for my $h (@{$request->{headers}}) {
            if ($h->[0] eq 'connection' && lc($h->[1]) =~ /keep-alive/) {
                $client_wants_keepalive = 1;
                last;
            }
        }
    }

    weaken(my $weak_self = $self);

    # Publish the closure-local $seq where the scope's owner reads it, so the
    # app-return path can tell a completed response from one the app left
    # incomplete: the connection for an http scope, and for a refusal the
    # refusing scope's own send closure, which passes its own publisher.
    my $publish = $opt{on_state}
        // sub { $weak_self->{h1_seq} = $_[0] if $weak_self };
    $publish->($seq);

    my $send_event = async sub  {
        my ($event) = @_;
        return Future->done unless $weak_self;
        return Future->done if $weak_self->{closed};

        # Reset stall timer on write activity
        $weak_self->_reset_stall_timer;

        my $type = $event->{type} // '';

        # Mandatory event validation and sequencing (PAGI spec compliance).
        # Order per spec: transport-closed no-op check above runs first.
        PAGI::Server::EventValidator::validate_http_send(
            $event, { extensions => $weak_self->{extensions} });
        my $seq_before_advance = $seq;
        $seq = PAGI::Server::EventValidator::advance_http($seq, $event);
        $publish->($seq);

        if ($type eq 'http.response.start') {
            $weak_self->{response_started} = 1;
            $weak_self->{current_connection_state}->_mark_response_started
                if $weak_self->{current_connection_state};
            $weak_self->{response_status} = $event->{status} // 200;  # Track for logging
            $expects_trailers = $event->{trailers} // 0;

            my $status = $event->{status} // 200;
            my $headers = $event->{headers} // [];
            # PAGI spec — HTTP/1.1 owns Transfer-Encoding and Connection;
            # strip any app-supplied values before they reach the wire.
            $headers = $weak_self->_h1_strip_connection_headers($headers);

            # Check if we need chunked encoding (no Content-Length)
            my $has_content_length = 0;
            for my $h (@$headers) {
                if (lc($h->[0]) eq 'content-length') {
                    $has_content_length = 1;
                    last;
                }
            }

            # Add Date header, but only if the app didn't already supply one.
            my @final_headers = @$headers;
            unless (grep { lc($_->[0]) eq 'date' } @final_headers) {
                push @final_headers, ['date', $weak_self->{protocol}->format_date];
            }

            # PAGI spec Upgrade companion rule: over HTTP/1.1, a response
            # carrying an app-supplied Upgrade header (e.g. 426 Upgrade
            # Required) must also carry 'upgrade' among the server-supplied
            # Connection tokens -- RFC 9110 requires the pair from any
            # Upgrade sender, and the app's own Connection header was
            # stripped above. HTTP/1.0 has no upgrade mechanism.
            if (!$is_http10 && grep { lc($_->[0]) eq 'upgrade' } @final_headers) {
                push @final_headers, ['connection', 'upgrade'];
            }

            # For HEAD requests, don't use chunked encoding (no body will be sent)
            # For HTTP/1.0, don't use chunked encoding - use Connection: close instead
            if ($is_head_request || $is_http10) {
                $chunked = 0;
                if ($is_http10) {
                    if (!$has_content_length) {
                        # No Content-Length means we can't do keep-alive
                        push @final_headers, ['connection', 'close'];
                    } elsif ($client_wants_keepalive && !$is_refusal) {
                        # HTTP/1.0 client requested keep-alive and we can honor it
                        # Must explicitly acknowledge with Connection: keep-alive
                        push @final_headers, ['connection', 'keep-alive'];
                    }
                }
            } else {
                $chunked = !$has_content_length;
            }

            # Www.pod "Refusing the handshake"/"Refusing the stream": on
            # HTTP/1.1 the server closes the connection after a refusal and
            # the response carries Connection: close, so a pooling client does
            # not reuse the socket. Appended rather than replacing, because a
            # refusal that answers with 426 Upgrade Required already carries
            # the server-supplied 'upgrade' token above and needs both.
            if ($is_refusal && !grep { lc($_->[0]) eq 'connection' && lc($_->[1]) eq 'close' } @final_headers) {
                push @final_headers, ['connection', 'close'];
            }

            my $response = $weak_self->{protocol}->serialize_response_start(
                $status, \@final_headers, $chunked, $http_version
            );

            # Buffer the headers instead of writing them now; they are flushed
            # together with the first body write (or at finalization). This
            # coalesces the common "start + complete body" case into a single
            # stream write instead of one per headers/chunk/terminator.
            $weak_self->{_resp_pending} = $response;
        }
        elsif ($type eq 'http.response.body') {
            # For HEAD requests, suppress the body
            if ($is_head_request) {
                # HEAD has headers but no body, so flush the buffered headers now.
                $weak_self->_flush_pending_headers;
                return;  # Don't send any body for HEAD
            }

            # --- BACKPRESSURE CHECK ---
            # Wait for buffer to drain if we're above high watermark
            # This prevents unbounded memory growth with slow clients
            if ($weak_self->_get_write_buffer_size >= $weak_self->{write_high_watermark}) {
                await $weak_self->_wait_for_drain;
                # Re-check connection state after await
                return Future->done unless $weak_self;
                return Future->done if $weak_self->{closed};
            }
            # --- END BACKPRESSURE CHECK ---

            # Determine body source: body, file, or fh (mutually exclusive)
            my $body = $event->{body};
            my $file = $event->{file};
            my $fh = $event->{fh};
            my $offset = $event->{offset} // 0;
            my $length = $event->{length};

            if (defined $file) {
                # File path response - stream from file (async, non-blocking)
                # File responses are implicitly complete (more is ignored) on
                # success. A failed file/fh send must NOT mark the response
                # complete: advance_http already advanced $seq to a terminal
                # state before we got here (it can't know the read will fail),
                # so on failure we roll $seq back to its pre-event value. That
                # lets a conforming app recover with a normal error body
                # instead of being permanently locked out by "response already
                # complete" for a body that was never actually sent.
                $weak_self->_flush_pending_headers;   # headers before the file body
                eval {
                    # h2 parity (Connection.pm _h2_create_send file arm): fail
                    # fast on a missing/unreadable file with the same messages,
                    # ahead of _send_file_response's own -s/open, which would
                    # otherwise report a less specific error for the same fault.
                    die "File not found: $file\n"  unless -f $file;
                    die "Cannot read file: $file\n" unless -r $file;

                    await $weak_self->_send_file_response($file, $offset, $length, $chunked);
                    1;
                } or do {
                    my $error = $@;
                    $seq = $seq_before_advance;
                    $publish->($seq);
                    die $error;
                };
            }
            elsif (defined $fh) {
                # Filehandle response - stream from handle (async, non-blocking)
                # Filehandle responses are implicitly complete (more is ignored)
                # on success; see the file-path comment above for why a failed
                # send must roll $seq back instead of leaving it 'complete'.
                $weak_self->_flush_pending_headers;   # headers before the fh body
                eval {
                    await $weak_self->_send_fh_response($fh, $offset, $length, $chunked);
                    1;
                } or do {
                    my $error = $@;
                    $seq = $seq_before_advance;
                    $publish->($seq);
                    die $error;
                };
            }
            else {
                # Traditional body response
                $body //= '';
                my $more = $event->{more} // 0;

                $weak_self->{_response_size} += length($body);

                # Coalesce any buffered headers, the body (chunk framing if
                # chunked), and the final terminator into a single stream write.
                # The common start + complete-body response becomes one write
                # rather than three.
                my $out = $weak_self->{_resp_pending};
                $out = '' unless defined $out;
                $weak_self->{_resp_pending} = undef;

                if ($chunked) {
                    if (length $body) {
                        my $len = sprintf("%x", length($body));
                        $out .= "$len\r\n$body\r\n";
                    }
                    if (!$more && !$expects_trailers) {
                        $out .= "0\r\n\r\n";
                    }
                }
                else {
                    $out .= $body;
                }

                $weak_self->{stream}->write($out) if length $out;
                $weak_self->_notify_transport_write;
            }
        }
        elsif ($type eq 'http.response.trailers') {
            # No "return unless $expects_trailers" guard here: advance_http
            # (called unconditionally above, line ~2886) already croaks for
            # undeclared trailers -- "cannot send http.response.trailers:
            # trailers were not declared or body is not complete" -- before
            # execution ever reaches this branch, so the guard was dead code.

            if ($is_head_request) {
                # HEAD: accept-and-discard, per PAGI Www.pod's HEAD rule
                # (mirrors the h2 HEAD block from Phase 2 Task 1). The
                # generic advance_http call above already advanced the
                # machine; transmit nothing.
                return;
            }

            unless ($chunked) {
                # Trailers ride chunked framing only (RFC 7230); a
                # content-length response has no place to put them, and
                # silently dropping promised trailers lies to the
                # application. advance_http already advanced $seq to
                # 'complete' generically above (it can't know the framing
                # can't carry trailers) -- roll it back to its pre-event
                # value, same guard/hoist pattern used by the file/fh arms
                # above, so the machine stays 'awaiting_trailers' and never
                # claims a response completed that never actually went out.
                # advance_http is a pure function with no side effects beyond
                # its return value, which is what makes advance-then-rollback
                # safe.
                $seq = $seq_before_advance;
                $publish->($seq);
                die "http.response.trailers requires chunked framing (response declared content-length)\n";
            }

            # RFC 9110 section 6.5.1 additionally forbids connection-specific/
            # framing fields in trailers outright, on any HTTP version -- not
            # just the PAGI spec's h1 response-header rule this strip
            # otherwise exists for. Strip at ingestion, same placement as
            # every h1 response-header site above.
            my $trailer_headers = $weak_self->_h1_strip_connection_headers($event->{headers} // []);

            # Send final chunk + trailers (prepend any still-buffered headers).
            my $trailers = $weak_self->{_resp_pending} // '';
            $weak_self->{_resp_pending} = undef;
            $trailers .= "0\r\n";

            my @validated_trailers;
            for my $header (@$trailer_headers) {
                my ($name, $value) = @$header;
                $name  = _validate_header_name($name);
                $value = _validate_header_value($value);
                push @validated_trailers, [$name, $value];
            }
            $trailers .= $weak_self->{protocol}->serialize_trailers(\@validated_trailers);

            $weak_self->{stream}->write($trailers);
        }
        elsif ($type eq 'http.fullflush') {
            # Fullflush extension - force immediate TCP buffer flush.
            # validate_http_send (called above, unconditionally) already
            # croaks "Extension not enabled: fullflush" when the extension
            # isn't advertised, so no re-check is needed here.

            # Force flush by ensuring TCP_NODELAY and flushing any pending writes
            my $handle = $weak_self->{stream}->write_handle;
            if ($handle && $handle->can('setsockopt')) {
                # Ensure TCP_NODELAY is set to disable Nagle buffering
                require Socket;
                $handle->setsockopt(Socket::IPPROTO_TCP(), Socket::TCP_NODELAY(), 1);
            }

            # In IO::Async, writes are queued and sent when the event loop allows.
            # The above TCP_NODELAY ensures no Nagle buffering delays.
            # For this reference implementation, we return immediately as the
            # write buffer will be flushed by the event loop.
        }

        return;
    };

    # The HTTP/1.1 twin of _h2_create_send's terminal-event site, and the same
    # one site for all of them. The scope ends at the server's last output,
    # request body read or not; what is left of an unread request body is the
    # transport's business, and the request tail still decides it.
    return sub {
        my ($event) = @_;
        return $send_event->($event)->on_done(sub {
            $weak_self->_h1_end_scope_output
                if $weak_self
                && PAGI::Server::EventValidator::scope_send_clean('http', $seq);
        });
    };
}

# Flush any response headers buffered by http.response.start that were not yet
# paired with a body write (HEAD/file/fh paths, started-but-incomplete responses).
sub _flush_pending_headers {
    my ($self) = @_;
    my $pending = $self->{_resp_pending};
    return unless defined $pending && length $pending;
    $self->{_resp_pending} = undef;
    $self->{stream}->write($pending);
}

sub _send_error_response {
    my ($self, $status, $message) = @_;

    return if $self->{closed};
    return if $self->{response_started};

    my $body = $message;
    my $headers = [
        ['content-type', 'text/plain'],
        ['content-length', length($body)],
        ['date', $self->{protocol}->format_date],
    ];

    my $response = $self->{protocol}->serialize_response_start($status, $headers, 0);
    $response .= $body;

    $self->{stream}->write($response);
    $self->{response_started} = 1;
    # A server-synthesized response is still "this request's response started".
    $self->{current_connection_state}->_mark_response_started
        if $self->{current_connection_state};
    $self->{response_status} = $status;  # Track for logging
}

sub _write_access_log {
    my ($self) = @_;

    return unless $self->{access_log};
    return unless $self->{current_request};

    my $request = $self->{current_request};

    # Calculate request duration
    my $duration = 0;
    if ($self->{request_start}) {
        $duration = tv_interval($self->{request_start});
    }

    # Per-second cached CLF timestamp
    my $now = time();
    if ($now != $_cached_log_time) {
        $_cached_log_time = $now;
        my @gmt = gmtime($now);
        my @months = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
        $_cached_log_timestamp = sprintf("%02d/%s/%04d:%02d:%02d:%02d +0000",
            $gmt[3], $months[$gmt[4]], $gmt[5] + 1900,
            $gmt[2], $gmt[1], $gmt[0]);
    }

    my $info = {
        client_ip       => $self->{client_host} // ($self->{transport_type} eq 'unix' ? 'unix' : '-'),
        timestamp       => $_cached_log_timestamp,
        method          => $request->{method} // '-',
        path            => $request->{raw_path} // '/',
        query           => $request->{query_string},
        http_version    => $request->{http_version} // '1.1',
        status          => $self->{response_status} // '-',
        size            => $self->{_response_size} // 0,
        duration        => $duration,
        request_headers => $request->{headers} // [],
    };

    my $formatter = $self->{_access_log_formatter};
    if ($formatter) {
        print {$self->{access_log}} $formatter->($info), "\n";
    }
    else {
        # Fallback (should not happen with properly initialized server)
        my $path = $info->{path};
        my $query = $info->{query};
        $path .= "?$query" if defined $query && length $query;
        print {$self->{access_log}} "$info->{client_ip} - - [$info->{timestamp}] \"$info->{method} $path\" $info->{status} $info->{duration}s\n";
    }
}

# Reasons passed to _handle_disconnect only for teardown after a clean finish
# (the app has already returned). They are completions, not abnormal disconnects,
# and must not surface as a disconnect reason to the application.
my %COMPLETION_REASON = map { ($_ => 1) } qw(
    request_complete
    stream_complete
    session_complete
);

# Build the app-facing websocket.disconnect event for a server-detected close.
# The code and reason come from the close the server initiated; the defaults are
# the RFC 6455 "abnormal closure, no status received" pair (1006 / empty), used
# when the connection dropped with no close handshake (timeout, TCP FIN).
sub _ws_disconnect_event {
    my ($self) = @_;

    # This scope has already delivered its disconnect, so a further receive()
    # resolves with that same event rather than with a fresh reading of the
    # ending record (Www.pod "Disconnect - receive event"). The record cannot
    # stand in for it: a peer's Close frame names its own RFC code and its own
    # reason TEXT, while the record's vocabulary is the standard reason tokens
    # the connection object reports. A copy goes out, so an application that
    # edits the event it received cannot alter what a later reader sees.
    return { %{ $self->{ws_disconnect_event} } } if $self->{ws_disconnect_event};

    return {
        type   => 'websocket.disconnect',
        # A server-reported abnormal end names itself: 'client_closed' is the
        # token for the bare transport drop that leaves no other reason
        # behind (Www.pod "Disconnect - receive event": abnormal drop with no
        # close handshake).
        code   => $self->_end_code($self),
        reason => $self->_end_reason($self),
    };
}

# The disconnect receive event this scope must deliver, or undef when its
# ending delivers none.
#
# The scope kind picks the event type, not the accept flag: a websocket or
# sse scope that ends before it was established still delivers its own
# scope's event (Www.pod "Disconnect - receive event", "before as well as
# after websocket.accept"; "SSE Disconnect - receive event", "whether or not
# sse.start has been sent"). A normally completed refusal is a clean end and
# delivers no event at all (Www.pod "Meaning per scope", Agreement with
# disconnect events).
sub _scope_disconnect_event {
    my ($self) = @_;

    my $kind = $self->{scope_kind} // 'http';
    my $seq  = $self->{h1_seq} // '';

    if ($kind eq 'websocket') {
        return undef if $seq eq 'refusal_complete';
        return $self->_ws_disconnect_event;
    }
    if ($kind eq 'sse') {
        return undef if $seq eq 'refusal_complete';
        return {
            type   => 'sse.disconnect',
            reason => $self->_end_reason($self),
        };
    }
    return { type => 'http.disconnect' };
}

# One scope's cap on receives answered with a SYNTHESIZED end-of-scope event
# (PAGI::Server max_disconnect_receives), whether the scope ended abnormally
# or cleanly. Www.pod "Receiving after the scope's end" reports the end to
# every later receive(), so an application that never checks for it loops on
# already-resolved Futures, the event loop never turns, and every other
# connection in the process is starved. The spec lets a server bound those
# re-deliveries; this is that bound, and it ends the loop by failing the call
# instead. 0 restores unlimited re-delivery.
#
# $cap is the receive closure's own per-scope record -- scope and transport
# for the log line, plus the running count, which never resets. $event is the
# event or a coderef producing it. $parked says the call had already been
# waiting when the scope ended: such a call is an ordinary delivery of the
# scope's terminal state, not a repeat request for it, so it is answered
# without counting. The scope's single queued disconnect event never reaches
# here at all -- a receive that shifts it off the queue returns it directly.
#
# Each caller tests its own weak connection reference before calling: a
# receive() the application kept past the connection object's collection is
# answered with the event, uncounted, because there is no connection left to
# starve.
#
# Returns the Future the receive() closure hands back: done with the event
# while the cap allows it, failed once it does not, with one error line per
# scope naming the scope, the transport and the count.
sub _disconnect_receive_future {
    my ($self, $cap, $event, $parked) = @_;

    my $max = $self->{max_disconnect_receives} // 0;
    my $n   = ($max && !$parked) ? ++$cap->{count} : 0;

    return Future->done(ref $event eq 'CODE' ? $event->() : $event)
        if !$max || $parked || $n <= $max;

    # "after the scope ended", not "after the scope's disconnect event": a
    # clean end the application produced delivers no disconnect event at all,
    # and an operator who hits this line after sse.close must not be sent
    # looking for one.
    my $message = "receive() called $n times after the scope ended; "
                . "the application is not checking for it "
                . "(PAGI::Server max_disconnect_receives=$max)";

    $self->_log(error => "$cap->{scope} scope on $cap->{transport}: $message")
        unless $cap->{logged}++;

    return Future->fail("$message\n");
}

# Settle this connection's scope: record how it ended, mark every object it
# owns, release parked I/O, and deliver the scope's disconnect event.
# $detail is the human-readable supplement to $reason, supplied by whichever
# site detected the end (Www.pod: disconnect_detail); it describes the one
# event, so an h2 connection-level teardown hands the same words to every
# stream it ends.
sub _handle_disconnect {
    my ($self, $reason, $detail) = @_;

    # Idempotency guard - prevent duplicate disconnect handling
    # Multiple paths can trigger disconnect (timeout, protocol error, session end)
    return if $self->{_disconnect_handled};
    $self->{_disconnect_handled} = 1;

    # Auto-detect server shutdown (PAGI spec compliance)
    # If no explicit reason and server is shutting down, use server_shutdown
    if (!$reason && $self->{server} && $self->{server}{shutting_down}) {
        $reason = 'server_shutdown';
    }

    # Default reason is client_closed (TCP FIN received)
    $reason //= 'client_closed';

    # A clean completion is not an abnormal disconnect: don't surface its reason.
    my $is_completion = $COMPLETION_REASON{$reason};

    unless ($is_completion) {
        # Why this whole connection is ending, for the sites that settle
        # parked I/O after the objects have been marked. _close's h2 sweep
        # and the disconnect event below read the record, so they name the
        # same token the objects carry (Www.pod "Agreement with disconnect
        # events").
        $self->_record_end($self, reason => $reason, detail => $detail);

        # Mark this scope's connection_state as disconnected (abnormal only).
        # Applies uniformly to http, websocket, and sse scopes: every scope's
        # pagi.connection attaches at scope creation (Www.pod "Connection State").
        if ($self->{current_connection_state}) {
            $self->{current_connection_state}->_mark_disconnected(
                $self->_end_reason($self), $self->_end_detail($self));
        }

        # HTTP/2: connection-level teardown (server shutdown, socket error, ...)
        # records this reason on every open stream and marks its own
        # connection_state, so a stream still mid-response when the whole
        # connection dies still reports why. First-wins: a stream that already
        # named its own end (idle timeout, protocol close, ...) keeps that
        # reason, and its object and its disconnect event still agree.
        # _mark_disconnected is idempotent, so a stream _h2_on_close already
        # took to a terminal state keeps that first mark. http, websocket, and
        # sse streams all attach a connection_state, so the guard on
        # $stream->{connection_state} is defensive only.
        if ($self->{is_h2} && $self->{h2_streams}) {
            for my $stream (values %{$self->{h2_streams}}) {
                $self->_record_end($stream, reason => $reason, detail => $detail);
                $stream->{connection_state}->_mark_disconnected(
                    $self->_end_reason($stream), $self->_end_detail($stream))
                    if $stream->{connection_state};
            }
        }
    }

    # Cancel any pending drain waiters (backpressure) AFTER connection state
    # is marked above: resolving a parked waiter can synchronously resume an
    # awaiting app coroutine (Future::AsyncAwait resumes inline off ->done),
    # and that resumed app's first act may be to read is_connected() /
    # disconnect_reason() -- those must already reflect this disconnect, not
    # a stale "still connected" snapshot from before it was detected.
    $self->_cancel_drain_waiters($reason);

    my $disconnect_event = $self->_scope_disconnect_event;

    # A websocket scope keeps its one disconnect as well as queuing it, so a
    # receive() made after the queued copy was drained is answered with the
    # very event that was delivered (see _ws_disconnect_event).
    $self->{ws_disconnect_event} = { %$disconnect_event }
        if $disconnect_event && $disconnect_event->{type} eq 'websocket.disconnect';

    # Queue disconnect event (do this even if already closed)
    push @{$self->{receive_queue}}, $disconnect_event if $disconnect_event;

    # Complete any pending receive
    if ($disconnect_event && $self->{receive_pending} && !$self->{receive_pending}->is_ready) {
        $self->{receive_pending}->done($disconnect_event);
        $self->{receive_pending} = undef;
    }
}

# Send a WebSocket close frame with status code and optional reason
# Per RFC 6455 Section 7.4, common codes:
#   1000 - Normal closure
#   1007 - Invalid frame payload data (e.g., invalid UTF-8)
#   1009 - Message too big
#   1011 - Unexpected condition
sub _send_close_frame {
    my ($self, $code, $reason) = @_;
    $reason //= '';

    return unless $self->{stream};
    return if $self->{close_sent};

    # Remember the wire code so the app-facing websocket.disconnect event reports
    # the same code the peer received, rather than the 1006 abnormal-close default.
    $self->_record_end($self, code => $code);

    my $frame = Protocol::WebSocket::Frame->new(
        type   => 'close',
        buffer => pack('n', $code) . $reason,
    );

    $self->{stream}->write($frame->to_bytes);
    $self->{close_sent} = 1;
}

# Tell an HTTP/2 peer that this session is shutting down: GOAWAY naming the
# highest stream taken up, queued and flushed. True once it is on its way out;
# an announcement that could not be queued or flushed leaves the session
# running in nghttp2, so a caller that is ending the connection still needs the
# ordinary ending. Called twice on a connection the drain sweep announced to
# and then closed -- RFC 9113 section 6.8 permits a repeated GOAWAY as long as
# the last stream id does not rise, and nghttp2 sends the lower of the two.
sub _h2_announce_shutdown {
    my ($self) = @_;
    return 0 unless $self->{h2_session};
    return eval {
        $self->{h2_session}->graceful_shutdown;
        $self->_h2_write_pending;
        1;
    } ? 1 : 0;
}

sub _close {
    my ($self, %opt) = @_;

    # Idempotency guard for cleanup, kept separate from the "closed" flag
    # itself: _handle_disconnect_and_close may already have set {closed} = 1
    # before calling here, so the send-side closed-check sees it early.
    return if $self->{_cleanup_done};
    $self->{_cleanup_done} = 1;
    $self->{closed} = 1;

    # Cancel pending drain waiters early (before other cleanup)
    $self->_cancel_drain_waiters('connection closing');

    # Clean up HTTP/2 per-stream state
    if ($self->{h2_streams}) {
        for my $stream (values %{$self->{h2_streams}}) {
            # Whole connection is going away -- stop every WS/SSE stream's
            # keepalive (and SSE idle) timers so none leak past this teardown
            # sweep.
            $self->_h2_stop_ws_keepalive($stream) if $stream->{is_websocket};
            if ($stream->{is_sse}) {
                $self->_h2_stop_sse_keepalive($stream);
                $self->_h2_stop_sse_idle_timer($stream);
            }

            $stream->{h2_closed} = 1;   # liveness for the dispatch wrapper
            if ($stream->{body_pending} && !$stream->{body_pending}->is_ready
                && !_h2_refusal_complete($stream)) {
                # Www.pod "Agreement with disconnect events": this event's
                # reason MUST match the token the stream's connection_state
                # was (or will be) marked with.
                my $reason = $self->_end_reason($stream);
                my $event = $stream->{is_sse}       ? { type => 'sse.disconnect', reason => $reason }
                          : $stream->{is_websocket} ? { type => 'websocket.disconnect',
                                                        code => $self->_end_code($stream), reason => $reason }
                          :                           { type => 'http.disconnect' };
                $stream->{body_pending}->done($event);
            }
            # Release producers blocked on per-stream backpressure so they
            # don't hang on a connection that is going away.
            $self->_h2_resolve_stream_drain_waiters($stream);
            # Same for a send() parked in the deferred trailers branch --
            # without this, a peer that just drops the TCP connection (FIN,
            # idle timeout, shutdown) never fires on_stream_close (no h2
            # protocol event at all), so _h2_on_close's own release never
            # runs, and the parked send hangs forever.
            $self->_h2_resolve_stream_trailer_wait($stream);
            # Drop (don't fire) the app's on_drain fires: the connection is going
            # away, not draining. Also break the $stream <-> transport_state cycle
            # so the stream state is freed when h2_streams is deleted below.
            $stream->{transport_drain_fires} = [];
            delete $stream->{transport_state};
        }
        delete $self->{h2_streams};
    }
    if ($self->{h2_session}) {
        # A shutdown is the one ending the peer can still act on: RFC 9113
        # section 6.8 has it read GOAWAY's last stream id to learn which of
        # its requests this server never took up, and may retry elsewhere.
        # terminate_session puts nothing on the wire. Every other ending keeps
        # it -- a protocol error nghttp2 already answered with its own GOAWAY,
        # a socket that has gone away.
        my $announced = 0;
        if ($self->_end_reason($self, '') eq 'server_shutdown') {
            $announced = $self->_h2_announce_shutdown;
        }
        # An announcement that could not be queued or flushed leaves the
        # session running in nghttp2, so it still needs the ordinary ending.
        eval { $self->{h2_session}->terminate(0) } unless $announced;
        delete $self->{h2_session};
    }

    # Clean up WebSocket frame parser to free memory immediately
    delete $self->{websocket_frame};

    # Remove from server's connection list (O(1) hash delete)
    if ($self->{server}) {
        delete $self->{server}{connections}{refaddr($self)};

        # Signal drain complete if this was the last connection during shutdown
        if ($self->{server}{shutting_down} &&
            keys %{$self->{server}{connections}} == 0 &&
            $self->{server}{drain_complete} &&
            !$self->{server}{drain_complete}->is_ready) {
            $self->{server}{drain_complete}->done;
        }
    }

    # Stop idle timer
    $self->_stop_idle_timer;

    # Stop stall timer
    $self->_stop_stall_timer;

    # Stop WS/SSE idle timers
    $self->_stop_ws_idle_timer;
    $self->_stop_sse_idle_timer;

    # Stop keepalive timers
    $self->_stop_ws_keepalive;
    $self->_stop_sse_keepalive;

    # Note: _close is resource cleanup ONLY. Callers should use
    # _handle_disconnect_and_close() which handles both protocol
    # notification and cleanup.

    my $disconnect_event = $self->_scope_disconnect_event;

    # Cancel any tracked receive Futures that are still pending
    if ($disconnect_event) {
        for my $future (@{$self->{receive_futures}}) {
            if (!$future->is_ready) {
                # Complete with disconnect event instead of cancelling
                # This allows the async sub to complete cleanly
                $future->done($disconnect_event);
            }
        }
    }
    $self->{receive_futures} = [];

    if ($self->{stream}) {
        # abort() MUST NOT wait for an in-flight write to drain, since that
        # write may be blocked by the peer (Www.pod "Connection Object
        # Interface"): close_when_empty does wait, so an app_abort teardown
        # asks for close_now instead. Every other reason keeps
        # close_when_empty (flush what's already queued, then close).
        #
        # A scope that ended with app_abort closes now however this teardown
        # was reached, which the option alone cannot guarantee: settling the
        # abort resolves the parked send, which resumes the application
        # inline, which returns into the incomplete-response tail, which
        # calls in here first with no option of its own. _cleanup_done then
        # makes the abort's own call a no-op, so the option it carried never
        # runs. The ending record is the durable fact that survives that
        # re-entrancy, and exactly one site writes app_abort into it.
        if ($opt{close_now} || $self->_end_reason($self) eq 'app_abort') {
            $self->{stream}->close_now;
        } else {
            $self->{stream}->close_when_empty;
        }
    }
}

# Combined disconnect and close - use this from callbacks where $weak_self may
# become undefined after _handle_disconnect completes its Future callbacks.
# This method holds a strong reference to $self throughout the operation.
sub _handle_disconnect_and_close {
    my ($self, $reason, %opt) = @_;

    # Mark the transport closed before notifying: _handle_disconnect below
    # completes any pending receive(), which can synchronously resume the
    # app coroutine (it may run straight through a subsequent send()), so
    # the send-side closed-check needs "closed" to already be true at that
    # point (spec order: closed-check precedes validation). Resource
    # cleanup itself still happens in _close, gated by its own idempotency
    # flag so it isn't skipped by this early flip.
    $self->{closed} = 1;

    $self->_handle_disconnect($reason, $opt{detail});
    $self->_close(close_now => $opt{close_now});
}

# End the current h1 scope: access log, request-complete accounting, then the
# transport transition. $reason is a standard token or a completion reason
# (session_complete, stream_complete, request_complete).
sub _end_scope {
    my ($self, $reason, %opt) = @_;

    $self->_write_access_log;
    $self->{server}->_on_request_complete if $self->{server};
    $self->_handle_disconnect_and_close($reason, %opt);
    return;
}

# Teardown hook handed to every h1 ConnectionState: the object has already
# marked itself app_abort when this runs. Close the transport without waiting
# for any in-flight write to drain -- that write may be blocked by the peer
# (Www.pod "Connection Object Interface"), so this teardown asks for close_now
# instead of the default close_when_empty. _handle_disconnect settles pending
# I/O and, because the object is already terminal, its own _mark_disconnected
# is a no-op.
sub _h1_abort_hook {
    my ($self) = @_;
    weaken(my $weak_self = $self);
    return sub {
        my ($cs, $detail) = @_;
        return unless $weak_self && !$weak_self->{closed};
        $weak_self->_handle_disconnect_and_close('app_abort',
            detail => $detail, close_now => 1);
    };
}

#
# TLS Support Methods
#

sub _extract_tls_info {
    my ($self) = @_;

    my $stream = $self->{stream};
    my $handle = $stream->read_handle;

    # Check if handle is an IO::Socket::SSL
    return unless $handle && $handle->isa('IO::Socket::SSL');

    my $tls_info = {
        server_cert       => undef,
        client_cert_chain => [],
        client_cert_name  => undef,
        client_cert_error => undef,
        tls_version       => undef,
        cipher_suite      => undef,
    };

    # Get TLS version - IO::Socket::SSL returns something like 'TLSv1_3'
    if (my $version_str = $handle->get_sslversion) {
        # Map version string to numeric value per TLS spec
        my %version_map = (
            'SSLv3'   => 0x0300,
            'TLSv1'   => 0x0301,
            'TLSv1_1' => 0x0302,
            'TLSv1_2' => 0x0303,
            'TLSv1_3' => 0x0304,
        );
        $tls_info->{tls_version} = $version_map{$version_str};
    }

    # Cipher suite (numeric IANA id). Net::SSLeay/IO::Socket::SSL expose only the
    # cipher *name*, not the 16-bit id the spec asks for. For TLS 1.3 the OpenSSL
    # name IS the IANA name and the registry is frozen at five suites, so we map
    # those exactly. For TLS 1.2 the names are OpenSSL-specific (a large, shifting
    # set), so we leave cipher_suite undef -- the spec permits undef when the
    # server cannot determine the value.
    if (my $cipher_name = $handle->get_cipher) {
        my %tls13_cipher_suites = (
            'TLS_AES_128_GCM_SHA256'       => 0x1301,
            'TLS_AES_256_GCM_SHA384'       => 0x1302,
            'TLS_CHACHA20_POLY1305_SHA256' => 0x1303,
            'TLS_AES_128_CCM_SHA256'       => 0x1304,
            'TLS_AES_128_CCM_8_SHA256'     => 0x1305,
        );
        $tls_info->{cipher_suite} = $tls13_cipher_suites{$cipher_name}
            if exists $tls13_cipher_suites{$cipher_name};
    }

    # Get server certificate (our certificate)
    # IO::Socket::SSL uses sock_certificate() for the server's own cert
    eval {
        my $cert = $handle->sock_certificate;
        if ($cert) {
            require Net::SSLeay;
            $tls_info->{server_cert} = Net::SSLeay::PEM_get_string_X509($cert);
        }
    };
    if ($@) {
        $self->_log(warn => "TLS server certificate extraction error: $@");
    }

    # Get client certificate if provided
    eval {
        my $client_cert = $handle->peer_certificate;
        if ($client_cert) {
            require Net::SSLeay;

            # Get client cert chain
            my @chain;
            push @chain, Net::SSLeay::PEM_get_string_X509($client_cert);

            # Try to get additional certs in chain
            if (my $ssl = $handle->_get_ssl_object) {
                my $chain_obj = Net::SSLeay::get_peer_cert_chain($ssl);
                if ($chain_obj) {
                    for my $i (0 .. Net::SSLeay::sk_X509_num($chain_obj) - 1) {
                        my $cert = Net::SSLeay::sk_X509_value($chain_obj, $i);
                        push @chain, Net::SSLeay::PEM_get_string_X509($cert) if $cert;
                    }
                }
            }
            $tls_info->{client_cert_chain} = \@chain;

            # Get client cert DN (Subject)
            my $subject = Net::SSLeay::X509_NAME_oneline(
                Net::SSLeay::X509_get_subject_name($client_cert)
            );
            $tls_info->{client_cert_name} = $subject if $subject;

            # Check for verification errors
            my $verify_result = $handle->get_sslversion_int;
            # Actually, use verify_result
            if (my $ssl = $handle->_get_ssl_object) {
                my $result = Net::SSLeay::get_verify_result($ssl);
                if ($result != 0) {  # X509_V_OK = 0
                    $tls_info->{client_cert_error} = Net::SSLeay::X509_verify_cert_error_string($result);
                }
            }
        }
    };
    if ($@) {
        $self->_log(warn => "TLS client certificate extraction error: $@");
    }

    $self->{tls_info} = $tls_info;
}

sub _get_scheme {
    my ($self) = @_;

    return $self->{tls_enabled} ? 'https' : 'http';
}

sub _get_ws_scheme {
    my ($self) = @_;

    return $self->{tls_enabled} ? 'wss' : 'ws';
}

sub _get_extensions_for_scope {
    my ($self) = @_;

    my %extensions = %{$self->{extensions}};

    # Add TLS info to extensions if this is a TLS connection
    if ($self->{tls_enabled} && $self->{tls_info}) {
        $extensions{tls} = $self->{tls_info};
    }
    # Remove tls extension if not a TLS connection (per spec)
    elsif (!$self->{tls_enabled}) {
        delete $extensions{tls};
    }

    return \%extensions;
}

#
# SSE (Server-Sent Events) Support Methods
#

async sub _handle_sse_request {
    my ($self, $request) = @_;

    $self->{h1_seq} = 'initial';  # the sse send closure mirrors its state here
    $self->_stop_idle_timer;  # SSE connections are long-lived
    $self->_start_sse_idle_timer;  # Start SSE-specific idle timer if configured

    my $scope = $self->_create_sse_scope($request);
    my $receive = $self->_create_sse_receive($request);
    my $send = $self->_create_sse_send($request);

    my $app_failed = 0;
    my $app_error;
    eval {
        await $self->{app}->($scope, $receive, $send);
    };
    my $error = $@;

    # The boundary _handle_request's tail has, for the same reason and caught
    # the same way: this tail also resumes on the event loop's stack once the
    # application suspends, and its Future is adopted by the same call.
    eval {
        if ($error) {
            $app_error = $error;
            # If SSE not yet started, send HTTP error
            if (!_sse_stream_started($self->{h1_seq})) {
                $self->_send_error_response(500, "Internal Server Error");
            }
            $self->_log(error => "PAGI application error (SSE): $error");
            # An exception is not a clean end -- never keep the connection alive
            # after one, exactly as the plain HTTP request path does.
            $app_failed = 1;
        }

        # Www.pod "Application Left a Response Incomplete": a stream
        # started via sse.start but never ended via sse.close before the
        # application's Future resolved -- whether it returned or threw -- is an
        # incomplete response, never stream_complete. sse.close is the scope's
        # one way to end cleanly (Www.pod "Meaning per scope"); write no
        # terminator the application never asserted, and never keep this
        # connection alive afterward.
        if (($self->{h1_seq} // '') eq 'streaming' && !$self->{sse_finished}) {
            $self->{sse_finished} = 1;   # matches _finish_sse_stream's own guard: no terminator goes out
            $self->_stop_sse_keepalive;
            $self->_stop_sse_idle_timer;
            # Client-already-gone carve-out (mirrors Application Produced No
            # Response): if the transport is already gone, this is not an
            # application error, and the exception (if any) was already logged
            # above.
            $self->_log(error => "PAGI application returned after sse.start without sse.close")
                unless $self->{closed} || $app_failed;
            my $sse_detail = $app_failed ? _detail_from_error($app_error)
                           : 'sse.start without sse.close';
            $self->{current_connection_state}->_mark_disconnected('server_error', $sse_detail)
                if $self->{current_connection_state};
            $self->_end_scope('server_error', detail => $sse_detail);
            return;
        }

        # A refusal (Www.pod "Refusing the stream") answers this scope with an
        # ordinary HTTP response instead of a stream, and it ends the connection
        # either way. Completed, it is a clean end and its response already
        # carried Connection: close, so keep-alive is not on offer. Abandoned
        # before its terminal event, it is an incomplete response under
        # "Application Left a Response Incomplete": no terminator is synthesized
        # and the connection must not serve another request.
        my $refusal_state = $self->{h1_seq} // '';
        if ($refusal_state eq 'refusing' || $refusal_state eq 'refusal_complete') {
            $self->_stop_sse_idle_timer;
            if ($refusal_state eq 'refusing') {
                $self->_flush_pending_headers;   # no terminator follows these
                # Client-already-gone carve-out, as everywhere else: a request
                # that already ended abnormally is not an application error.
                $self->_log(error => "PAGI application returned with an incomplete response")
                    unless $self->{closed} || $app_failed;
                $self->_end_scope('server_error',
                    detail => 'refusal started but never completed');
                return;
            }
            $self->{current_connection_state}->_mark_complete
                if $self->{current_connection_state};
            $self->_end_scope('session_complete');
            return;
        }

        # End the stream (no-op if an explicit sse.close already finished it).
        $self->_finish_sse_stream;

        # The application produced no response at all: no sse.start, and no
        # refusal. That is the same protocol error the plain HTTP path reports,
        # and it must never reach the keep-alive branch below -- the client is
        # still waiting for a response, so handing it back a connection with zero
        # bytes written would hang it until the idle timeout, or forever when
        # timeout => 0. Both refusal outcomes were answered above.
        if (!_sse_stream_started($self->{h1_seq}) && !$self->{response_started}) {
            unless ($self->{closed}) {
                $self->_log(error => "PAGI application returned without starting an SSE stream or a response");
                $self->_send_error_response(500, "Internal Server Error");
            }
            $self->_end_scope('server_error', detail => 'no response was started');
            return;
        }

        # Write access log entry (logs at stream end with total duration). Must
        # precede the reset below, which clears the request it logs.
        $self->_write_access_log;

        # Notify server that request completed (for max_requests tracking). One
        # call site covers both fates below (keep-alive and close) -- the SSE
        # "request" completes here, when the stream ends, not at sse.start.
        $self->{server}->_on_request_complete if $self->{server};

        # Design section 11.6: a CLEAN end -- a stream that never started, or a
        # started stream properly ended with sse.close (the incomplete-response
        # branch above already intercepted a bare return with no sse.close, which
        # is never clean; both refusal outcomes closed the connection above) -- honors the "Connection: keep-alive"
        # header the server itself emitted on sse.start; the terminator is written
        # above and the connection returns to ordinary request handling.
        # Keep-alive still yields to the usual overrides: an application
        # exception, a transport already gone (client disconnect, timeout, write
        # error, server shutdown), a client "Connection: close", and HTTP/1.0
        # semantics.
        if (!$app_failed && !$self->{closed} && $self->_should_keep_alive($request)) {
            $self->_reset_after_sse_stream($request);
            return;
        }

        # The application ended this stream itself with sse.close: the connection
        # ends under the reason it gave, or under the token that names the
        # application as the closer. Anything else reaching here is an ordinary
        # stream completion.
        my $end_reason = (($self->{h1_seq} // '') eq 'closed')
                       ? $self->_end_reason($self, 'app_closed')
                       : 'stream_complete';
        $self->_handle_disconnect_and_close($end_reason);
    };
    $self->_connection_handler_error('HTTP/1.1', $@) if $@;
}

# Idempotently end an SSE stream: write the chunked terminator (HTTP/1.1) and
# release the stream's timers so nothing can write after the terminator.
# Called both by the on-return path above and by an explicit sse.close event
# (which ends the stream while the application is still running), so it must
# run exactly once and must NOT decide the connection's fate -- that decision
# belongs to _handle_sse_request, once the application has actually returned.
# True once the application sent sse.start: the streaming branch of the
# sse send-state machine ('streaming', and 'closed' once sse.close has
# gone out -- advance_sse only reaches 'closed' from 'streaming'). The
# refusal branch ('refusing', 'refusal_complete') is a response but not
# a stream, so it is deliberately excluded: the chunked terminator, the
# incomplete-response check and the stream-scoped send guards all
# apply to a started stream only.
sub _sse_stream_started {
    my ($state) = @_;
    $state //= '';
    return $state eq 'streaming' || $state eq 'closed';
}

sub _finish_sse_stream {
    my ($self) = @_;
    return if $self->{sse_finished};
    $self->{sse_finished} = 1;

    # Nothing may reach the wire after the terminating chunk.
    $self->_stop_sse_keepalive;
    $self->_stop_sse_idle_timer;

    # Send chunked terminator if SSE was started and the stream is still writable
    if (_sse_stream_started($self->{h1_seq}) && !$self->{closed} &&
        $self->{stream} && $self->{stream}->write_handle) {
        $self->{stream}->write("0\r\n\r\n");
    }

    # Mark then wake, for the same reasons as _h2_end_scope_output. A started
    # stream here is always a genuine sse.close: _handle_sse_request's tail
    # intercepts a bare return without one before it ever calls here. The wake
    # is for the clean end alone -- a stream this call is ending because the
    # application abandoned it is not an end the application produced, and
    # _end_scope answers a receive parked on that one, with its reason.
    $self->{current_connection_state}->_mark_complete
        if $self->{current_connection_state} && _sse_stream_started($self->{h1_seq});
    $self->_wake_receive_pending
        if PAGI::Server::EventValidator::scope_send_clean('sse', $self->{h1_seq});
}

# Per-request state that must not survive a kept-alive HTTP/1.1 SSE stream
# (design section 11.6). The connection is about to serve an ordinary request
# on the same socket, so every field the stream touched has to be back at its
# constructor value. Inventory, as a constraint list:
#
#   SSE stream flags   sse_finished
#   Ending record      end_reason, end_detail, end_code
#   SSE timers/writer  sse_keepalive_timer + comment, sse_idle_timer,
#                      sse_keepalive_writer (closes over this stream's framing)
#   Response state     handling_request, response_started, h1_seq,
#                      _resp_pending, response_status, _response_size
#   Request state      request_start, current_request, request_future,
#                      current_connection_state, current_transport_state
#   Receive state      receive_queue, receive_pending, receive_futures
#   Idle timeout       the between-requests idle timer, removed when the
#                      stream started, must be re-armed
#   Request body       discarding_body, set from what the stream left unread
#
# The send closure's own $seq is per-request by construction (a new closure is
# built per request), so the post-sse.close raise contract survives untouched.
#
# _disconnect_handled is deliberately NOT reset: every _handle_disconnect pairs
# with _close (and so with {closed}), which the keep-alive branch excludes, so
# reaching here means no disconnect was ever handled and the flag is still 0.
sub _reset_after_sse_stream {
    my ($self, $request) = @_;

    $self->_stop_sse_keepalive;
    $self->_stop_sse_idle_timer;
    delete $self->{sse_keepalive_writer};

    $self->{scope_kind}   = 'http';
    $self->{sse_finished} = 0;
    $self->{end_reason}   = undef;
    $self->{end_detail}   = undef;
    $self->{end_code}     = undef;

    # Mirrors the keep-alive reset in _handle_request.
    $self->{handling_request}         = 0;
    $self->{response_started}         = 0;
    $self->{h1_seq}                   = 'initial';
    $self->{_resp_pending}            = undef;
    $self->{response_status}          = undef;
    $self->{_response_size}           = 0;
    $self->{request_start}            = undef;
    $self->{current_request}          = undef;
    $self->{request_future}           = undef;
    $self->{current_connection_state} = undef;
    $self->{current_transport_state}  = undef;

    # The finished stream's receive bookkeeping never belongs to the next
    # request. The application has returned, so nothing is awaiting these.
    $self->{receive_queue}   = [];
    $self->{receive_pending} = undef;
    $self->{receive_futures} = [];

    # The SSE stream that just ended completed a request on this connection,
    # same as the plain HTTP path: the next idle expiry reports
    # keepalive_timeout rather than idle_timeout.
    $self->{_served_a_request} = 1;

    # SSE removed the between-requests idle timer as a long-lived mode; an
    # ordinary keep-alive connection must not sit open forever.
    $self->_start_idle_timer;

    # Whatever the stream left unread of the request body is this connection's
    # to discard before it parses anything else, exactly as after a response.
    $self->_begin_body_discard($request);

    # Check if there's more data in the buffer (pipelining)
    if (length($self->{buffer}) > 0) {
        $self->_try_handle_request;
    }
}

sub _create_sse_scope {
    my ($self, $request) = @_;

    my $connection_state = PAGI::Server::ConnectionState->new(
        connection => $self,
        server     => $self->{server},
        on_abort   => $self->_h1_abort_hook,
    );
    $self->{current_connection_state} = $connection_state;

    my $scope = {
        type         => 'sse',
        pagi         => {
            version      => '0.5',
            spec_version => '0.6',
        },
        http_version => $request->{http_version},
        method       => $request->{method},
        scheme       => $self->_get_scheme,
        path         => $request->{path},
        raw_path     => $request->{raw_path},
        query_string => $request->{query_string},
        root_path    => '',
        headers      => $request->{headers},
        (defined $self->{client_host}
            ? (client => [$self->{client_host}, $self->{client_port}])
            : ()
        ),
        server       => [$self->{server_host}, $self->{server_port}],
        # Optimized: avoid hash copy when state is empty (common case)
        state        => keys %{$self->{state}} ? { %{$self->{state}} } : {},
        extensions   => $self->_get_extensions_for_scope,
        # Connection state for non-destructive disconnect detection
        'pagi.connection' => $connection_state,
        # Outbound flow-control introspection (buffered_amount, watermarks,
        # on_high_water/on_drain). Stashed on the connection too, so the send
        # path can poke _check_watermarks after each write.
        'pagi.transport' => ($self->{current_transport_state} = $self->_h1_transport_state),
    };

    return $scope;
}

sub _create_sse_receive {
    my ($self, $request) = @_;

    my $content_length = $request->{content_length};
    my $is_chunked = $request->{chunked} // 0;
    my $expect_continue = $request->{expect_continue} // 0;
    my $has_body = defined($content_length) && $content_length > 0 || $is_chunked;

    # On the request record for the same reason as the http scope's twin: the
    # tail of a kept-alive stream asks what is left of this body.
    $request->{body_complete}   = 0;
    $request->{body_bytes_read} = 0;
    $request->{continue_sent}   = 0;

    weaken(my $weak_self = $self);

    # Helper to create SSE disconnect event with reason
    my $sse_disconnect = sub {
        return {
            type   => 'sse.disconnect',
            reason => $weak_self ? $weak_self->_end_reason($weak_self) : 'client_closed',
        };
    };

    # This scope's cap record and its gate. See _disconnect_receive_future.
    my %cap = (scope => 'sse', transport => 'HTTP/1.1', count => 0);
    my $disconnect = sub {
        my ($parked) = @_;
        return Future->done($sse_disconnect->()) unless $weak_self;
        return $weak_self->_disconnect_receive_future(\%cap, $sse_disconnect, $parked);
    };

    # The answer for a receive made after this scope's clean end -- the
    # application's own sse.close, or a completed refusal. It goes through the
    # same gate, so the cap counts it exactly like every other synthesized
    # answer.
    my $scope_end = sub {
        my ($parked) = @_;
        return Future->done(_sse_scope_end_event()) unless $weak_self;
        return $weak_self->_disconnect_receive_future(
            \%cap, \&_sse_scope_end_event, $parked);
    };

    # This scope reached a clean end: the application closed its own stream
    # with sse.close, or the server finished its output of a refusal. The send
    # state is the whole fact -- advance_sse reaches 'closed' only through
    # sse.close and 'refusal_complete' only through a finished refusal
    # response -- so a server-decided end (shutdown, idle timeout) or a
    # transport that goes away is never one of these.
    my $clean_end = sub {
        return 0 unless $weak_self;
        return PAGI::Server::EventValidator::scope_send_clean('sse', $weak_self->{h1_seq});
    };

    # The stream is over: the transport is gone, or the application abandoned
    # a started stream without sse.close (_handle_sse_request's tail marks it
    # finished). Nothing more will ever arrive on this scope, so answer with
    # its disconnect instead of blocking forever. A clean end is not here --
    # the scope-end answer below reports that one.
    my $stream_over = sub {
        return 1 unless $weak_self;
        return $weak_self->{closed} || $weak_self->{sse_finished};
    };

    return sub {
        # A clean end the application itself produced -- its own sse.close, or
        # a finished refusal response -- delivers no sse.disconnect of its own
        # (Www.pod "SSE Disconnect - receive event" lists what does: the client
        # disconnecting, and the server shutting down a started stream). A
        # receive made after it reports the end, with the reason key absent,
        # because the object was marked complete with no reason to agree with
        # (Www.pod "Receiving after the scope's end"). Asked ahead of the
        # transport test, because the ending is a fact about the scope: the
        # connection closes when the application returns, not at the terminal
        # event, so the app may well still be running against a live socket.
        # The websocket receive answers the same way, and so does the h2 side
        # of both.
        return $scope_end->()
            if $weak_self && $clean_end->();

        if ($stream_over->()) {
            return $disconnect->();
        }

        my $future = (async sub {
            # True once this call has waited: the answer it then gets is a
            # delivery of the scope's terminal state, not a repeat request
            # for it, so the cap does not count it.
            my $parked = 0;

            if ($stream_over->()) {
                return await $disconnect->($parked);
            }

            # Check queue first
            if (@{$weak_self->{receive_queue}}) {
                return shift @{$weak_self->{receive_queue}};
            }

            # Handle request body for POST/PUT SSE requests
            if ($has_body && !$request->{body_complete}) {
                # Send 100 Continue if client expects it (before reading body)
                if ($expect_continue && !$request->{continue_sent}) {
                    $request->{continue_sent} = 1;
                    $weak_self->{stream}->write($weak_self->{protocol}->serialize_continue);
                }

                if ($is_chunked) {
                    # The clean-end pair travels with the disconnect: this
                    # sub parks on the same receive_pending, and the sibling
                    # Content-Length branch below asks the same question in
                    # the same place.
                    return await $weak_self->_read_chunked_body(
                        'sse.request',
                        $disconnect,
                        \$request->{body_complete},
                        \$request->{body_bytes_read},
                        $clean_end,
                        $scope_end,
                    );
                }

                my $remaining = $content_length - $request->{body_bytes_read};

                # Wait for data if buffer is empty
                while (length($weak_self->{buffer}) == 0 && !$weak_self->{closed} && $remaining > 0) {
                    # The scope ended cleanly under this call -- sse.close from
                    # another Future while this one waits for the request body.
                    # The end is what this call reports, rather than a
                    # truncated body or a reason this scope never had. The h2
                    # twin asks in the same place.
                    return await $scope_end->($parked) if $clean_end->();

                    if (!$weak_self->{receive_pending}) {
                        $weak_self->{receive_pending} = Future->new;
                    }
                    $parked = 1;
                    await $weak_self->{receive_pending};
                    $weak_self->{receive_pending} = undef;

                    if (@{$weak_self->{receive_queue}}) {
                        return shift @{$weak_self->{receive_queue}};
                    }
                }

                if ($weak_self->{closed}) {
                    return await $disconnect->($parked);
                }

                # Read available data up to remaining
                my $to_read = $remaining < length($weak_self->{buffer})
                    ? $remaining
                    : length($weak_self->{buffer});

                my $chunk = substr($weak_self->{buffer}, 0, $to_read, '');
                $request->{body_bytes_read} += length($chunk);

                my $more = ($request->{body_bytes_read} < $content_length) ? 1 : 0;
                $request->{body_complete} = 1 if !$more;

                return {
                    type => 'sse.request',
                    body => $chunk,
                    more => $more,
                };
            }

            # No body or body complete - return empty body if not yet returned
            if (!$request->{body_complete}) {
                $request->{body_complete} = 1;
                return {
                    type => 'sse.request',
                    body => '',
                    more => 0,
                };
            }

            # Wait for disconnect
            while (1) {
                # A call already parked when the scope ends cleanly is
                # answered with that end, for the reason the head of this
                # closure gives. Asked ahead of the queue: a transport that
                # goes away after a clean end still queues its own disconnect,
                # and it is the scope's ending, not the transport's, that
                # decides what this call reports.
                return await $scope_end->($parked)
                    if $clean_end->();

                if (@{$weak_self->{receive_queue}}) {
                    return shift @{$weak_self->{receive_queue}};
                }

                if ($weak_self->{closed}) {
                    return await $disconnect->($parked);
                }

                if (!$weak_self->{receive_pending}) {
                    $weak_self->{receive_pending} = Future->new;
                }
                $parked = 1;
                await $weak_self->{receive_pending};
                $weak_self->{receive_pending} = undef;
            }
        })->();

        # Track this Future
        push @{$weak_self->{receive_futures}}, $future;
        @{$weak_self->{receive_futures}} = grep { !$_->is_ready } @{$weak_self->{receive_futures}};

        return $future;
    };
}

sub _format_sse_event {
    my ($event) = @_;
    my $sse_data = '';

    if (defined $event->{event} && length $event->{event}) {
        die "Invalid SSE event name: contains newline\n"
            if $event->{event} =~ /[\r\n]/;
        $sse_data .= "event: $event->{event}\n";
    }

    my $data = $event->{data} // '';
    for my $line (split /\r?\n|\r/, $data, -1) {
        $sse_data .= "data: $line\n";
    }

    if (defined $event->{id} && length $event->{id}) {
        die "Invalid SSE id: contains newline\n"
            if $event->{id} =~ /[\r\n]/;
        $sse_data .= "id: $event->{id}\n";
    }

    if (defined $event->{retry}) {
        die "Invalid SSE retry: must be a non-negative integer\n"
            unless $event->{retry} =~ /\A[0-9]+\z/;
        $sse_data .= "retry: $event->{retry}\n";
    }

    $sse_data .= "\n";
    return $sse_data;
}

sub _format_sse_comment {
    my ($event) = @_;
    my $text = $event->{comment} // '';
    my $formatted = '';
    for my $line (split /\r?\n|\r/, $text, -1) {
        $line = ":$line" unless $line =~ /^:/;
        $formatted .= "$line\n";
    }
    $formatted .= "\n";
    return $formatted;
}

sub _create_sse_send {
    my ($self, $request) = @_;

    weaken(my $weak_self = $self);
    my $seq = 'initial';

    # Refusing the stream is an ordinary HTTP response on this scope (Www.pod
    # "Refusing the stream"); see the twin in _create_websocket_send for why
    # the http send path writes it and why its own state is the fact.
    my $refusal_send;

    return async sub  {
        my ($event) = @_;
        return Future->done unless $weak_self;

        my $type = $event->{type} // '';

        # Once the machine has already recorded this stream as closed --
        # either an app-initiated sse.close (which closes the transport as a
        # side effect of ending the stream) or a completed refusal response
        # (whose finalize branch also closes the transport as a side
        # effect) -- the machine, not the transport-closed check below,
        # decides what happens next: idempotent no-op for a repeat
        # sse.close, croak for anything else (refusal_complete has no
        # idempotent case; it croaks unconditionally).
        my $already_closed = ($seq eq 'closed' || $seq eq 'refusal_complete');

        # Transport already gone for reasons other than our own close (a real
        # client disconnect): sends are a silent no-op.
        return Future->done if $weak_self->{closed} && !$already_closed;

        # Reset SSE idle timer on send activity (skip once fully closed)
        $weak_self->_reset_sse_idle_timer unless $already_closed;

        # Mandatory event validation and sequencing (PAGI spec compliance).
        # Order per spec: transport-closed no-op check above runs first.
        PAGI::Server::EventValidator::validate_sse_send(
            $event, { extensions => $weak_self->{extensions} });
        $seq = PAGI::Server::EventValidator::advance_sse($seq, $event);
        $weak_self->{h1_seq} = $seq;

        if ($type =~ /^http\.response\./) {
            # Refusing the stream (Www.pod): identical on the wire to the same
            # response on an http scope, so that path writes it. advance_sse
            # has already rejected an HTTP event after sse.start.
            $refusal_send //= $weak_self->_create_send($request,
                refusal  => 1,
                on_state => sub {
                    return unless $weak_self;
                    $seq = $_[0] eq 'complete' ? 'refusal_complete' : 'refusing';
                    $weak_self->{h1_seq} = $seq;
                },
            );
            my $ok = eval { await $refusal_send->($event); 1 };
            my $error = $@;
            die $error unless $ok;
            return;
        }

        if ($type eq 'sse.start') {
            $weak_self->{response_started} = 1;

            my $status = $event->{status} // 200;
            $weak_self->{response_status} = $status;  # Track for access logging
            my $headers = $event->{headers} // [];
            # PAGI spec — HTTP/1.1 owns Transfer-Encoding and Connection;
            # strip any app-supplied values before they reach the wire.
            $headers = $weak_self->_h1_strip_connection_headers($headers);

            # Ensure Content-Type is text/event-stream
            my $has_content_type = 0;
            for my $h (@$headers) {
                if (lc($h->[0]) eq 'content-type') {
                    $has_content_type = 1;
                    last;
                }
            }

            my @final_headers = @$headers;
            if (!$has_content_type) {
                push @final_headers, ['content-type', 'text/event-stream'];
            }

            # Cache-Control and Date: server-supplied only when the app didn't
            # supply them (design doc section 11.4). Connection is a framing
            # header the protocol requires the server to control, so it is
            # always advertised regardless of what the app sent.
            unless (grep { lc($_->[0]) eq 'cache-control' } @final_headers) {
                push @final_headers, ['cache-control', 'no-cache'];
            }
            push @final_headers, ['connection', 'keep-alive'];
            unless (grep { lc($_->[0]) eq 'date' } @final_headers) {
                push @final_headers, ['date', $weak_self->{protocol}->format_date];
            }

            # SSE uses chunked encoding implicitly (no Content-Length)
            my $response = $weak_self->{protocol}->serialize_response_start(
                $status, \@final_headers, 1  # chunked = 1
            );

            $weak_self->{stream}->write($response);

            # Set protocol-specific keepalive writer (HTTP/1.1 chunked)
            $weak_self->{sse_keepalive_writer} = sub {
                my ($text) = @_;
                return unless $weak_self;
                return if $weak_self->{closed};
                # PAGI Www.pod "Send SSE": encode to UTF-8 exactly once, at
                # the wire boundary — the chunk-size prefix below is on the
                # resulting BYTE string.
                my $bytes = eval { Encode::encode('UTF-8', $text, Encode::FB_CROAK) };
                die "sse payload is not encodable as UTF-8: $@" unless defined $bytes;
                my $len = sprintf("%x", length($bytes));
                $weak_self->{stream}->write("$len\r\n$bytes\r\n");
            };
        }
        elsif ($type eq 'sse.send') {
            # --- BACKPRESSURE CHECK ---
            if ($weak_self->_get_write_buffer_size >= $weak_self->{write_high_watermark}) {
                await $weak_self->_wait_for_drain;
                return Future->done unless $weak_self;
                return Future->done if $weak_self->{closed};
            }
            # --- END BACKPRESSURE CHECK ---

            my $sse_data = _format_sse_event($event);
            # PAGI Www.pod "Send SSE": encode to UTF-8 exactly once, at the
            # wire boundary — a failed encode fails this send's Future.
            my $bytes = eval { Encode::encode('UTF-8', $sse_data, Encode::FB_CROAK) };
            die "sse payload is not encodable as UTF-8: $@" unless defined $bytes;

            # Send as chunked data
            my $len = sprintf("%x", length($bytes));
            $weak_self->{stream}->write("$len\r\n$bytes\r\n");
            $weak_self->_notify_transport_write;
        }
        elsif ($type eq 'sse.comment') {
            my $comment = _format_sse_comment($event);
            my $bytes = eval { Encode::encode('UTF-8', $comment, Encode::FB_CROAK) };
            die "sse payload is not encodable as UTF-8: $@" unless defined $bytes;

            my $len = sprintf("%x", length($bytes));
            $weak_self->{stream}->write("$len\r\n$bytes\r\n");
        }
        elsif ($type eq 'sse.keepalive') {
            # SSE keepalive - starts/stops periodic comment timer
            my $interval = $event->{interval} // 0;
            my $comment = $event->{comment};

            if ($interval > 0) {
                $weak_self->_start_sse_keepalive($interval, $comment);
            }
            else {
                $weak_self->_stop_sse_keepalive;
            }
        }
        elsif ($type eq 'sse.close') {
            # Explicit application-initiated end of the SSE stream. End it now,
            # decoupled from the application returning: the terminator goes out
            # here, but the connection's fate is settled by _handle_sse_request
            # once the application has actually returned (design section 11.6),
            # so the application keeps running against a live transport and its
            # further sends are rejected by the sequence machine, not by a
            # closed check. `reason` is server-side metadata only and is never
            # written to the wire. Idempotency for a repeat sse.close is handled
            # by advance_sse (the 'closed' state accepts another sse.close as a
            # no-op), and h1_seq is already 'closed' by the time this arm runs.
            $weak_self->_record_end($weak_self, reason => $event->{reason});
            $weak_self->_finish_sse_stream;
        }
        elsif ($type eq 'http.fullflush') {
            # Fullflush extension - force immediate TCP buffer flush.
            # validate_sse_send (called above, unconditionally) already
            # croaks "Extension not enabled: fullflush" when the extension
            # isn't advertised, so no re-check is needed here.

            # Force flush by ensuring TCP_NODELAY
            my $handle = $weak_self->{stream}->write_handle;
            if ($handle && $handle->can('setsockopt')) {
                require Socket;
                $handle->setsockopt(Socket::IPPROTO_TCP(), Socket::TCP_NODELAY(), 1);
            }
        }

        return;
    };
}

#
# WebSocket Support Methods
#

# WebSocket handshake magic GUID per RFC 6455
use constant WS_GUID => '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

async sub _handle_websocket_request {
    my ($self, $request) = @_;

    $self->{h1_seq} = 'connecting';  # the websocket send closure mirrors its state here
    $self->_stop_idle_timer;  # WebSocket connections are long-lived
    $self->_start_ws_idle_timer;  # Start WebSocket-specific idle timer if configured

    my $scope = $self->_create_websocket_scope($request);
    my $receive = $self->_create_websocket_receive($request);
    my $send = $self->_create_websocket_send($request);

    eval {
        await $self->{app}->($scope, $receive, $send);
    };
    my $error = $@;

    # The boundary _handle_request's tail has, for the same reason and caught
    # the same way: this tail also resumes on the event loop's stack once the
    # application suspends, and its Future is adopted by the same call.
    eval {
        my $app_failed = 0;
        if ($error) {
            # If neither the handshake nor a refusal response has begun, answer
            # with an HTTP error; once a refusal has started there is no room on
            # the wire for a second response.
            if (!_ws_handshake_accepted($self->{h1_seq}) && !$self->{response_started}) {
                $self->_send_error_response(500, "Internal Server Error");
            }
            $self->_log(error => "PAGI application error (WebSocket): $error");
            $app_failed = 1;
        }

        # A refusal abandoned before its terminal event is an incomplete response
        # (Www.pod "Application Left a Response Incomplete", which names the
        # refusal case first): no terminator is synthesized, the connection ends,
        # and the scope reports server_error. A completed refusal falls through to
        # the clean-end mark below.
        if (($self->{h1_seq} // '') eq 'refusing') {
            $self->_flush_pending_headers;   # no terminator follows these
            $self->_log(error => "PAGI application returned with an incomplete response")
                unless $self->{closed} || $app_failed;
            $self->_end_scope('server_error',
                detail => 'refusal started but never completed');
            return;
        }

        # Www.pod "Meaning per scope": returning from a websocket scope without
        # websocket.accept or a refusal is governed by "Application Produced No
        # Response" -- the 500 backstop, its log, and its client-gone carve-out,
        # exactly as on an http or sse scope. Until sub-spec 0.6 a pre-accept
        # websocket.close answered such a client with a 403; that event is now out
        # of sequence, so nothing else covers this path and the client would be
        # left waiting. _send_error_response is a no-op once a response started,
        # so an exception already answered above cannot produce a second one.
        if (!_ws_handshake_accepted($self->{h1_seq}) && !$self->{response_started}) {
            unless ($self->{closed}) {
                $self->_log(error => "PAGI application returned without accepting the WebSocket or refusing the handshake")
                    unless $app_failed;
                $self->_send_error_response(500, "Internal Server Error");
            }
            $self->_end_scope('server_error', detail => 'no response was started');
            return;
        }

        # Write access log entry (logs at connection close with total duration)
        $self->_write_access_log;
        $self->{server}->_on_request_complete if $self->{server};

        if (my $cs = $self->{current_connection_state}) {
            if (_ws_handshake_accepted($self->{h1_seq})
                && !PAGI::Server::EventValidator::scope_send_clean('websocket', $self->{h1_seq})
                && !$self->{ws_peer_closed}
                && !$self->{closed}) {
                # App returned from an accepted socket without a closing
                # handshake: an incomplete response (Www.pod "Application Left a
                # Response Incomplete"). Close with 1011, report server_error,
                # log; never a clean end. The guard asks the three facts the
                # clean-end test asks: the handshake was accepted, the
                # application's own send state never reached a clean end, and no
                # peer Close frame validated. _send_close_frame records the code;
                # flushed by _close's close_when_empty before the socket closes.
                $self->_send_close_frame(1011, '');
                $self->_record_end($self, reason => 'server_error');
                $self->_log(error => "PAGI application returned from an accepted WebSocket without a closing handshake");
                $self->_handle_disconnect_and_close('server_error',
                    detail => 'accepted socket left without a closing handshake');
                return;
            }
            # Keyed on the app's own send state and the validated peer Close
            # frame rather than close_sent||close_received: a server-initiated
            # protocol close (which also sets close_sent, via _send_close_frame)
            # never advances h1_seq to 'closed' and never sets ws_peer_closed, so
            # it can never satisfy this guard -- robust by construction rather
            # than by the ordering that currently marks the connection_state
            # abnormal before this tail ever runs.
            $cs->_mark_complete
                if (_ws_handshake_accepted($self->{h1_seq})
                    && (($self->{h1_seq} // '') eq 'closed' || $self->{ws_peer_closed}))
                || (($self->{h1_seq} // '') eq 'refusal_complete');
        }

        # Close connection after WebSocket session ends
        $self->_handle_disconnect_and_close('session_complete');
    };
    $self->_connection_handler_error('HTTP/1.1', $@) if $@;
}

sub _create_websocket_scope {
    my ($self, $request) = @_;

    # Extract WebSocket key and subprotocols from headers
    my $ws_key;
    my @subprotocols;

    for my $header (@{$request->{headers}}) {
        my ($name, $value) = @$header;
        if ($name eq 'sec-websocket-key') {
            $ws_key = $value;
        }
        elsif ($name eq 'sec-websocket-protocol') {
            # Parse comma-separated list of subprotocols
            push @subprotocols, map { s/^\s+|\s+$//gr } split /,/, $value;
        }
    }

    # Store ws_key for handshake response
    $self->{ws_key} = $ws_key;

    my $connection_state = PAGI::Server::ConnectionState->new(
        connection => $self,
        server     => $self->{server},
        on_abort   => $self->_h1_abort_hook,
    );
    $self->{current_connection_state} = $connection_state;

    my $scope = {
        type         => 'websocket',
        pagi         => {
            version      => '0.5',
            spec_version => '0.6',
        },
        http_version => $request->{http_version},
        scheme       => $self->_get_ws_scheme,
        path         => $request->{path},
        raw_path     => $request->{raw_path},
        query_string => $request->{query_string},
        root_path    => '',
        headers      => $request->{headers},
        (defined $self->{client_host}
            ? (client => [$self->{client_host}, $self->{client_port}])
            : ()
        ),
        server       => [$self->{server_host}, $self->{server_port}],
        subprotocols => \@subprotocols,
        # Optimized: avoid hash copy when state is empty (common case)
        state        => keys %{$self->{state}} ? { %{$self->{state}} } : {},
        extensions   => do {
            my %ext = %{$self->_get_extensions_for_scope};
            # fullflush has no validate_websocket_send arm; advertising it here
            # would lie to the app (design 13.2).
            delete $ext{fullflush};
            \%ext;
        },
        # max_frame_size: omitted when unenforced (max_ws_frame_size 0/undef
        # means unlimited, per Protocol::WebSocket::Frame's max_payload_size
        # semantics -- a server that does not enforce a cap must not
        # advertise one). max_receive_queue has no unlimited mode (a hard,
        # always-enforced cap), so it is always present.
        ($self->{max_ws_frame_size}
            ? (max_frame_size => $self->{max_ws_frame_size})
            : ()
        ),
        max_receive_queue => $self->{max_receive_queue},
        # Connection state for non-destructive disconnect detection
        'pagi.connection' => $connection_state,
        # Outbound flow-control introspection (buffered_amount, watermarks,
        # on_high_water/on_drain). Stashed on the connection too, so the send
        # path can poke _check_watermarks after each write.
        'pagi.transport' => ($self->{current_transport_state} = $self->_h1_transport_state),
    };

    return $scope;
}

sub _create_websocket_receive {
    my ($self, $request) = @_;

    my $connect_sent = 0;
    weaken(my $weak_self = $self);

    # This scope's cap record and its gate. See _disconnect_receive_future.
    my %cap = (scope => 'websocket', transport => 'HTTP/1.1', count => 0);
    my $disconnect = sub {
        my ($parked) = @_;
        return Future->done({ type => 'websocket.disconnect', code => 1006, reason => 'client_closed' })
            unless $weak_self;
        return $weak_self->_disconnect_receive_future(
            \%cap, $weak_self->_ws_disconnect_event, $parked);
    };

    # This scope's clean end, which on a websocket scope is a completed
    # refusal: advance_websocket reaches 'refusal_complete' only when the
    # refusal response finished, so no server-decided ending is ever mistaken
    # for one.
    my $refusal_over = sub {
        return 0 unless $weak_self;
        return (($weak_self->{h1_seq} // '') eq 'refusal_complete') ? 1 : 0;
    };

    # The answer for a receive made after that clean end. It goes through the
    # same gate, so the cap counts it exactly like every other synthesized
    # answer.
    my $scope_end = sub {
        my ($parked) = @_;
        return Future->done(_ws_refusal_end_event()) unless $weak_self;
        return $weak_self->_disconnect_receive_future(
            \%cap, \&_ws_refusal_end_event, $parked);
    };

    # Nothing more will ever arrive on this scope, so a receive() answers now
    # instead of parking on a Future nothing is left to resolve. The two
    # endings are not the same moment on HTTP/1.1: a completed closing
    # handshake queues the scope's one websocket.disconnect and leaves the
    # socket open until the application returns, and Www.pod "Disconnect -
    # receive event" says that once this event has been delivered the scope is
    # over, and a further receive() resolves with the same websocket.disconnect
    # again. _disconnect_handled is the fact every delivery path sets, and the
    # sse receive asks its own stream the same question ($stream_over).
    my $scope_over = sub {
        return 1 unless $weak_self;
        return $weak_self->{closed} || $weak_self->{_disconnect_handled};
    };

    return sub {
        return Future->done({ type => 'websocket.disconnect', code => 1006, reason => 'client_closed' })
            unless $weak_self;

        # Check queue first - drain queued messages even if closed
        if (@{$weak_self->{receive_queue}}) {
            return Future->done(shift @{$weak_self->{receive_queue}});
        }

        # A completed refusal is this scope's clean end: it delivers no
        # websocket.disconnect of its own (Www.pod "Disconnect - receive
        # event"), and a receive made after it reports the end with the
        # http.disconnect of the HTTP exchange that refusal was (Www.pod
        # "Receiving after the scope's end"). The sse refusal answers the same
        # way, and so does the h2 side of both.
        return $scope_end->()
            if $refusal_over->();

        # $weak_self is known live here, so the fallback can report the
        # abnormal reason a server-initiated close recorded (idle timeout,
        # queue overflow, ...) instead of the bare 'client_closed' default.
        return $disconnect->()
            if $scope_over->();

        my $future = (async sub {
            return { type => 'websocket.disconnect', code => 1006, reason => 'client_closed' }
                unless $weak_self;

            # True once this call has waited: the answer it then gets is a
            # delivery of the scope's terminal state, not a repeat request
            # for it, so the cap does not count it.
            my $parked = 0;

            # Check queue first - drain queued messages even if closed
            if (@{$weak_self->{receive_queue}}) {
                return shift @{$weak_self->{receive_queue}};
            }

            if ($scope_over->()) {
                return await $disconnect->($parked);
            }

            # First call returns websocket.connect
            if (!$connect_sent) {
                $connect_sent = 1;
                return { type => 'websocket.connect' };
            }

            # If not in WebSocket mode yet (waiting for accept), wait
            while (!_ws_handshake_accepted($weak_self->{h1_seq}) && !$scope_over->()) {
                # The scope ended cleanly under this call: the refusal was
                # sent from another Future while this one waited. A refusal
                # never accepts, so this loop is where such a call is parked,
                # and the end is what it reports.
                return await $scope_end->($parked) if $refusal_over->();

                if (!$weak_self->{receive_pending}) {
                    $weak_self->{receive_pending} = Future->new;
                }
                $parked = 1;
                await $weak_self->{receive_pending};
                $weak_self->{receive_pending} = undef;

                if (@{$weak_self->{receive_queue}}) {
                    return shift @{$weak_self->{receive_queue}};
                }
            }

            if ($scope_over->()) {
                return await $disconnect->($parked);
            }

            # Wait for events from frame processing
            while (1) {
                if (@{$weak_self->{receive_queue}}) {
                    return shift @{$weak_self->{receive_queue}};
                }

                if ($scope_over->()) {
                    return await $disconnect->($parked);
                }

                if (!$weak_self->{receive_pending}) {
                    $weak_self->{receive_pending} = Future->new;
                }
                $parked = 1;
                await $weak_self->{receive_pending};
                $weak_self->{receive_pending} = undef;
            }
        })->();

        # Track this Future
        push @{$weak_self->{receive_futures}}, $future;
        @{$weak_self->{receive_futures}} = grep { !$_->is_ready } @{$weak_self->{receive_futures}};

        return $future;
    };
}

sub _create_websocket_send {
    my ($self, $request) = @_;

    weaken(my $weak_self = $self);
    my $seq = 'connecting';

    # Refusing the handshake is an ordinary HTTP response on this scope
    # (Www.pod "Refusing the handshake"), so it goes out through the ordinary
    # HTTP send path rather than a second implementation of one. That closure
    # runs the http send machine, and its own state is the fact about whether
    # the refusal completed -- advance_websocket abbreviates that question
    # (see its POD).
    my $refusal_send;

    return async sub  {
        my ($event) = @_;
        return Future->done unless $weak_self;

        # Once the machine has recorded the connection closed or the refusal
        # response complete, the machine decides what happens next -- both
        # 'closed' and 'refusal_complete' have no idempotent case, so any
        # further send always raises -- not the transport-closed check below.
        # Only app-initiated terminals get this carve-out: websocket.close
        # itself flips {closed} synchronously via _handle_disconnect_and_close
        # when the client had already sent its own close frame first, but
        # that's still the app's own logical close (it chose to answer with
        # websocket.close), so a post-close send from the app is a protocol
        # violation, not a benign transport no-op. A real client disconnect
        # with no app-initiated terminal in play still falls through to the
        # plain no-op below.
        my $already_closed = ($seq eq 'closed' || $seq eq 'refusal_complete');

        return Future->done if $weak_self->{closed} && !$already_closed;

        # Reset WebSocket idle timer on send activity
        $weak_self->_reset_ws_idle_timer;

        my $type = $event->{type} // '';

        # Mandatory event validation and sequencing (PAGI spec compliance).
        # Order per spec: transport-closed no-op check above runs first.
        PAGI::Server::EventValidator::validate_websocket_send(
            $event, { extensions => $weak_self->{extensions} });
        my $seq_before = $seq;
        $seq = PAGI::Server::EventValidator::advance_websocket($seq, $event);
        $weak_self->{h1_seq} = $seq;

        if ($type =~ /^http\.response\./) {
            # Refusing the handshake (Www.pod): the wire response is identical
            # to the same response on an http scope, so it is that path that
            # writes it. advance_websocket has already rejected an HTTP event
            # after websocket.accept, so reaching here means the handshake is
            # still open.
            # The http machine inside the delegate is the authority on whether
            # the refusal completed: it knows whether the start declared
            # trailers, and it rolls its own state back for a send it rejects
            # (an unopenable file, undeclared trailers), which must leave no
            # state behind here either. So it publishes straight into this
            # scope's own state.
            $refusal_send //= $weak_self->_create_send($request,
                refusal  => 1,
                on_state => sub {
                    return unless $weak_self;
                    $seq = $_[0] eq 'complete' ? 'refusal_complete' : 'refusing';
                    $weak_self->{h1_seq} = $seq;
                },
            );
            my $ok = eval { await $refusal_send->($event); 1 };
            my $error = $@;
            die $error unless $ok;
            return;
        }

        if ($type eq 'websocket.accept') {
            # A duplicate accept is already rejected by advance_websocket.

            # Complete the WebSocket handshake
            my $ws_key = $weak_self->{ws_key};
            my $accept_key = sha1_base64($ws_key . WS_GUID);
            # sha1_base64 doesn't add padding, but WebSocket requires it
            $accept_key .= '=' while length($accept_key) % 4;

            # Build and write the handshake under advance-then-rollback (the
            # file's D1 pattern): an app-supplied subprotocol or header that
            # fails byte validation must leave no state behind, and the send
            # state is now the only record of whether the handshake completed.
            # Without the rollback a rejected accept would read as an accepted
            # socket, and the client would get no response at all.
            my $ok = eval {
                my @headers = (
                    "HTTP/1.1 101 Switching Protocols\r\n",
                    "Upgrade: websocket\r\n",
                    "Connection: Upgrade\r\n",
                    "Sec-WebSocket-Accept: $accept_key\r\n",
                );

                # Add subprotocol if specified (with validation)
                if (my $subprotocol = $event->{subprotocol}) {
                    $subprotocol = _validate_subprotocol($subprotocol);
                    push @headers, "Sec-WebSocket-Protocol: $subprotocol\r\n";
                }

                # Add custom headers if specified (with CRLF injection
                # validation). The server's own "Connection: Upgrade" line
                # above is untouched (RFC 6455 requires it) -- strip the app's
                # extra headers the same way every other h1 response-header
                # path already does, so an app-supplied
                # connection/transfer-encoding value can't duplicate or
                # contradict it on the wire.
                if (my $extra_headers = $event->{headers}) {
                    $extra_headers = $weak_self->_h1_strip_connection_headers($extra_headers);
                    for my $h (@$extra_headers) {
                        my ($name, $value) = @$h;
                        $name = _validate_header_name($name);
                        $value = _validate_header_value($value);
                        push @headers, "$name: $value\r\n";
                    }
                }

                push @headers, "\r\n";

                $weak_self->{stream}->write(join('', @headers));
                1;
            };
            unless ($ok) {
                my $error = $@;
                $seq = $seq_before;
                $weak_self->{h1_seq} = $seq;
                die $error;
            }

            # Switch to WebSocket mode. h1_seq is already 'accepted' -- the
            # mirror above runs before this arm, so _ws_handshake_accepted is
            # true for every reader from here on.
            $weak_self->{websocket_frame} = Protocol::WebSocket::Frame->new(
                max_payload_size => $weak_self->{max_ws_frame_size},
            );
            $weak_self->{response_status} = 101;  # Track for access logging

            # Notify any waiting receive
            if ($weak_self->{receive_pending} && !$weak_self->{receive_pending}->is_ready) {
                my $f = $weak_self->{receive_pending};
                $weak_self->{receive_pending} = undef;
                $f->done;
            }

            # Process any data that arrived before accept
            if (length($weak_self->{buffer}) > 0) {
                $weak_self->_process_websocket_frames;
            }
        }
        elsif ($type eq 'websocket.send') {
            return unless _ws_handshake_accepted($weak_self->{h1_seq});

            # --- BACKPRESSURE CHECK ---
            if ($weak_self->_get_write_buffer_size >= $weak_self->{write_high_watermark}) {
                await $weak_self->_wait_for_drain;
                return Future->done unless $weak_self;
                return Future->done if $weak_self->{closed};
            }
            # --- END BACKPRESSURE CHECK ---

            my $frame;
            if (defined $event->{text}) {
                $frame = Protocol::WebSocket::Frame->new(
                    buffer => $event->{text},
                    type   => 'text',
                );
            }
            elsif (defined $event->{bytes}) {
                $frame = Protocol::WebSocket::Frame->new(
                    buffer => $event->{bytes},
                    type   => 'binary',
                );
            }
            else {
                return;  # Nothing to send
            }

            my $bytes = $frame->to_bytes;
            $weak_self->{stream}->write($bytes);
            $weak_self->_notify_transport_write;
        }
        elsif ($type eq 'websocket.close') {
            # A close before accept is already rejected by advance_websocket
            # ("cannot send 'websocket.close' before websocket.accept"), so
            # the handshake is known complete here.

            # Send close frame
            my $code = $event->{code} // 1000;
            my $reason = $event->{reason} // '';

            my $frame = Protocol::WebSocket::Frame->new(
                type   => 'close',
                buffer => pack('n', $code) . $reason,
            );

            $weak_self->{stream}->write($frame->to_bytes);
            $weak_self->{close_sent} = 1;
            # App-initiated close: a clean end (Www.pod "Meaning per scope").
            # h1_seq is already 'closed' by the time this arm runs, and
            # _handle_websocket_request's tail reads it there.

            # If we received a close frame, close immediately
            # Otherwise wait for close from client (handled in frame processing)
            if ($weak_self->{close_received}) {
                $weak_self->_handle_disconnect_and_close('client_closed');
            }
        }
        elsif ($type eq 'websocket.keepalive') {
            return unless _ws_handshake_accepted($weak_self->{h1_seq});

            my $interval = $event->{interval} // 0;
            my $timeout = $event->{timeout};

            if ($interval > 0) {
                $weak_self->_start_ws_keepalive($interval, $timeout);
            }
            else {
                $weak_self->_stop_ws_keepalive;
            }
        }

        return;
    };
}

sub _process_websocket_frames {
    my ($self) = @_;

    return unless _ws_handshake_accepted($self->{h1_seq});
    return if $self->{closed};

    # Reset WebSocket idle timer on receive activity
    $self->_reset_ws_idle_timer;

    my $frame = $self->{websocket_frame};

    # Append buffer to frame parser
    $frame->append($self->{buffer});
    $self->{buffer} = '';

    # Process all complete frames - use next_bytes to get raw bytes
    # Protocol::WebSocket::Frame->next() decodes as UTF-8, which corrupts binary data
    while (defined(my $bytes = $frame->next_bytes)) {
        my $opcode = $frame->opcode;

        # RFC 6455 Section 5.2: RSV1-3 MUST be 0 unless extension defines meaning
        # PAGI doesn't support compression extensions, so RSV must always be 0
        my $rsv = $frame->rsv;
        if ($rsv && ref($rsv) eq 'ARRAY') {
            if (grep { $_ } @$rsv) {
                $self->_send_close_frame(1002, 'RSV bits must be 0');
                $self->_handle_disconnect_and_close('protocol_error', detail => 'RSV bits must be 0');
                return;
            }
        }

        # RFC 6455 Section 5.2: Opcodes 3-7 and 11-15 (0xB-0xF) are reserved
        # Must fail connection with 1002 Protocol Error
        if (($opcode >= 3 && $opcode <= 7) || ($opcode >= 11 && $opcode <= 15)) {
            $self->_send_close_frame(1002, 'Reserved opcode');
            $self->_handle_disconnect_and_close('protocol_error', detail => 'Reserved opcode');
            return;
        }

        # RFC 6455 Section 5.5: Control frames (close/ping/pong) MUST have
        # payload length <= 125 bytes
        if (($opcode == 8 || $opcode == 9 || $opcode == 10) && length($bytes) > 125) {
            $self->_send_close_frame(1002, 'Control frame too large');
            $self->_handle_disconnect_and_close('protocol_error', detail => 'Control frame too large');
            return;
        }

        if ($opcode == 1) {
            # Text frame - decode as UTF-8
            my $text = eval { Encode::decode('UTF-8', $bytes, Encode::FB_CROAK) };
            unless (defined $text) {
                # Invalid UTF-8 - close with 1007 per RFC 6455
                $self->_send_close_frame(1007, 'Invalid UTF-8');
                $self->_handle_disconnect_and_close('protocol_error', detail => 'Invalid UTF-8');
                return;
            }
            # Check queue limit before adding (DoS protection)
            if (@{$self->{receive_queue}} >= $self->{max_receive_queue}) {
                $self->_send_close_frame(1008, 'Message queue overflow');
                $self->_handle_disconnect_and_close('queue_overflow',
                    detail => "inbound message queue at $self->{max_receive_queue}");
                return;
            }
            push @{$self->{receive_queue}}, {
                type => 'websocket.receive',
                text => $text,
            };
        }
        elsif ($opcode == 2) {
            # Binary frame - keep as raw bytes
            # Check queue limit before adding (DoS protection)
            if (@{$self->{receive_queue}} >= $self->{max_receive_queue}) {
                $self->_send_close_frame(1008, 'Message queue overflow');
                $self->_handle_disconnect_and_close('queue_overflow',
                    detail => "inbound message queue at $self->{max_receive_queue}");
                return;
            }
            push @{$self->{receive_queue}}, {
                type  => 'websocket.receive',
                bytes => $bytes,
            };
        }
        elsif ($opcode == 8) {
            # Close frame
            $self->{close_received} = 1;
            my ($code, $reason) = (1005, '');

            # RFC 6455 Section 5.5.1: Close frame payload is 0 or >=2 bytes
            # 1 byte is invalid
            if (length($bytes) == 1) {
                $self->_send_close_frame(1002, 'Invalid close frame');
                $self->_handle_disconnect_and_close('protocol_error', detail => 'Invalid close frame');
                return;
            }

            if (length($bytes) >= 2) {
                $code = unpack('n', substr($bytes, 0, 2));
                $reason = substr($bytes, 2) // '';

                # RFC 6455 Section 7.4.1: Validate close code
                # Valid codes: 1000-1003, 1007-1011, 3000-4999
                # Invalid: 0-999, 1004-1006, 1012-2999, 5000+
                my $valid_code = 0;
                if ($code == 1000 || $code == 1001 || $code == 1002 || $code == 1003) {
                    $valid_code = 1;
                }
                elsif ($code >= 1007 && $code <= 1011) {
                    $valid_code = 1;
                }
                elsif ($code >= 3000 && $code <= 4999) {
                    $valid_code = 1;
                }
                unless ($valid_code) {
                    $self->_send_close_frame(1002, 'Invalid close code');
                    $self->_handle_disconnect_and_close('protocol_error', detail => 'Invalid close code');
                    return;
                }

                # RFC 6455: Close reason must be valid UTF-8
                if (length($reason) > 0) {
                    my $reason_copy = $reason;
                    my $decoded = eval { Encode::decode('UTF-8', $reason_copy, Encode::FB_CROAK) };
                    unless (defined $decoded) {
                        $self->_send_close_frame(1007, 'Invalid UTF-8 in close reason');
                        $self->_handle_disconnect_and_close('protocol_error', detail => 'Invalid UTF-8 in close reason');
                        return;
                    }
                }
            }

            # If we haven't sent close yet, send it now
            if (!$self->{close_sent}) {
                my $close_frame = Protocol::WebSocket::Frame->new(
                    type   => 'close',
                    buffer => pack('n', $code) . $reason,
                );
                $self->{stream}->write($close_frame->to_bytes);
                $self->{close_sent} = 1;
            }

            # Kept as well as queued: this event names the peer's own close
            # code and its own reason text, which the ending record cannot
            # spell, so a receive() made after the queued copy was drained is
            # answered from here (see _ws_disconnect_event).
            $self->{ws_disconnect_event} = {
                type   => 'websocket.disconnect',
                code   => $code,
                reason => $reason,
            };
            push @{$self->{receive_queue}}, { %{ $self->{ws_disconnect_event} } };

            # A completed closing handshake is a clean end regardless of the
            # peer's close code (Www.pod "Meaning per scope"); consumed by
            # _handle_websocket_request's tail. Receive-side, so the
            # validator never sees it, and deliberately narrower than
            # close_received: that is set before this frame has been
            # validated, and an invalid Close frame is a protocol error,
            # not a completed handshake.
            $self->{ws_peer_closed} = 1;

            # This is the scope's one and only websocket.disconnect: mark
            # disconnect-handled now (the same guard _handle_disconnect
            # checks) so a later TCP close (on_closed -> client_closed) or
            # the app's own session-complete teardown finds the guard
            # already set and delivers nothing further. Without this, either
            # of those paths queues a second, ghost disconnect event that
            # nobody asked for and nobody drains (h2's equivalent guard is
            # ws_disconnect_delivered / _h2_ws_enqueue_disconnect).
            $self->{_disconnect_handled} = 1;

            # Mark the scope's connection state complete now, at the moment the
            # closing handshake finishes, rather than waiting for the app to
            # return. Www.pod ("Observing the end of a scope") requires the
            # terminal notification to reach the scope on every ending, without
            # the app draining the queue -- an accepted app that parks on
            # unrelated work and never calls receive() again must still see its
            # on_complete fire. This mirrors the app-return tail's own
            # ws_peer_closed-driven _mark_complete; _mark_complete is
            # idempotent, so that tail stays a correct backstop and nothing
            # double-fires. h2 marks its ConnectionState independently in the
            # same way (_h2_on_close). A pre-accept Close ends no accepted scope,
            # so the mark is gated on the handshake having been accepted.
            $self->{current_connection_state}->_mark_complete
                if $self->{current_connection_state}
                && _ws_handshake_accepted($self->{h1_seq});
        }
        elsif ($opcode == 9) {
            # Ping - respond with pong (transparent to app)
            my $pong = Protocol::WebSocket::Frame->new(
                type   => 'pong',
                buffer => $bytes,
            );
            $self->{stream}->write($pong->to_bytes);
        }
        elsif ($opcode == 10) {
            # Pong - cancel any pending timeout (response to our ping)
            $self->_cancel_ws_pong_timeout;
        }
    }

    # Notify any waiting receive
    if ($self->{receive_pending} && !$self->{receive_pending}->is_ready && @{$self->{receive_queue}}) {
        my $f = $self->{receive_pending};
        $self->{receive_pending} = undef;
        $f->done;
    }
}

# Async file response - prioritizes speed based on file size:
#   1. Small files (<=64KB): direct in-process read (fastest for small files)
#   2. Large files: async chunked reads via worker pool (non-blocking)
async sub _send_file_response {
    my ($self, $file, $offset, $length, $chunked) = @_;

    # Get file size if length not specified
    my $file_size = -s $file;
    die "Cannot stat file $file: $!\n" unless defined $file_size;
    $length //= $file_size - $offset;

    # PAGI spec (Www.pod, Response Body validation): an offset past the end
    # of the file SHOULD send zero bytes rather than fail the response.
    $length = 0 if $length < 0;

    $self->{_response_size} += $length;

    my $stream = $self->{stream};

    if ($self->{sync_file_threshold} > 0 && $length <= $self->{sync_file_threshold}) {
        # Small file fast path: read directly in-process
        # For files <= 64KB, a simple read() is fast and avoids async overhead
        open my $fh, '<:raw', $file or die "Cannot open file $file: $!";
        seek($fh, $offset, 0) if $offset;
        my $bytes_read = read($fh, my $data, $length);
        close $fh;

        die "Failed to read file $file: $!" unless defined $bytes_read;

        if ($chunked) {
            # A zero-length body IS the terminator chunk -- writing a
            # separate empty data chunk before it would send "0\r\n\r\n"
            # twice and desync the connection.
            if (length($data)) {
                my $len = sprintf("%x", length($data));
                $stream->write("$len\r\n$data\r\n");
            }
            $stream->write("0\r\n\r\n");
        }
        else {
            $stream->write($data);
        }
    }
    else {
        # Large file path: async chunked reads via worker pool
        my $loop = $self->{server} ? $self->{server}->loop : undef;
        die "No event loop available for async file I/O" unless $loop;

        await PAGI::Server::AsyncFile->read_file_chunked(
            $loop, $file,
            sub {
                my ($chunk) = @_;
                if ($chunked) {
                    my $len = sprintf("%x", length($chunk));
                    $stream->write("$len\r\n$chunk\r\n");
                }
                else {
                    $stream->write($chunk);
                }
                return;  # Sync callback
            },
            offset     => $offset,
            length     => $length,
            chunk_size => FILE_CHUNK_SIZE,
        );

        # Send final chunk terminator if chunked
        if ($chunked) {
            $stream->write("0\r\n\r\n");
        }
    }
}

# Async filehandle response - synchronous chunked reads in the send loop
# (the fh can't cross a fork into the worker pool; see below).
# Note: Can't easily use sendfile for arbitrary filehandles (may not have fd,
# may be pipes, may be in-memory). Falls back to chunked reads.
async sub _send_fh_response {
    my ($self, $fh, $offset, $length, $chunked) = @_;

    # Seek to offset if specified
    if ($offset && $offset > 0) {
        seek($fh, $offset, 0) or die "Cannot seek: $!";
    }

    # For filehandles, we can't easily use the worker pool (can't pass fh across fork).
    # Use blocking reads in small chunks - not ideal but practical.
    # TODO: Consider IO::Async::FileStream for better event loop integration.

    my $remaining = $length;  # undef means read to EOF
    my $stream = $self->{stream};

    while (1) {
        my $to_read = FILE_CHUNK_SIZE;
        if (defined $remaining) {
            $to_read = $remaining if $remaining < $to_read;
            last if $to_read <= 0;
        }

        my ($bytes_read, $chunk);
        {
            no warnings 'closed';
            $bytes_read = read($fh, $chunk, $to_read);
        }

        die "Failed to read filehandle: $!\n" unless defined $bytes_read;
        last if $bytes_read == 0;      # EOF

        $self->{_response_size} += $bytes_read;

        if ($chunked) {
            my $len = sprintf("%x", length($chunk));
            $stream->write("$len\r\n$chunk\r\n");
        }
        else {
            $stream->write($chunk);
        }

        if (defined $remaining) {
            $remaining -= $bytes_read;
        }
    }

    # Send final chunk if chunked encoding
    if ($chunked) {
        $stream->write("0\r\n\r\n");
    }
}

# Diagnostics go through the server so log_level governs them and a replaced
# sink sees them. A connection can outlive its server reference during
# shutdown, so falling back to STDERR is a real path, not a formality.
sub _log {
    my ($self, $level, $msg) = @_;

    my $server = $self->{server};
    return $server->_log($level, $msg, __PACKAGE__) if $server;

    warn "$msg\n";
    return;
}

1;

__END__

=head1 SSE OVER HTTP/2

SSE events (C<sse.start>, C<sse.send>, C<sse.comment>, C<sse.keepalive>)
work transparently over both HTTP/1.1 and HTTP/2. Applications do not need
to change their SSE handling code based on protocol version.

=head2 How It Works

A request is detected as SSE when its combined C<Accept> header values
contain the exact media range C<text/event-stream>, case-insensitively,
with an effective quality value greater than zero (see L<PAGI::Spec::Www/
"SSE Connection Detection">); a C<q=0> refusal or a wildcard range such as
C<*/*> never signals SSE. Detection works identically regardless of HTTP
version. Over HTTP/1.1, SSE data is sent using chunked Transfer-Encoding.
Over HTTP/2, SSE data is sent as DATA frames via the
C<submit_response_streaming>/C<data_callback> mechanism. This difference is
transparent to the application.

The C<http_version> field in the scope hash will be C<'2'> for HTTP/2
connections, allowing applications to distinguish if needed.

=head2 SSE Idle Timeout over HTTP/2

The C<sse_idle_timeout> setting is enforced B<per stream> on HTTP/2: each
SSE stream owns its own idle timer, armed when that stream's
C<sse.start> is sent and reset by that stream's own send activity
(C<sse.send>, C<sse.comment>, C<sse.keepalive>, C<sse.close>). When a
stream's timer expires, only that stream ends -- the server marks the
stream closing, lets it flush any already-queued data, and then emits
the final HTTP/2 END_STREAM frame, the same path an application-initiated
C<sse.close> takes. Sibling SSE (and other) streams multiplexed on the
same HTTP/2 connection are unaffected, and the connection itself stays
open.

Over HTTP/1.1, each SSE stream already owns its own TCP connection, so
C<sse_idle_timeout> is enforced at the connection level there -- expiry
closes that connection, which only ever carries the one SSE stream.

=head2 Connection Reuse after an SSE Stream (HTTP/1.1)

C<sse.start> advertises C<Connection: keep-alive>, and the server honors it.
An HTTP/1.1 SSE stream ends B<cleanly> only when the application sends
C<sse.close>; a plain return without it leaves the response incomplete. On
C<sse.close> the server writes the chunked terminator, resets the per-request
state the stream accumulated, and hands the connection back to ordinary
keep-alive request handling, including serving any request already pipelined
in the read buffer. A pooled client (browser, C<Net::Async::HTTP>, curl) can
therefore reuse the same socket for its next request, which matters for the
short POST-SSE-exchange pattern used by fetch-event-source and datastar.

Keep-alive yields to the usual overrides, each of which closes the connection
the same way it does outside SSE: a client C<Connection: close>, HTTP/1.0
semantics, server shutdown, an application exception, a completed refusal, and
any B<abnormal> end (client disconnect, idle timeout, write error, C<abort>,
or a stream abandoned without C<sse.close>). An abnormal end is also the only
thing that delivers C<sse.disconnect> to the application; a clean end never
does.

Ending the stream is decoupled from the application returning. After
C<sse.close> the application keeps running against a live transport, and any
further send on that scope fails through the event sequence machine (C<after
sse.close>) rather than being silently swallowed by a closed transport.

=head1 SEE ALSO

L<PAGI::Server>, L<PAGI::Server::Protocol::HTTP1>

=head1 AUTHOR

John Napiorkowski E<lt>jjnapiork@cpan.orgE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
