package PAGI::Server::Connection;
use strict;
use warnings;

our $VERSION = '0.002011';

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
            warn "PAGI: connection-specific header '$name' stripped from HTTP/2 $context (RFC 9113)\n";
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
    my ($headers) = @_;
    my @kept;
    for my $h (@$headers) {
        my ($name, $value) = @$h;
        if ($H1_CONNECTION_SPECIFIC_HEADER{lc $name}) {
            warn "PAGI: connection-specific header '$name' stripped from HTTP/1.1 response\n";
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
        access_log        => $args{access_log},     # Filehandle for access logging
        _access_log_formatter => $args{_access_log_formatter},  # Pre-compiled format closure
        max_receive_queue => $args{max_receive_queue} // 1000,  # Max WebSocket receive queue size
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
        h1_seq        => 'initial',  # Mirrors the h1 send closure's $seq (see _create_send)
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
        websocket_mode    => 0,
        websocket_frame   => undef,  # Protocol::WebSocket::Frame for parsing
        websocket_accepted => 0,
        # SSE state
        sse_mode          => 0,
        sse_started       => 0,
        sse_disconnect_reason => undef,  # Reason for SSE disconnect (client_closed, write_error, etc.)
        ws_disconnect_reason  => undef,  # Standard reason token for the app-facing websocket.disconnect event
        ws_disconnect_code    => undef,  # Wire close code for that event (defaults to 1006, abnormal closure)
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
                if ($weak_self->{websocket_mode}) {
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
                warn "PAGI connection error: $error";
                return 0 unless $weak_self;
                $weak_self->_handle_disconnect_and_close('server_error');
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
            $weak_self->_handle_disconnect_and_close('read_error');
        },
        on_write_error => sub {
            my ($s, $errno) = @_;
            return unless $weak_self;
            $weak_self->_handle_disconnect_and_close('write_error');
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
            $weak_self->_handle_disconnect_and_close($reason);
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
        on_header_overflow => sub {
            my ($stream_id) = @_;
            return unless $weak_self;
            $weak_self->_h2_on_header_overflow($stream_id);
        },
    );

    # Send initial SETTINGS to client
    $self->_h2_write_pending;
}

sub _h2_process_data {
    my ($self) = @_;
    return unless $self->{h2_session};

    if (length($self->{buffer}) > 0) {
        $self->{h2_session}->feed($self->{buffer});
        $self->{buffer} = '';
    }

    $self->_h2_write_pending;

    # Close connection when session is done (GOAWAY received or sent)
    if ($self->{h2_session} && !$self->{h2_session}->want_read) {
        $self->_h2_write_pending;  # Flush any remaining output
        $self->_handle_disconnect_and_close;
    }
}

sub _h2_write_pending {
    my ($self) = @_;
    return unless $self->{h2_session};
    while (1) {
        my $data = $self->{h2_session}->extract;
        last unless defined $data && length($data) > 0;
        $self->{stream}->write($data);
    }
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
# Mirrors the plain-CONNECT-501 and content-length-413 idiom below: defer
# the response to avoid re-entrant nghttp2 calls (we're inside feed/mem_recv).
sub _h2_on_header_overflow {
    my ($self, $stream_id) = @_;

    weaken(my $ws = $self);
    $self->{server}->loop->later(sub {
        return unless $ws;
        return if $ws->{closed};
        $ws->{h2_session}->submit_response($stream_id,
            status  => 431,
            headers => [
                ['content-type', 'text/plain'],
                ['date', $ws->{protocol}->format_date],
            ],
            body    => "Request Header Fields Too Large\n",
        );
        $ws->_h2_write_pending;
    });
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
        warn "PAGI::Server::Connection: ignoring duplicate HTTP/2 request dispatch for stream $stream_id (existing in-flight request would have been destroyed)\n";
        return;
    }

    # Detect CONNECT method
    my $is_websocket = 0;
    if (($pseudo->{':method'} // '') eq 'CONNECT') {
        if (($pseudo->{':protocol'} // '') eq 'websocket') {
            # Extended CONNECT for WebSocket (RFC 8441)
            $is_websocket = 1;
        } else {
            # Plain CONNECT not supported — defer response to avoid
            # re-entrant nghttp2 calls (we're inside feed/mem_recv)
            weaken(my $ws = $self);
            $self->{server}->loop->later(sub {
                return unless $ws;
                return if $ws->{closed};
                $ws->{h2_session}->submit_response($stream_id,
                    status  => 501,
                    headers => [
                        ['content-type', 'text/plain'],
                        ['date', $ws->{protocol}->format_date],
                    ],
                    body    => "CONNECT method not supported\n",
                );
                $ws->_h2_write_pending;
            });
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
        seq_state => 'initial',
        is_websocket => $is_websocket,
        is_sse       => $is_sse,
        ws_accepted  => 0,
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
                    # Defer response to avoid re-entrant nghttp2 calls
                    weaken(my $ws = $self);
                    $self->{server}->loop->later(sub {
                        return unless $ws;
                        return if $ws->{closed};
                        $ws->{h2_session}->submit_response($stream_id,
                            status  => 413,
                            headers => [
                                ['content-type', 'text/plain'],
                                ['date', $ws->{protocol}->format_date],
                            ],
                            body    => "Payload Too Large\n",
                        );
                        $ws->_h2_write_pending;
                    });
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
        $weak_self->_h2_dispatch_stream($stream_id);
    });
}

sub _h2_on_body {
    my ($self, $stream_id, $data, $eof) = @_;

    my $stream = $self->{h2_streams}{$stream_id};
    return unless $stream;

    if ($stream->{is_websocket} && $stream->{ws_accepted}) {
        # WebSocket: DATA frames contain raw WebSocket frames
        $self->_h2_process_ws_frames($stream_id, $stream, $data) if length($data);

        if ($eof) {
            # END_STREAM with no close handshake (bare END_STREAM): abnormal
            # closure per RFC 6455 (Www.pod "Disconnect - receive event").
            $self->_h2_ws_enqueue_disconnect($stream, 1006, 'client_closed');
        }
        return;
    }

    if (length($data) > 0) {
        $stream->{body} .= $data;

        # Enforce max_body_size (0 = unlimited)
        if ($self->{max_body_size} && length($stream->{body}) > $self->{max_body_size}) {
            $self->{h2_session}->submit_response($stream_id,
                status  => 413,
                headers => [
                    ['content-type', 'text/plain'],
                    ['date', $self->{protocol}->format_date],
                ],
                body    => 'Payload Too Large',
            );
            # No _h2_write_pending here — we're inside feed(); _h2_process_data
            # flushes after feed() returns
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
            # event, drive connection_state (http scopes only -- sse/ws
            # scopes never attach one, per spec), then wake body_pending.
            # Order matters: the queued event must land BEFORE the wake so a
            # parked receive() sees it (both h2 receive closures check the
            # queue first on resume), and all of this must happen BEFORE the
            # delete below, after which the stream state is unreachable.
            $stream->{body_complete} = 1;
            if ($stream->{is_sse}) {
                push @{$stream->{receive_queue}}, {
                    type   => 'sse.disconnect',
                    reason => 'body_too_large',
                };
            } elsif ($stream->{is_websocket}) {
                # An accepted ws stream returns early at the top of this sub
                # (the ws_accepted guard above), so is_websocket true here
                # always means pre-accept. Deliver the scope's single
                # websocket.disconnect (Www.pod "Disconnect - receive
                # event": server-detected abnormal close is code 1006 plus
                # the matching Standard Disconnect Reasons token, and
                # body_too_large is one). Pushed directly rather than
                # through _h2_ws_enqueue_disconnect: that helper also calls
                # _h2_wake_pending, but the wake here must wait until
                # h2_closed is set below (same ordering the sse/http arms
                # rely on -- see the comment above h2_closed). Still honor
                # the single-delivery contract so a later delivery attempt
                # for this (about-to-be-deleted) stream is a no-op.
                $stream->{ws_disconnect_delivered}++;
                push @{$stream->{receive_queue}}, {
                    type   => 'websocket.disconnect',
                    code   => 1006,
                    reason => 'body_too_large',
                };
            } else {
                push @{$stream->{receive_queue}}, { type => 'http.disconnect' };
            }
            $stream->{connection_state}->_mark_disconnected('body_too_large')
                if $stream->{connection_state};
            # Mark death BEFORE the wake, mirroring _h2_on_close's own
            # h2_closed marker (see its comment): the wake below can resume
            # the app's async sub synchronously, and a resumed app may call
            # $send before this function reaches its own delete further
            # down -- $ss would still be a live h2_streams entry at that
            # point (STREAM_GONE / already-closed carve-outs key off entry
            # existence). h2_closed lets every send closure recognize a
            # doomed-but-still-present entry and no-op on it, same as an
            # absent one, per the post-close contract (design §6.2 / §21
            # item 1).
            $stream->{h2_closed} = 1;
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

# Enqueue the scope's single websocket.disconnect event (PAGI: exactly one
# per WebSocket scope). Every h2 delivery site MUST come through here.
sub _h2_ws_enqueue_disconnect {
    my ($self, $stream, $code, $reason) = @_;
    return if $stream->{ws_disconnect_delivered}++;
    push @{$stream->{receive_queue}}, {
        type   => 'websocket.disconnect',
        code   => $code,
        reason => $reason,
    };
    $self->_h2_wake_pending($stream);
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
    # server_close_reason: a server-initiated per-stream teardown (idle
    # timeout, keepalive timeout, ...) records its own token here BEFORE
    # driving the close, so it takes precedence over the generic
    # 'client_closed' fallback below in the queued disconnect events pushed
    # further down -- that part is operative today, since WebSocket
    # keepalive timeout and SSE idle timeout are exactly the writers of this
    # token and both stream types push a queued disconnect. The
    # nonzero-error-code $cs mark below also consults $reason, but that arm
    # is a forward guard, not yet operative: WebSocket/SSE streams (this
    # token's only current writers) never attach a connection_state, so no
    # $cs-bearing stream sets server_close_reason today -- this exists for
    # when a server-initiated per-stream teardown reaches a stream that does
    # carry one. The zero-error-code arms above are untouched either way:
    # they distinguish a clean completion from an incomplete response, not
    # a server-initiated abnormal close, so they never consult this token.
    my $reason = $stream->{server_close_reason};
    if (my $cs = $stream->{connection_state}) {
        if (($stream->{seq_state} // '') eq 'complete' && !$error_code) {
            $cs->_mark_complete;
        } elsif (!$error_code) {
            $cs->_mark_disconnected('server_error');
        } else {
            $cs->_mark_disconnected($reason // 'client_closed');
        }
    }

    # Mark body complete to unblock any pending receive
    $stream->{body_complete} = 1;

    # Enqueue disconnect event
    if ($stream->{is_websocket}) {
        # Close without a WebSocket close handshake (RST_STREAM, timeout, ...):
        # abnormal closure per RFC 6455. Deduped -- a no-op if the close-frame
        # or bare-END_STREAM path already delivered the scope's one disconnect.
        $self->_h2_ws_enqueue_disconnect($stream, 1006, $reason // 'client_closed');
    } elsif ($stream->{is_sse}) {
        push @{$stream->{receive_queue}}, {
            type   => 'sse.disconnect',
            reason => $reason // 'client_closed',
        };
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
            # Streams with no connection_state (WebSocket/SSE) fall back to
            # a liveness fact ($stream_alive below) that is true if and
            # only if nghttp2 still owns the stream.
            #
            # A recorded reason of 'server_error' is NOT a client-gone signal:
            # it means the SERVER caused the abnormal end (e.g. this very
            # dispatch wrapper's own incomplete-response branch below marks
            # server_error before resetting the stream with RST_STREAM
            # INTERNAL_ERROR for a response left started-but-unfinished,
            # including a trailers-declared response that never sent its
            # trailers). Per the PAGI spec's incomplete-response section, the
            # log carve-out exists only for "the client had already
            # disconnected" -- so server-caused ends must still warn.
            #
            # $stream_alive is entry-exists-AND-not-yet-closed, not merely
            # entry-exists: _h2_on_close marks $stream->{h2_closed} the
            # instant it runs, but its h2_streams delete is deferred one
            # tick (loop->later) so pending futures can resolve. Waking
            # this very Future is one of those pending resolutions -- a
            # receive() blocked on body_pending can be woken by
            # _h2_on_close and resume this app synchronously, landing here
            # before that deferred delete has run. Keying liveness off
            # entry-existence alone (as before) missed exactly that
            # window: nghttp2 had already forgotten the stream, but
            # h2_streams still had it, so the carve-out below saw a "live"
            # stream and went on to call submit_response/submit_rst_stream
            # on a stream id nghttp2 no longer tracks -- undefined
            # behavior at the C level, not something the surrounding eval
            # can catch.
            my $stream_alive = $weak_self->{h2_streams}{$stream_id}
                                && !$weak_self->{h2_streams}{$stream_id}{h2_closed};
            my $reason = $cs ? $cs->disconnect_reason : undef;
            my $client_gone = $cs ? (defined($reason) && $reason ne 'server_error')
                                  : !$stream_alive;

            if ($client_gone) {
                # Nothing to do.
            }
            elsif (!$stream_state->{response_started}) {
                # If the application failed, OR returned without starting a
                # response, synthesize a 500 (only possible while no response
                # has begun). A clean return that produced no response is a
                # protocol error, same as a throw.
                warn $error
                    ? "PAGI application error (HTTP/2 stream $stream_id): $error\n"
                    : "PAGI application returned without starting a response (HTTP/2 stream $stream_id)\n";
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
                $cs->_mark_response_started if $cs;
                $cs->_mark_disconnected('server_error') if $cs;
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
                };
            }
            elsif ($cs && (($stream_state->{seq_state} // 'complete') ne 'complete')) {
                # Response started but never finished: the app either
                # returned cleanly without sending the terminal body/file/fh
                # (or trailers), or threw after starting. Either way the
                # stream is framed but unterminated -- there is no way to
                # synthesize END_STREAM without lying about the body, so
                # reset the stream instead. (Only plain HTTP streams reach
                # here: WebSocket/SSE never attach a connection_state, so
                # $cs guards them out -- their seq_state is never tracked
                # and would otherwise misread as permanently incomplete.)
                # Trailers-specific parenthetical mirrors the h1 wording (see
                # _handle_request's incomplete branch) -- appended after the
                # stream-id parenthetical so t/http2/24-incomplete-response.t's
                # "...incomplete response (HTTP/2 stream $stream_id)" pin
                # still matches as a contiguous substring.
                my $trailers_note = (($stream_state->{seq_state} // '') eq 'awaiting_trailers')
                    ? ' (trailers were declared but never sent)' : '';
                warn $error
                    ? "PAGI application error after response started (HTTP/2 stream $stream_id): $error\n"
                    : "PAGI application returned with an incomplete response (HTTP/2 stream $stream_id)$trailers_note\n";
                # Mark BEFORE the RST. _h2_on_close fires for our own RST
                # too (as an abnormal close, since seq_state never reached
                # 'complete') and would mark this same connection_state
                # 'client_closed' if it got there first. _mark_disconnected
                # is idempotent -- first mark wins -- so marking here first
                # guarantees the app observes server_error.
                $cs->_mark_disconnected('server_error');
                eval {
                    $weak_self->{h2_session}->submit_rst_stream(
                        $stream_id, _h2_rst_error_code());
                    $weak_self->_h2_write_pending;
                };
            }
            elsif ($error) {
                # Response already complete; cannot send a 500 or usefully
                # reset a finished stream. Log only.
                warn "PAGI application error after response started (HTTP/2 stream $stream_id): $error\n";
            }
        }

        # Notify server that request completed (for max_requests tracking)
        $weak_self->{server}->_on_request_complete if $weak_self && $weak_self->{server};
    })->();

    $self->{server}->adopt_future($future);
}

# HTTP/2 error code for the RST_STREAM sent when a response is left started
# but incomplete (see _h2_dispatch_stream above). NGHTTP2_INTERNAL_ERROR
# (RFC 9113 section 7, code 0x2) is exported by Net::HTTP2::nghttp2.
sub _h2_rst_error_code {
    return Net::HTTP2::nghttp2::NGHTTP2_INTERNAL_ERROR();
}

# RST_STREAM error code for a stream the server no longer needs but that is
# nobody's fault -- RFC 9113 section 7 CANCEL. NGHTTP2_CANCEL is exported by
# Net::HTTP2::nghttp2.
sub _h2_rst_cancel_code {
    return Net::HTTP2::nghttp2::NGHTTP2_CANCEL();
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
    );
    # Store on the stream-state so the send path can mark response_started on
    # this stream's own connection object (h2 multiplexes many streams).
    $stream_state->{connection_state} = $connection_state;

    return {
        type         => 'http',
        pagi         => {
            version      => '0.5',
            spec_version => '0.4',
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

    return sub {
        return Future->done({ type => 'http.disconnect' }) unless $weak_self;
        return Future->done({ type => 'http.disconnect' }) if $weak_self->{closed};

        my $ss = $weak_self->{h2_streams}{$stream_id};
        return Future->done({ type => 'http.disconnect' }) unless $ss;

        my $future = (async sub {
            return { type => 'http.disconnect' } unless $weak_self;

            my $ss = $weak_self->{h2_streams}{$stream_id};
            return { type => 'http.disconnect' } unless $ss;

            # Check queue first
            if (@{$ss->{receive_queue}}) {
                return shift @{$ss->{receive_queue}};
            }

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
                # Wait for body data (or, once the request has been fully
                # delivered, for the stream to end)
                if (!$ss->{body_pending}) {
                    $ss->{body_pending} = Future->new;
                }
                await $ss->{body_pending};

                # Re-fetch stream state (may have changed)
                $ss = $weak_self->{h2_streams}{$stream_id};
                return { type => 'http.disconnect' } unless $ss;

                # Check queue after waking -- a queued event wins over the
                # body fallthrough (a close can set body_complete AND queue
                # http.disconnect on the same wake)
                if (@{$ss->{receive_queue}}) {
                    return shift @{$ss->{receive_queue}};
                }

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
    my ($self, $stream_id, $stream_state) = @_;

    weaken(my $weak_self = $self);

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
    # pattern -- then wakes the waiting send() on the next loop tick (never
    # resolve an app Future from inside a native nghttp2 callback -- same
    # discipline as the drain waiters above).
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
        return unless $f;
        $weak_self->{server}->loop->later(sub {
            return if $f->is_ready;
            if ($ok) { $f->done(1) } else { $f->fail($err) }
        });
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
        $ss->{seq_state} = $seq if $ss;
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
    # EOF pending on success. Mirrors h1's D1 advance-then-rollback pattern.
    my $finish_or_rollback_body_send = sub {
        my ($ok, $err, $ss, $seq_before) = @_;
        if (!$ok) {
            return if $err eq $STREAM_GONE;   # client reset: quiet no-op
            $seq = $seq_before;                # recoverable, per contract
            $ss->{seq_state} = $seq if $ss;
            die $err;
        }
        $finish_body_stream->();
    };

    return async sub {
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
            $ss->{seq_state} = $seq if $ss;
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
        # trailers at start. advance-then-rollback (h1 D1 pattern, mirrored
        # from _create_send's chunked-framing check): the sequence machine
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
            $ss->{seq_state} = $seq if $ss;

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
            $trailer_headers = _h2_strip_connection_headers($trailer_headers, 1);

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
                $ss->{seq_state} = $seq if $ss;
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
        $ss->{seq_state} = $seq if $ss;

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
            @response_headers = @{ _h2_strip_connection_headers(\@response_headers) };
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

    return {
        type         => 'websocket',
        pagi         => {
            version      => '0.5',
            spec_version => '0.4',
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
            my %ext = (%{$self->_get_extensions_for_scope}, 'websocket.http.response' => {});
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
    # gone or the connection is closed: 1006/'' by default (RFC 6455
    # abnormal closure, no reason available), but prefers the per-stream
    # server_close_reason token when the stream's own state is still
    # reachable in h2_streams -- a server-initiated teardown (idle timeout,
    # keepalive timeout, ...) records that token there before tearing the
    # stream down, so a receive() racing that teardown still reports why.
    my $fallback_disconnect = sub {
        my $ss = $weak_self && $weak_self->{h2_streams}{$stream_id};
        return {
            type   => 'websocket.disconnect',
            code   => 1006,
            reason => ($ss && $ss->{server_close_reason}) // '',
        };
    };

    return sub {
        return Future->done($fallback_disconnect->())
            unless $weak_self;
        return Future->done($fallback_disconnect->())
            if $weak_self->{closed};

        my $ss = $weak_self->{h2_streams}{$stream_id};
        return Future->done($fallback_disconnect->())
            unless $ss;

        my $future = (async sub {
            return $fallback_disconnect->()
                unless $weak_self;

            my $ss = $weak_self->{h2_streams}{$stream_id};
            return $fallback_disconnect->()
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
                if (@{$ss->{receive_queue}}) {
                    return shift @{$ss->{receive_queue}};
                }

                return $fallback_disconnect->()
                    if $weak_self->{closed};

                if (!$ss->{body_pending}) {
                    $ss->{body_pending} = Future->new;
                }
                await $ss->{body_pending};

                $ss = $weak_self->{h2_streams}{$stream_id};
                return $fallback_disconnect->()
                    unless $ss;
            }
        })->();

        return $future;
    };
}

sub _h2_create_websocket_send {
    my ($self, $stream_id, $stream_state) = @_;

    weaken(my $weak_self = $self);
    my $seq = 'connecting';

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

        # Once the machine has recorded the connection closed or the denial
        # response complete, the machine decides what happens next -- both
        # 'closed' and 'denial_complete' have no idempotent case, so any
        # further send always raises -- not the stream-gone check below.
        # h2_streams entries for a finished stream are reclaimed
        # asynchronously by _h2_on_close (websocket.close itself triggers
        # this via END_STREAM, same mechanism as the h2 HTTP/SSE closures),
        # so by the time a post-close send arrives $ss may already be gone.
        my $already_closed = ($seq eq 'closed' || $seq eq 'denial_complete');

        my $ss = $weak_self->{h2_streams}{$stream_id};
        # A doomed-but-still-present entry (h2_closed set but not yet
        # deleted -- see the 413-overrun branch in _h2_on_body) is treated
        # the same as an absent one: both are post-close sends and must
        # silently no-op, not reach nghttp2 a second time on this stream id.
        return if (!$ss || $ss->{h2_closed}) && !$already_closed;

        return if $weak_self->{closed} && !$already_closed;

        # websocket.http.response is always available on this path (the
        # scope advertises it unconditionally; see
        # _h2_create_websocket_scope), unlike connection-level extensions
        # such as fullflush.
        PAGI::Server::EventValidator::validate_websocket_send(
            $event, { extensions => { %{$weak_self->{extensions}}, 'websocket.http.response' => {} } });
        $seq = PAGI::Server::EventValidator::advance_websocket($seq, $event);

        if ($type eq 'websocket.accept') {
            # A duplicate accept is already rejected by advance_websocket.

            # HTTP/2 WebSocket: respond with 200 (not 101)
            my @headers;
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
            @headers = @{ _h2_strip_connection_headers(\@headers) };

            $ss->{ws_accepted} = 1;
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

            # Process any data that arrived before accept
            if (length($ss->{body}) > 0) {
                my $buffered = $ss->{body};
                $ss->{body} = '';
                $weak_self->_h2_process_ws_frames($stream_id, $ss, $buffered);
            }
        }
        elsif ($type eq 'websocket.send') {
            return unless $ss->{ws_accepted};

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
        elsif ($type eq 'websocket.http.response.start') {
            return if $ss->{ws_accepted};
            return if $ss->{ws_denial_started};
            $ss->{ws_denial_started} = 1;
            $ss->{ws_denial_status}  = $event->{status} // 403;
            $ss->{ws_denial_headers} = [
                map { [_validate_header_name($_->[0]), _validate_header_value($_->[1])] }
                    @{$event->{headers} // []}
            ];
            # RFC 9113 8.2.2 / design 13.3 — strip app-supplied connection,
            # transfer-encoding, etc. before submission.
            $ss->{ws_denial_headers} = _h2_strip_connection_headers($ss->{ws_denial_headers});
            $ss->{ws_denial_body} = '';
        }
        elsif ($type eq 'websocket.http.response.body') {
            return unless $ss->{ws_denial_started};
            return if $ss->{response_started};
            $ss->{ws_denial_body} .= $event->{body} // '';
            return if $event->{more};   # more chunks coming — keep buffering

            $ss->{response_started} = 1;
            unless (grep { lc($_->[0]) eq 'date' } @{$ss->{ws_denial_headers}}) {
                push @{$ss->{ws_denial_headers}}, ['date', $weak_self->{protocol}->format_date];
            }
            $weak_self->{h2_session}->submit_response($stream_id,
                status  => $ss->{ws_denial_status},
                headers => $ss->{ws_denial_headers},
                body    => $ss->{ws_denial_body},
            );
            $weak_self->_h2_write_pending;
        }
        elsif ($type eq 'websocket.close') {
            if (!$ss->{ws_accepted}) {
                # Reject: send 403
                $weak_self->{h2_session}->submit_response($stream_id,
                    status  => 403,
                    headers => [
                        ['content-type', 'text/plain'],
                        ['date', $weak_self->{protocol}->format_date],
                    ],
                    body    => 'Forbidden',
                );
                $weak_self->_h2_write_pending;
                return;
            }

            # Close frame + END_STREAM, and release this stream's keepalive
            # (_h2_ws_close does both). Runs outside feed(), so flush here.
            $weak_self->_h2_ws_close($stream_id,
                $event->{code} // 1000, $event->{reason} // '');
            $weak_self->_h2_write_pending;
        }
        elsif ($type eq 'websocket.keepalive') {
            return unless $ss->{ws_accepted};

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

    return {
        type         => 'sse',
        pagi         => {
            version      => '0.5',
            spec_version => '0.4',
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
        # Per-stream outbound flow-control handle. Like the h2 streaming scope,
        # it measures THIS stream's send queue (h2 multiplexes many streams over
        # one connection, so the shared TCP buffer is meaningless per stream).
        'pagi.transport'  => ($stream_state->{transport_state} = $self->_h2_transport_state($stream_state)),
    };
}

sub _h2_create_sse_receive {
    my ($self, $stream_id, $stream_state) = @_;

    weaken(my $weak_self = $self);

    my $sse_disconnect = sub {
        return {
            type   => 'sse.disconnect',
            reason => 'client_closed',
        };
    };

    return sub {
        return Future->done($sse_disconnect->()) unless $weak_self;
        return Future->done($sse_disconnect->()) if $weak_self->{closed};

        my $ss = $weak_self->{h2_streams}{$stream_id};
        return Future->done($sse_disconnect->()) unless $ss;

        my $future = (async sub {
            return $sse_disconnect->() unless $weak_self;

            my $ss = $weak_self->{h2_streams}{$stream_id};
            return $sse_disconnect->() unless $ss;

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
                    if (@{$ss->{receive_queue}}) {
                        return shift @{$ss->{receive_queue}};
                    }

                    return $sse_disconnect->() if $weak_self->{closed};

                    if (!$ss->{body_pending}) {
                        $ss->{body_pending} = Future->new;
                    }
                    await $ss->{body_pending};

                    $ss = $weak_self->{h2_streams}{$stream_id};
                    return $sse_disconnect->() unless $ss;
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
                if (@{$ss->{receive_queue}}) {
                    return shift @{$ss->{receive_queue}};
                }

                return $sse_disconnect->()
                    if $weak_self->{closed};

                if (!$ss->{body_pending}) {
                    $ss->{body_pending} = Future->new;
                }
                await $ss->{body_pending};

                $ss = $weak_self->{h2_streams}{$stream_id};
                return $sse_disconnect->() unless $ss;
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
        return ('', 1) if $ss->{sse_closing};

        # Queue empty — defer (NGHTTP2_ERR_DEFERRED in the C layer)
        return undef;
    };

    return async sub {
        my ($event) = @_;
        return unless $weak_self;

        my $type = $event->{type} // '';

        # Once the machine has already recorded this stream as closed --
        # either an app-initiated sse.close or a completed decline response
        # -- the machine, not the stream-gone check below, decides what
        # happens next: idempotent no-op for a repeat sse.close, croak for
        # anything else (decline_complete has no idempotent case; it croaks
        # unconditionally). h2_streams entries for a closed stream are
        # reclaimed asynchronously by _h2_on_close, so by the time a
        # post-close send arrives $ss may already be gone.
        my $already_closed = ($seq eq 'closed' || $seq eq 'decline_complete');

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
            @final_headers = @{ _h2_strip_connection_headers(\@final_headers) };
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

            # End THIS HTTP/2 stream now: flush remaining queued events, then the
            # data_callback emits a final END_STREAM frame. `reason` is
            # server-side only and is never written to the wire.
            $ss->{sse_close_sent} = 1;
            $ss->{sse_closing}    = 1;
            $weak_self->{sse_disconnect_reason} = $event->{reason}
                if defined $event->{reason};
            $weak_self->{h2_session}->resume_stream($stream_id);
            $weak_self->_h2_write_pending;
        }
        elsif ($type eq 'sse.http.response.start') {
            # A decline-after-start attempt is already rejected by advance_sse.
            return if $ss->{sse_decline_started};   # idempotent
            $ss->{sse_decline_started} = 1;
            $ss->{sse_decline_status}  = $event->{status} // 200;
            $ss->{sse_decline_headers} = [
                map { [_validate_header_name($_->[0]), _validate_header_value($_->[1])] }
                    @{$event->{headers} // []}
            ];
            # RFC 9113 8.2.2 / design 13.3 — strip app-supplied connection,
            # transfer-encoding, etc. before submission.
            $ss->{sse_decline_headers} = _h2_strip_connection_headers($ss->{sse_decline_headers});
            $ss->{sse_decline_body} = '';
        }
        elsif ($type eq 'sse.http.response.body') {
            return unless $ss->{sse_decline_started};
            return if $ss->{response_started};
            $ss->{sse_decline_body} .= $event->{body} // '';
            return if $event->{more};   # more chunks coming — keep buffering

            # Defensive: the send-sequence state machine never permits
            # sse.keepalive before sse.start, so neither timer can actually be
            # armed here -- stopped anyway for the same every-closure-path
            # discipline the other SSE/WS stop sites follow.
            $weak_self->_h2_stop_sse_keepalive($ss);
            $weak_self->_h2_stop_sse_idle_timer($ss);

            $ss->{response_started} = 1;
            unless (grep { lc($_->[0]) eq 'date' } @{$ss->{sse_decline_headers}}) {
                push @{$ss->{sse_decline_headers}}, ['date', $weak_self->{protocol}->format_date];
            }
            $weak_self->{h2_session}->submit_response($stream_id,
                status  => $ss->{sse_decline_status},
                headers => $ss->{sse_decline_headers},
                body    => $ss->{sse_decline_body},
            );
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
                $self->_h2_ws_close($stream_id, 1002, 'RSV bits must be 0');
                # Server-initiated protocol close (Www.pod: RFC code + 'protocol_error').
                $self->_h2_ws_enqueue_disconnect($stream, 1002, 'protocol_error');
                return;
            }
        }

        # RFC 6455 Section 5.2: Opcodes 3-7 and 11-15 (0xB-0xF) are reserved.
        # Must fail connection with 1002 Protocol Error.
        if (($opcode >= 3 && $opcode <= 7) || ($opcode >= 11 && $opcode <= 15)) {
            $self->_h2_ws_close($stream_id, 1002, 'Reserved opcode');
            # Server-initiated protocol close (Www.pod: RFC code + 'protocol_error').
            $self->_h2_ws_enqueue_disconnect($stream, 1002, 'protocol_error');
            return;
        }

        # RFC 6455 Section 5.5: Control frames (close/ping/pong) MUST have
        # payload length <= 125 bytes.
        if (($opcode == 8 || $opcode == 9 || $opcode == 10) && length($bytes) > 125) {
            $self->_h2_ws_close($stream_id, 1002, 'Control frame too large');
            # Server-initiated protocol close (Www.pod: RFC code + 'protocol_error').
            $self->_h2_ws_enqueue_disconnect($stream, 1002, 'protocol_error');
            return;
        }

        if ($opcode == 1) {
            # Text frame
            my $text = eval { Encode::decode('UTF-8', $bytes, Encode::FB_CROAK) };
            unless (defined $text) {
                $self->_h2_ws_close($stream_id, 1007, 'Invalid UTF-8');
                # Server-initiated protocol close (Www.pod: RFC code + 'protocol_error').
                $self->_h2_ws_enqueue_disconnect($stream, 1007, 'protocol_error');
                return;
            }
            # Check queue limit before adding (DoS protection) -- same cap
            # and pairing h1 enforces (Www.pod pins 1008 <-> queue_overflow).
            if (@{$stream->{receive_queue}} >= $self->{max_receive_queue}) {
                $self->_h2_ws_close($stream_id, 1008, 'Message queue overflow');
                $self->_h2_ws_enqueue_disconnect($stream, 1008, 'queue_overflow');
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
                $self->_h2_ws_close($stream_id, 1008, 'Message queue overflow');
                $self->_h2_ws_enqueue_disconnect($stream, 1008, 'queue_overflow');
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
                $self->_h2_ws_close($stream_id, 1002, 'Invalid close frame');
                # Server-initiated protocol close (Www.pod: RFC code + 'protocol_error').
                $self->_h2_ws_enqueue_disconnect($stream, 1002, 'protocol_error');
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
                    $self->_h2_ws_close($stream_id, 1002, 'Invalid close code');
                    # Server-initiated protocol close (Www.pod: RFC code + 'protocol_error').
                    $self->_h2_ws_enqueue_disconnect($stream, 1002, 'protocol_error');
                    return;
                }

                # RFC 6455: Close reason must be valid UTF-8
                if (length($reason) > 0) {
                    my $reason_copy = $reason;
                    my $decoded = eval { Encode::decode('UTF-8', $reason_copy, Encode::FB_CROAK) };
                    unless (defined $decoded) {
                        $self->_h2_ws_close($stream_id, 1007, 'Invalid UTF-8 in close reason');
                        # Server-initiated protocol close (Www.pod: RFC code + 'protocol_error').
                        $self->_h2_ws_enqueue_disconnect($stream, 1007, 'protocol_error');
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
            # No _h2_write_pending — inside feed(); flushed by _h2_process_data

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
            # No _h2_write_pending — inside feed(); flushed by _h2_process_data
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
# Callers inside feed() need no flush (_h2_process_data flushes after
# feed() returns); callers outside it must call _h2_write_pending
# themselves.
sub _h2_ws_close {
    my ($self, $stream_id, $code, $reason) = @_;

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
    # instead of the one the wire is supposed to see.
    return if $ss->{ws_eof_pending};

    $self->_h2_stop_ws_keepalive($ss);

    # Queued (not submit_data) so the data_callback's own eof_pending merge
    # puts END_STREAM on this exact chunk, same as the previous
    # submit_data(..., 1) did.
    my $frame = Protocol::WebSocket::Frame->new(
        type   => 'close',
        buffer => pack('n', $code) . ($reason // ''),
    );
    my $bytes = $frame->to_bytes;
    push @{$ss->{send_queue} ||= []}, $bytes;
    $ss->{send_queue_bytes} = ($ss->{send_queue_bytes} // 0) + length $bytes;
    $ss->{ws_eof_pending} = 1;
    $self->{h2_session}->resume_stream($stream_id);
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
            return unless $weak_ss->{ws_accepted};

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

                # Record why this stream is ending BEFORE the teardown: this
                # is a server-initiated close, not the client going away, and
                # _h2_on_close prefers this token over its hardcoded fallback
                # for the $cs marking (the queued disconnect event below is
                # already correct via the first-wins dedup, but the $cs mark
                # only happens in _h2_on_close, downstream of this RST).
                $weak_ss->{server_close_reason} = 'keepalive_timeout';

                # RFC 6455 section 7.4.1: 1006 MUST NOT be set as the status
                # code of a Close control frame -- it means "the connection
                # dropped with no close handshake". So a keepalive timeout
                # does not send a Close frame at all: it tears the stream
                # down with RST_STREAM, the HTTP/2 analogue of h1 dropping
                # the transport. The app still sees 1006/'keepalive_timeout'.
                #
                # Order dependency: the enqueue MUST precede the flush. The
                # flush is the only call here that can synchronously drive
                # on_stream_close (which enqueues the generic 1006/
                # 'client_closed'), and the enqueue is first-wins-deduped, so
                # enqueueing first is what makes 'keepalive_timeout' the
                # reason the app observes. Do not hoist _h2_write_pending.
                $weak_self->_h2_ws_enqueue_disconnect($weak_ss, 1006, 'keepalive_timeout');
                $weak_self->{h2_session}->submit_rst_stream(
                    $stream_id, _h2_rst_cancel_code());
                # This runs outside feed() (async timer callback), so flush
                # explicitly.
                $weak_self->_h2_write_pending;
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
            return if $weak_ss->{sse_closing};   # already ending some other way

            if ($weak_self->{server} && $weak_self->{server}->can('_log')) {
                $weak_self->{server}->_log(warn =>
                    "HTTP/2 SSE stream $stream_id idle timeout ($weak_self->{sse_idle_timeout}s) - closing stream");
            }

            $weak_self->_h2_stop_sse_keepalive($weak_ss);
            $weak_self->_h2_stop_sse_idle_timer($weak_ss);

            # Record why this stream is ending BEFORE the teardown: this is a
            # server-initiated close, not the client going away, and
            # _h2_on_close prefers this token over its hardcoded fallback for
            # both the $cs marking and the queued sse.disconnect event.
            $weak_ss->{server_close_reason} = 'idle_timeout';

            # End THIS stream only, the same way an app-initiated sse.close
            # does: mark it closing so the data_callback emits the final
            # END_STREAM frame once the queue drains. _h2_on_close delivers
            # the app-facing sse.disconnect once the h2 layer reports the
            # stream fully closed -- sibling streams on this connection are
            # unaffected.
            $weak_ss->{sse_close_sent} = 1;
            $weak_ss->{sse_closing}    = 1;
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
            $weak_self->_handle_disconnect_and_close('client_timeout');
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
            $weak_self->_handle_disconnect_and_close('idle_timeout');
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
            $weak_self->{sse_disconnect_reason} = 'idle_timeout';
            $weak_self->_handle_disconnect_and_close('idle_timeout');
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
# producer rechecks connection/stream state after the await. Some teardown
# sites run inside nghttp2's feed() (e.g. the oversize-body 413 path); completing
# a waiter resumes the producer synchronously, so defer to the next loop tick to
# keep the resumed producer out of a re-entrant nghttp2 call.
sub _h2_resolve_stream_drain_waiters {
    my ($self, $ss) = @_;
    return unless $ss && $ss->{stream_drain_waiters};
    my @waiters = splice @{$ss->{stream_drain_waiters}};
    return unless @waiters;
    $self->{server}->loop->later(sub {
        $_->done for grep { !$_->is_ready } @waiters;
    });
}

# Release a send() parked in the http.response.trailers arm awaiting
# $deliver_trailer_eof (the data callback's own terminal invocation) for a
# stream that is being torn down before that ever happens. Resolve, never
# fail -- same h2_closed carve-out contract as every other post-close send
# (design §6.2 / §21 item 1): a trailers send racing a disconnect is a
# successful no-op, not an error the app must handle. Same re-entrancy
# discipline as _h2_resolve_stream_drain_waiters above (deferred one loop
# tick -- some teardown sites run inside nghttp2's feed()).
sub _h2_resolve_stream_trailer_wait {
    my ($self, $ss) = @_;
    return unless $ss;
    my $f = delete $ss->{trailer_wait};
    return unless $f;
    $self->{server}->loop->later(sub {
        $f->done unless $f->is_ready;
    });
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
            return unless $weak_self->{websocket_mode};

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
                $weak_self->_handle_disconnect_and_close('keepalive_timeout');
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

    # Try to parse a request from the buffer
    my ($request, $consumed) = $self->{protocol}->parse_request($self->{buffer});

    return unless $request;

    # Remove consumed bytes from buffer
    substr($self->{buffer}, 0, $consumed) = '';

    # Handle parse errors (malformed request, header too large)
    if ($request->{error}) {
        # Mark connection as disconnected with protocol_error reason (PAGI spec compliance)
        $self->_handle_disconnect('protocol_error');
        $self->_send_error_response($request->{error}, $request->{message});
        $self->_close;
        return;
    }

    # Check Content-Length against max_body_size limit (0 = unlimited)
    if ($self->{max_body_size} && defined $request->{content_length}) {
        if ($request->{content_length} > $self->{max_body_size}) {
            $self->_handle_disconnect('body_too_large');
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

    if (my $error = $@) {
        # Delivery defines completion (Www.pod:1156-1165): if the terminal
        # response event already went out, an exception thrown afterward is
        # not a disconnect. Fire on_complete (never on_disconnect), leave
        # disconnect_reason() undef, still warn (SHOULD log), and still
        # close (MAY close) -- mirrors _h2_dispatch_stream's own log-only
        # branch for an error after the response is already complete: that
        # stream's connection_state is likewise marked complete before the
        # exception is even observed there.
        if ($self->{response_started} && (($self->{h1_seq} // '') eq 'complete')) {
            warn "PAGI application error (after response complete): $error\n";
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
            warn "PAGI application error (after response started): $error\n";
        } else {
            $self->_send_error_response(500, "Internal Server Error");
            warn "PAGI application error: $error\n";
        }
        # Write access log before closing
        $self->_write_access_log;
        # Notify server that request completed (for max_requests tracking)
        $self->{server}->_on_request_complete if $self->{server};
        # Always close connection after exception (3.2) - don't try keep-alive
        $self->_handle_disconnect('server_error');
        $self->_close;
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
            warn "PAGI application returned without starting a response\n";
            $self->_send_error_response(500, "Internal Server Error");
        }
        $self->_write_access_log;
        $self->{server}->_on_request_complete if $self->{server};
        $self->_handle_disconnect('server_error');
        $self->_close;
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
        $self->_write_access_log;
        $self->{server}->_on_request_complete if $self->{server};
        $self->_handle_disconnect_and_close('server_error');
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

        # Check if there's more data in the buffer (pipelining)
        if (length($self->{buffer}) > 0) {
            $self->_try_handle_request;
        }
    } else {
        # Not keeping alive - close connection
        $self->_handle_disconnect_and_close('request_complete');
    }
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

sub _create_scope {
    my ($self, $request) = @_;

    # Create connection state object for disconnect tracking
    # Uses lazy Future creation - Future only allocated if disconnect_future() is called
    my $connection_state = PAGI::Server::ConnectionState->new(
        connection => $self,
    );
    $self->{current_connection_state} = $connection_state;

    my $scope = {
        type         => 'http',
        pagi         => {
            version      => '0.5',
            spec_version => '0.4',
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
# is 'http.request' or 'sse.request'; $make_disconnect is a coderef
# producing this scope's disconnect event ({type=>'http.disconnect'} or
# the SSE closure's sse.disconnect constructor).
async sub _read_chunked_body {
    my ($self, $event_type, $make_disconnect, $body_complete_ref, $bytes_read_ref) = @_;

    # Wait for data if buffer is empty
    while (length($self->{buffer}) == 0 && !$self->{closed}) {
        if (!$self->{receive_pending}) {
            $self->{receive_pending} = Future->new;
        }
        await $self->{receive_pending};
        $self->{receive_pending} = undef;

        # Check queue after waiting
        if (@{$self->{receive_queue}}) {
            return shift @{$self->{receive_queue}};
        }
    }

    return $make_disconnect->()
        if $self->{closed} && length($self->{buffer}) == 0;

    # Try to parse chunked data
    my ($data, $consumed, $complete) = $self->{protocol}->parse_chunked_body($self->{buffer});

    # Check for parse error (invalid chunk size)
    if (ref($data) eq 'HASH' && $data->{error}) {
        $self->_handle_disconnect('protocol_error');
        $self->_send_error_response($data->{error}, $data->{message} // 'Bad Request');
        $self->_close;
        return $make_disconnect->();
    }

    if ($consumed > 0) {
        substr($self->{buffer}, 0, $consumed) = '';

        # Track total bytes read for max_body_size check
        $$bytes_read_ref += length($data // '');

        # Check max_body_size for chunked requests (0 = unlimited)
        if ($self->{max_body_size} && $$bytes_read_ref > $self->{max_body_size}) {
            # Body too large - close connection
            $self->_send_error_response(413, 'Payload Too Large');
            $self->_handle_disconnect('body_too_large');
            $self->_close;
            return $make_disconnect->();
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
    if (!$self->{receive_pending}) {
        $self->{receive_pending} = Future->new;
    }
    await $self->{receive_pending};
    $self->{receive_pending} = undef;

    # Recursive call to re-process - but we can't use __SUB__ in nested async
    # Just return disconnect if closed
    return $make_disconnect->() if $self->{closed};
    # This shouldn't happen often - caller should retry
    return { type => $event_type, body => '', more => 1 };
}

sub _create_receive {
    my ($self, $request) = @_;

    my $content_length = $request->{content_length};
    my $is_chunked = $request->{chunked} // 0;
    my $expect_continue = $request->{expect_continue} // 0;
    my $continue_sent = 0;
    my $body_complete = 0;
    my $bytes_read = 0;
    my $chunk_size = 65536;  # 64KB chunks for large bodies

    # For requests without Content-Length and not chunked, treat as no body
    my $has_body = defined($content_length) && $content_length > 0 || $is_chunked;

    weaken(my $weak_self = $self);

    # Return a wrapper that tracks the Future from the async receive
    return sub {
        return Future->done({ type => 'http.disconnect' }) unless $weak_self;
        return Future->done({ type => 'http.disconnect' }) if $weak_self->{closed};

        # The actual async implementation
        my $future = (async sub {
            return { type => 'http.disconnect' } unless $weak_self;
            return { type => 'http.disconnect' } if $weak_self->{closed};

            # Check queue first - events from disconnect handler
            if (@{$weak_self->{receive_queue}}) {
                return shift @{$weak_self->{receive_queue}};
            }

            # If body is already complete, wait for disconnect
            if ($body_complete) {
                if (!$weak_self->{receive_pending}) {
                    $weak_self->{receive_pending} = Future->new;
                }

                if ($weak_self->{closed}) {
                    $weak_self->{receive_pending} = undef;
                    return { type => 'http.disconnect' };
                }

                my $result = await $weak_self->{receive_pending};
                # receive_pending may be completed with a value (disconnect event)
                # or just done() as a signal
                return $result if ref $result eq 'HASH';
                # If no value, check queue
                if (@{$weak_self->{receive_queue}}) {
                    return shift @{$weak_self->{receive_queue}};
                }
                return { type => 'http.disconnect' };
            }

            # For requests without body, return empty body immediately
            if (!$has_body) {
                $body_complete = 1;
                return {
                    type => 'http.request',
                    body => '',
                    more => 0,
                };
            }

            # Send 100 Continue if client expects it (before reading body)
            if ($expect_continue && !$continue_sent) {
                $continue_sent = 1;
                $weak_self->{stream}->write($weak_self->{protocol}->serialize_continue);
            }

            # Handle chunked Transfer-Encoding
            if ($is_chunked) {
                return await $weak_self->_read_chunked_body(
                    'http.request',
                    sub { { type => 'http.disconnect' } },
                    \$body_complete,
                    \$bytes_read,
                );
            }

            # Handle Content-Length based body reading
            my $remaining = $content_length - $bytes_read;

            if ($remaining <= 0) {
                $body_complete = 1;
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
                await $weak_self->{receive_pending};
                $weak_self->{receive_pending} = undef;

                # Check queue after waiting
                if (@{$weak_self->{receive_queue}}) {
                    return shift @{$weak_self->{receive_queue}};
                }
            }

            # Return disconnect if closed while waiting
            if ($weak_self->{closed} && length($weak_self->{buffer}) == 0) {
                return { type => 'http.disconnect' };
            }

            # Read up to chunk_size or remaining bytes, whichever is smaller
            my $to_read = $remaining < $chunk_size ? $remaining : $chunk_size;
            $to_read = length($weak_self->{buffer}) if length($weak_self->{buffer}) < $to_read;

            my $body = substr($weak_self->{buffer}, 0, $to_read, '');
            $bytes_read += length($body);

            # Check if we've read all the body
            my $more = ($bytes_read < $content_length) ? 1 : 0;

            if (!$more) {
                $body_complete = 1;
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
    my ($self, $request) = @_;

    my $chunked = 0;
    my $expects_trailers = 0;
    my $seq = 'initial';
    # Publish the closure-local $seq on $self so the app-return path in
    # _handle_request can tell a completed response from one the app left
    # incomplete (see the mirror contract at every $seq assignment below).
    $self->{h1_seq} = $seq;
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

    return async sub  {
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
        $weak_self->{h1_seq} = $seq;

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
            $headers = _h1_strip_connection_headers($headers);

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
                    } elsif ($client_wants_keepalive) {
                        # HTTP/1.0 client requested keep-alive and we can honor it
                        # Must explicitly acknowledge with Connection: keep-alive
                        push @final_headers, ['connection', 'keep-alive'];
                    }
                }
            } else {
                $chunked = !$has_content_length;
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
                    $weak_self->{h1_seq} = $seq;
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
                    $weak_self->{h1_seq} = $seq;
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
                $weak_self->{h1_seq} = $seq;
                die "http.response.trailers requires chunked framing (response declared content-length)\n";
            }

            # RFC 9110 section 6.5.1 additionally forbids connection-specific/
            # framing fields in trailers outright, on any HTTP version -- not
            # just the PAGI spec's h1 response-header rule this strip
            # otherwise exists for. Strip at ingestion, same placement as
            # every h1 response-header site above.
            my $trailer_headers = _h1_strip_connection_headers($event->{headers} // []);

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
    return {
        type   => 'websocket.disconnect',
        code   => $self->{ws_disconnect_code}   // 1006,
        reason => $self->{ws_disconnect_reason} // '',
    };
}

sub _handle_disconnect {
    my ($self, $reason) = @_;

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

    # Mark HTTP connection state as disconnected (abnormal only).
    # Only for HTTP - WebSocket/SSE have their own patterns.
    if ($self->{current_connection_state} && !$self->{websocket_mode} && !$self->{sse_mode}) {
        $self->{current_connection_state}->_mark_disconnected($reason)
            unless $is_completion;
    }

    # HTTP/2: connection-level teardown (server shutdown, socket error, ...)
    # sweeps every open stream's own connection_state with this reason, so a
    # stream still mid-response when the whole connection dies still reports
    # why. _mark_disconnected is idempotent -- a stream _h2_on_close already
    # took to a terminal state (complete, or its own client_closed/server_error)
    # keeps that first reason; only still-open streams pick this one up.
    # WebSocket/SSE streams never attach a connection_state (N/A per spec),
    # so the guard on $stream->{connection_state} skips them naturally.
    if ($self->{is_h2} && $self->{h2_streams} && !$is_completion) {
        for my $stream (values %{$self->{h2_streams}}) {
            $stream->{connection_state}->_mark_disconnected($reason)
                if $stream->{connection_state};
        }
    }

    # Cancel any pending drain waiters (backpressure) AFTER connection state
    # is marked above: resolving a parked waiter can synchronously resume an
    # awaiting app coroutine (Future::AsyncAwait resumes inline off ->done),
    # and that resumed app's first act may be to read is_connected() /
    # disconnect_reason() -- those must already reflect this disconnect, not
    # a stale "still connected" snapshot from before it was detected.
    $self->_cancel_drain_waiters($reason);

    # Record the abnormal reason so the WebSocket disconnect event reports it
    # (instead of the old empty string). SSE tracks its own reason at the
    # detection sites via sse_disconnect_reason.
    if ($self->{websocket_mode} && !$is_completion) {
        $self->{ws_disconnect_reason} = $reason;
    }

    # Determine disconnect event type based on mode
    my $disconnect_event;
    if ($self->{websocket_mode}) {
        $disconnect_event = $self->_ws_disconnect_event;
    } elsif ($self->{sse_mode}) {
        # A completed decline is not an abnormal end -- the spec says a
        # decline delivers no events at all, so leave $disconnect_event
        # unset rather than synthesizing sse.disconnect for it.
        $disconnect_event = {
            type   => 'sse.disconnect',
            reason => $self->{sse_disconnect_reason} // 'client_closed',
        } unless $self->{sse_decline_completed};
    } else {
        $disconnect_event = { type => 'http.disconnect' };
    }

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
    $self->{ws_disconnect_code} = $code;

    my $frame = Protocol::WebSocket::Frame->new(
        type   => 'close',
        buffer => pack('n', $code) . $reason,
    );

    $self->{stream}->write($frame->to_bytes);
    $self->{close_sent} = 1;
}

sub _close {
    my ($self) = @_;

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

            if ($stream->{body_pending} && !$stream->{body_pending}->is_ready) {
                my $event = $stream->{is_sse}
                    ? { type => 'sse.disconnect', reason => 'client_closed' }
                    : { type => 'http.disconnect' };
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
        eval { $self->{h2_session}->terminate(0) };
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

    # Determine disconnect event type based on mode
    my $disconnect_event;
    if ($self->{websocket_mode}) {
        $disconnect_event = $self->_ws_disconnect_event;
    } elsif ($self->{sse_mode}) {
        # See _handle_disconnect: a completed decline delivers no events.
        $disconnect_event = {
            type   => 'sse.disconnect',
            reason => $self->{sse_disconnect_reason} // 'client_closed',
        } unless $self->{sse_decline_completed};
    } else {
        $disconnect_event = { type => 'http.disconnect' };
    }

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
        $self->{stream}->close_when_empty;
    }
}

# Combined disconnect and close - use this from callbacks where $weak_self may
# become undefined after _handle_disconnect completes its Future callbacks.
# This method holds a strong reference to $self throughout the operation.
sub _handle_disconnect_and_close {
    my ($self, $reason) = @_;

    # Mark the transport closed before notifying: _handle_disconnect below
    # completes any pending receive(), which can synchronously resume the
    # app coroutine (it may run straight through a subsequent send()), so
    # the send-side closed-check needs "closed" to already be true at that
    # point (spec order: closed-check precedes validation). Resource
    # cleanup itself still happens in _close, gated by its own idempotency
    # flag so it isn't skipped by this early flip.
    $self->{closed} = 1;

    $self->_handle_disconnect($reason);
    $self->_close;
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
        warn "TLS server certificate extraction error: $@\n";
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
        warn "TLS client certificate extraction error: $@\n";
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

    $self->{sse_mode} = 1;
    $self->_stop_idle_timer;  # SSE connections are long-lived
    $self->_start_sse_idle_timer;  # Start SSE-specific idle timer if configured

    my $scope = $self->_create_sse_scope($request);
    my $receive = $self->_create_sse_receive($request);
    my $send = $self->_create_sse_send($request);

    my $app_failed = 0;
    eval {
        await $self->{app}->($scope, $receive, $send);
    };

    if (my $error = $@) {
        # If SSE not yet started, send HTTP error
        if (!$self->{sse_started}) {
            $self->_send_error_response(500, "Internal Server Error");
        }
        warn "PAGI application error (SSE): $error\n";
        # An exception is not a clean end -- never keep the connection alive
        # after one, exactly as the plain HTTP request path does.
        $app_failed = 1;
    }

    # End the stream (no-op if an explicit sse.close already finished it).
    $self->_finish_sse_stream('stream_complete');

    # The application produced no response at all: no sse.start, and no
    # completed decline. That is the same protocol error the plain HTTP path
    # reports, and it must never reach the keep-alive branch below -- the
    # client is still waiting for a response, so handing it back a connection
    # with zero bytes written would hang it until the idle timeout, or forever
    # when timeout => 0. A COMPLETED decline set response_started and closed
    # the connection itself, so it never lands here; a decline that started
    # but never sent its terminal body does, and its buffered headers are
    # about to be discarded, so it is equally unanswerable.
    if (!$self->{sse_started} && !$self->{response_started}) {
        unless ($self->{closed}) {
            warn "PAGI application returned without starting an SSE stream or a response\n";
            $self->_send_error_response(500, "Internal Server Error");
        }
        $self->_write_access_log;
        # Notify server that request completed (for max_requests tracking)
        $self->{server}->_on_request_complete if $self->{server};
        $self->_handle_disconnect_and_close('server_error');
        return;
    }

    # Write access log entry (logs at stream end with total duration). Must
    # precede the reset below, which clears the request it logs.
    $self->_write_access_log;

    # Notify server that request completed (for max_requests tracking). One
    # call site covers both fates below (keep-alive and close) -- the SSE
    # "request" completes here, when the stream ends, not at sse.start.
    $self->{server}->_on_request_complete if $self->{server};

    # Design section 11.6: a CLEAN end (the application returned, or it sent
    # sse.close) honors the "Connection: keep-alive" header the server itself
    # emitted on sse.start -- the terminator is written above and the
    # connection returns to ordinary request handling. Keep-alive still yields
    # to the usual overrides: an application exception, a transport already
    # gone (client disconnect, timeout, write error, server shutdown), a
    # client "Connection: close", and HTTP/1.0 semantics.
    if (!$app_failed && !$self->{closed} && $self->_should_keep_alive($request)) {
        $self->_reset_after_sse_stream;
        return;
    }

    $self->_handle_disconnect_and_close($self->{sse_finish_reason} // 'stream_complete');
}

# Idempotently end an SSE stream: write the chunked terminator (HTTP/1.1) and
# release the stream's timers so nothing can write after the terminator.
# Called both by the on-return path above and by an explicit sse.close event
# (which ends the stream while the application is still running), so it must
# run exactly once and must NOT decide the connection's fate -- that decision
# belongs to _handle_sse_request, once the application has actually returned.
sub _finish_sse_stream {
    my ($self, $reason) = @_;
    return if $self->{sse_finished};
    $self->{sse_finished} = 1;
    $self->{sse_finish_reason} = $reason;

    # Nothing may reach the wire after the terminating chunk.
    $self->_stop_sse_keepalive;
    $self->_stop_sse_idle_timer;

    # Send chunked terminator if SSE was started and the stream is still writable
    if ($self->{sse_started} && !$self->{closed} &&
        $self->{stream} && $self->{stream}->write_handle) {
        $self->{stream}->write("0\r\n\r\n");
    }
}

# Per-request state that must not survive a kept-alive HTTP/1.1 SSE stream
# (design section 11.6). The connection is about to serve an ordinary request
# on the same socket, so every field the stream touched has to be back at its
# constructor value. Inventory, as a constraint list:
#
#   SSE stream flags   sse_mode, sse_started, sse_finished, sse_finish_reason,
#                      sse_close_sent, sse_disconnect_reason
#   SSE decline state  sse_decline_started/status/headers/body
#   SSE timers/writer  sse_keepalive_timer + comment, sse_idle_timer,
#                      sse_keepalive_writer (closes over this stream's framing)
#   Response state     handling_request, response_started, h1_seq,
#                      _resp_pending, response_status, _response_size
#   Request state      request_start, current_request, request_future,
#                      current_connection_state, current_transport_state
#   Receive state      receive_queue, receive_pending, receive_futures
#   Idle timeout       the between-requests idle timer, removed when the
#                      stream started, must be re-armed
#
# The send closure's own $seq is per-request by construction (a new closure is
# built per request), so the post-sse.close raise contract survives untouched.
#
# _disconnect_handled is deliberately NOT reset: every _handle_disconnect pairs
# with _close (and so with {closed}), which the keep-alive branch excludes, so
# reaching here means no disconnect was ever handled and the flag is still 0.
sub _reset_after_sse_stream {
    my ($self) = @_;

    $self->_stop_sse_keepalive;
    $self->_stop_sse_idle_timer;
    delete $self->{sse_keepalive_writer};

    $self->{sse_mode}              = 0;
    $self->{sse_started}           = 0;
    $self->{sse_finished}          = 0;
    $self->{sse_finish_reason}     = undef;
    $self->{sse_close_sent}        = 0;
    $self->{sse_disconnect_reason} = undef;
    delete @{$self}{qw(
        sse_decline_started sse_decline_status sse_decline_headers sse_decline_body
    )};

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

    # Check if there's more data in the buffer (pipelining)
    if (length($self->{buffer}) > 0) {
        $self->_try_handle_request;
    }
}

sub _create_sse_scope {
    my ($self, $request) = @_;

    my $scope = {
        type         => 'sse',
        pagi         => {
            version      => '0.5',
            spec_version => '0.4',
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
    my $continue_sent = 0;
    my $has_body = defined($content_length) && $content_length > 0 || $is_chunked;
    my $body_complete = 0;
    my $bytes_read = 0;

    weaken(my $weak_self = $self);

    # Helper to create SSE disconnect event with reason
    my $sse_disconnect = sub {
        return {
            type   => 'sse.disconnect',
            reason => ($weak_self ? $weak_self->{sse_disconnect_reason} : undef) // 'client_closed',
        };
    };

    # The stream is over -- either the transport is gone, or the application
    # ended it with sse.close and is still running (the connection stays open
    # for keep-alive, design section 11.6). Nothing more will ever arrive
    # on this scope, so answer immediately instead of blocking forever. This
    # answers a receive() the application chose to make; it is not a disconnect
    # fired at the application because the stream ended.
    my $stream_over = sub {
        return 1 unless $weak_self;
        return $weak_self->{closed} || $weak_self->{sse_finished};
    };

    return sub {
        if ($stream_over->()) {
            # A completed decline delivers no events at all (spec): a
            # receive() call made after the decline response has already
            # finished must not be answered with a synthesized
            # sse.disconnect -- nothing abnormal happened. Park instead;
            # the app has already gotten its answer (the decline response)
            # and has nothing further to receive.
            return $weak_self->{server}->loop->new_future
                if $weak_self && $weak_self->{sse_decline_completed};
            return Future->done($sse_disconnect->());
        }

        my $future = (async sub {
            return $sse_disconnect->()
                if $stream_over->();

            # Check queue first
            if (@{$weak_self->{receive_queue}}) {
                return shift @{$weak_self->{receive_queue}};
            }

            # Handle request body for POST/PUT SSE requests
            if ($has_body && !$body_complete) {
                # Send 100 Continue if client expects it (before reading body)
                if ($expect_continue && !$continue_sent) {
                    $continue_sent = 1;
                    $weak_self->{stream}->write($weak_self->{protocol}->serialize_continue);
                }

                if ($is_chunked) {
                    return await $weak_self->_read_chunked_body(
                        'sse.request',
                        $sse_disconnect,
                        \$body_complete,
                        \$bytes_read,
                    );
                }

                my $remaining = $content_length - $bytes_read;

                # Wait for data if buffer is empty
                while (length($weak_self->{buffer}) == 0 && !$weak_self->{closed} && $remaining > 0) {
                    if (!$weak_self->{receive_pending}) {
                        $weak_self->{receive_pending} = Future->new;
                    }
                    await $weak_self->{receive_pending};
                    $weak_self->{receive_pending} = undef;

                    if (@{$weak_self->{receive_queue}}) {
                        return shift @{$weak_self->{receive_queue}};
                    }
                }

                return $sse_disconnect->() if $weak_self->{closed};

                # Read available data up to remaining
                my $to_read = $remaining < length($weak_self->{buffer})
                    ? $remaining
                    : length($weak_self->{buffer});

                my $chunk = substr($weak_self->{buffer}, 0, $to_read, '');
                $bytes_read += length($chunk);

                my $more = ($bytes_read < $content_length) ? 1 : 0;
                $body_complete = 1 if !$more;

                return {
                    type => 'sse.request',
                    body => $chunk,
                    more => $more,
                };
            }

            # No body or body complete - return empty body if not yet returned
            if (!$body_complete) {
                $body_complete = 1;
                return {
                    type => 'sse.request',
                    body => '',
                    more => 0,
                };
            }

            # Wait for disconnect
            while (1) {
                if (@{$weak_self->{receive_queue}}) {
                    return shift @{$weak_self->{receive_queue}};
                }

                return $sse_disconnect->()
                    if $weak_self->{closed};

                if (!$weak_self->{receive_pending}) {
                    $weak_self->{receive_pending} = Future->new;
                }
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

    return async sub  {
        my ($event) = @_;
        return Future->done unless $weak_self;

        my $type = $event->{type} // '';

        # Once the machine has already recorded this stream as closed --
        # either an app-initiated sse.close (which closes the transport as a
        # side effect of ending the stream) or a completed decline response
        # (whose finalize branch also closes the transport as a side
        # effect) -- the machine, not the transport-closed check below,
        # decides what happens next: idempotent no-op for a repeat
        # sse.close, croak for anything else (decline_complete has no
        # idempotent case; it croaks unconditionally).
        my $already_closed = ($seq eq 'closed' || $seq eq 'decline_complete');

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

        if ($type eq 'sse.start') {
            return if $weak_self->{sse_started};
            $weak_self->{sse_started} = 1;
            $weak_self->{response_started} = 1;

            my $status = $event->{status} // 200;
            $weak_self->{response_status} = $status;  # Track for access logging
            my $headers = $event->{headers} // [];
            # PAGI spec — HTTP/1.1 owns Transfer-Encoding and Connection;
            # strip any app-supplied values before they reach the wire.
            $headers = _h1_strip_connection_headers($headers);

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
            return unless $weak_self->{sse_started};

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
            return unless $weak_self->{sse_started};

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
            # no-op).
            $weak_self->{sse_close_sent} = 1;
            $weak_self->{sse_disconnect_reason} = $event->{reason}
                if defined $event->{reason};
            $weak_self->_finish_sse_stream($event->{reason} // 'app_closed');
        }
        elsif ($type eq 'sse.http.response.start') {
            # Decline the SSE stream and return a normal HTTP response. Valid only
            # before sse.start; namespaced under sse. (mirrors websocket.http.response.*).
            # A decline-after-start attempt, and a repeat sse.http.response.start,
            # are already rejected by advance_sse (the 'declining' state only
            # accepts sse.http.response.body).
            $weak_self->{sse_decline_started} = 1;
            $weak_self->{sse_decline_status}  = $event->{status} // 200;
            $weak_self->{sse_decline_headers} = [
                map { [_validate_header_name($_->[0]), _validate_header_value($_->[1])] }
                    @{$event->{headers} // []}
            ];
            # PAGI spec — HTTP/1.1 owns Transfer-Encoding and Connection;
            # strip any app-supplied values before submission.
            $weak_self->{sse_decline_headers} = _h1_strip_connection_headers($weak_self->{sse_decline_headers});
            $weak_self->{sse_decline_body} = '';
        }
        elsif ($type eq 'sse.http.response.body') {
            return unless $weak_self->{sse_decline_started};
            return if $weak_self->{response_started};
            $weak_self->{sse_decline_body} .= $event->{body} // '';
            return if $event->{more};   # more chunks coming — keep buffering

            my $status  = $weak_self->{sse_decline_status};
            my $body    = $weak_self->{sse_decline_body};
            my @headers = (
                @{$weak_self->{sse_decline_headers}},
                ['content-length', length $body],
            );
            unless (grep { lc($_->[0]) eq 'date' } @headers) {
                push @headers, ['date', $weak_self->{protocol}->format_date];
            }
            # This response is always followed by closing the connection; tell
            # a keep-alive-pooling client so it doesn't reuse a dead socket.
            unless (grep { lc($_->[0]) eq 'connection' } @headers) {
                push @headers, ['connection', 'close'];
            }
            my $response = $weak_self->{protocol}->serialize_response_start($status, \@headers, 0);
            $response .= $body;
            $weak_self->{stream}->write($response);
            $weak_self->{response_started} = 1;
            $weak_self->{response_status}  = $status;   # access log
            # Declined: close the connection (no event stream was started).
            # Marked BEFORE teardown so _handle_disconnect and a later
            # receive() both know this closure is a completed decline, not
            # an abnormal end -- per the spec, a decline delivers no events
            # at all, so neither may synthesize sse.disconnect for it.
            $weak_self->{sse_decline_completed} = 1;
            $weak_self->_handle_disconnect_and_close('client_closed');
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

    $self->_stop_idle_timer;  # WebSocket connections are long-lived
    $self->_start_ws_idle_timer;  # Start WebSocket-specific idle timer if configured

    my $scope = $self->_create_websocket_scope($request);
    my $receive = $self->_create_websocket_receive($request);
    my $send = $self->_create_websocket_send($request);

    eval {
        await $self->{app}->($scope, $receive, $send);
    };

    if (my $error = $@) {
        # If handshake not yet done, send HTTP error
        if (!$self->{websocket_accepted}) {
            $self->_send_error_response(500, "Internal Server Error");
        }
        warn "PAGI application error (WebSocket): $error\n";
    }

    # Write access log entry (logs at connection close with total duration)
    $self->_write_access_log;

    # Notify server that request completed (for max_requests tracking)
    $self->{server}->_on_request_complete if $self->{server};

    # Close connection after WebSocket session ends
    $self->_handle_disconnect_and_close('session_complete');
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

    my $scope = {
        type         => 'websocket',
        pagi         => {
            version      => '0.5',
            spec_version => '0.4',
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
            my %ext = (%{$self->_get_extensions_for_scope}, 'websocket.http.response' => {});
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

    return sub {
        return Future->done({ type => 'websocket.disconnect', code => 1006, reason => '' })
            unless $weak_self;

        # Check queue first - drain queued messages even if closed
        if (@{$weak_self->{receive_queue}}) {
            return Future->done(shift @{$weak_self->{receive_queue}});
        }

        # $weak_self is known live here, so the fallback can report the
        # abnormal reason a server-initiated close recorded (idle timeout,
        # queue overflow, ...) instead of an always-empty ''.
        return Future->done($weak_self->_ws_disconnect_event)
            if $weak_self->{closed};

        my $future = (async sub {
            return { type => 'websocket.disconnect', code => 1006, reason => '' }
                unless $weak_self;

            # Check queue first - drain queued messages even if closed
            if (@{$weak_self->{receive_queue}}) {
                return shift @{$weak_self->{receive_queue}};
            }

            return $weak_self->_ws_disconnect_event
                if $weak_self->{closed};

            # First call returns websocket.connect
            if (!$connect_sent) {
                $connect_sent = 1;
                return { type => 'websocket.connect' };
            }

            # If not in WebSocket mode yet (waiting for accept), wait
            while (!$weak_self->{websocket_mode} && !$weak_self->{closed}) {
                if (!$weak_self->{receive_pending}) {
                    $weak_self->{receive_pending} = Future->new;
                }
                await $weak_self->{receive_pending};
                $weak_self->{receive_pending} = undef;

                if (@{$weak_self->{receive_queue}}) {
                    return shift @{$weak_self->{receive_queue}};
                }
            }

            return $weak_self->_ws_disconnect_event
                if $weak_self->{closed};

            # Wait for events from frame processing
            while (1) {
                if (@{$weak_self->{receive_queue}}) {
                    return shift @{$weak_self->{receive_queue}};
                }

                return $weak_self->_ws_disconnect_event
                    if $weak_self->{closed};

                if (!$weak_self->{receive_pending}) {
                    $weak_self->{receive_pending} = Future->new;
                }
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

    return async sub  {
        my ($event) = @_;
        return Future->done unless $weak_self;

        # Once the machine has recorded the connection closed or the denial
        # response complete, the machine decides what happens next -- both
        # 'closed' and 'denial_complete' have no idempotent case, so any
        # further send always raises -- not the transport-closed check below.
        # Only app-initiated terminals get this carve-out: websocket.close
        # itself flips {closed} synchronously via _handle_disconnect_and_close
        # when the client had already sent its own close frame first, but
        # that's still the app's own logical close (it chose to answer with
        # websocket.close), so a post-close send from the app is a protocol
        # violation, not a benign transport no-op. A real client disconnect
        # with no app-initiated terminal in play still falls through to the
        # plain no-op below.
        my $already_closed = ($seq eq 'closed' || $seq eq 'denial_complete');

        return Future->done if $weak_self->{closed} && !$already_closed;

        # Reset WebSocket idle timer on send activity
        $weak_self->_reset_ws_idle_timer;

        my $type = $event->{type} // '';

        # Mandatory event validation and sequencing (PAGI spec compliance).
        # Order per spec: transport-closed no-op check above runs first.
        # websocket.http.response is always available on this path (the scope
        # advertises it unconditionally; see _create_websocket_scope), unlike
        # connection-level extensions such as fullflush.
        PAGI::Server::EventValidator::validate_websocket_send(
            $event, { extensions => { %{$weak_self->{extensions}}, 'websocket.http.response' => {} } });
        $seq = PAGI::Server::EventValidator::advance_websocket($seq, $event);

        if ($type eq 'websocket.accept') {
            # A duplicate accept is already rejected by advance_websocket.

            # Complete the WebSocket handshake
            my $ws_key = $weak_self->{ws_key};
            my $accept_key = sha1_base64($ws_key . WS_GUID);
            # sha1_base64 doesn't add padding, but WebSocket requires it
            $accept_key .= '=' while length($accept_key) % 4;

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

            # Add custom headers if specified (with CRLF injection validation).
            # The server's own "Connection: Upgrade" line above is untouched
            # (RFC 6455 requires it) -- strip the app's extra headers the same
            # way every other h1 response-header path already does, so an
            # app-supplied connection/transfer-encoding value can't duplicate
            # or contradict it on the wire.
            if (my $extra_headers = $event->{headers}) {
                $extra_headers = _h1_strip_connection_headers($extra_headers);
                for my $h (@$extra_headers) {
                    my ($name, $value) = @$h;
                    $name = _validate_header_name($name);
                    $value = _validate_header_value($value);
                    push @headers, "$name: $value\r\n";
                }
            }

            push @headers, "\r\n";

            $weak_self->{stream}->write(join('', @headers));

            # Switch to WebSocket mode
            $weak_self->{websocket_mode} = 1;
            $weak_self->{websocket_accepted} = 1;
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
            return unless $weak_self->{websocket_mode};

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
        elsif ($type eq 'websocket.http.response.start') {
            # Custom handshake denial (websocket.http.response extension). Only
            # valid before accept; buffer the response head until the body
            # arrives.
            return if $weak_self->{websocket_accepted};
            return if $weak_self->{ws_denial_started};
            $weak_self->{ws_denial_started} = 1;
            $weak_self->{ws_denial_status}  = $event->{status} // 403;
            $weak_self->{ws_denial_headers} = [
                map { [_validate_header_name($_->[0]), _validate_header_value($_->[1])] }
                    @{$event->{headers} // []}
            ];
            # PAGI spec — HTTP/1.1 owns Transfer-Encoding and Connection;
            # strip any app-supplied values before submission.
            $weak_self->{ws_denial_headers} = _h1_strip_connection_headers($weak_self->{ws_denial_headers});
            $weak_self->{ws_denial_body} = '';
        }
        elsif ($type eq 'websocket.http.response.body') {
            return unless $weak_self->{ws_denial_started};
            return if $weak_self->{response_started};
            $weak_self->{ws_denial_body} .= $event->{body} // '';
            return if $event->{more};   # more chunks coming — keep buffering

            my $status  = $weak_self->{ws_denial_status};
            my $body    = $weak_self->{ws_denial_body};
            my @headers = (
                @{$weak_self->{ws_denial_headers}},
                ['content-length', length $body],
            );
            unless (grep { lc($_->[0]) eq 'date' } @headers) {
                push @headers, ['date', $weak_self->{protocol}->format_date];
            }
            my $response = $weak_self->{protocol}->serialize_response_start($status, \@headers, 0);
            $response .= $body;
            $weak_self->{stream}->write($response);
            $weak_self->{response_started} = 1;
            $weak_self->{response_status}  = $status;   # access log
            # Handshake rejected: close like the bare-403 path (no upgrade).
            $weak_self->_handle_disconnect_and_close('client_closed');
        }
        elsif ($type eq 'websocket.close') {
            # If not accepted yet, send 403 Forbidden
            if (!$weak_self->{websocket_accepted}) {
                $weak_self->_send_error_response(403, 'Forbidden');
                return;
            }

            # Send close frame
            my $code = $event->{code} // 1000;
            my $reason = $event->{reason} // '';

            my $frame = Protocol::WebSocket::Frame->new(
                type   => 'close',
                buffer => pack('n', $code) . $reason,
            );

            $weak_self->{stream}->write($frame->to_bytes);
            $weak_self->{close_sent} = 1;

            # If we received a close frame, close immediately
            # Otherwise wait for close from client (handled in frame processing)
            if ($weak_self->{close_received}) {
                $weak_self->_handle_disconnect_and_close('client_closed');
            }
        }
        elsif ($type eq 'websocket.keepalive') {
            return unless $weak_self->{websocket_mode};

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

    return unless $self->{websocket_mode};
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
                $self->_handle_disconnect_and_close('protocol_error');
                return;
            }
        }

        # RFC 6455 Section 5.2: Opcodes 3-7 and 11-15 (0xB-0xF) are reserved
        # Must fail connection with 1002 Protocol Error
        if (($opcode >= 3 && $opcode <= 7) || ($opcode >= 11 && $opcode <= 15)) {
            $self->_send_close_frame(1002, 'Reserved opcode');
            $self->_handle_disconnect_and_close('protocol_error');
            return;
        }

        # RFC 6455 Section 5.5: Control frames (close/ping/pong) MUST have
        # payload length <= 125 bytes
        if (($opcode == 8 || $opcode == 9 || $opcode == 10) && length($bytes) > 125) {
            $self->_send_close_frame(1002, 'Control frame too large');
            $self->_handle_disconnect_and_close('protocol_error');
            return;
        }

        if ($opcode == 1) {
            # Text frame - decode as UTF-8
            my $text = eval { Encode::decode('UTF-8', $bytes, Encode::FB_CROAK) };
            unless (defined $text) {
                # Invalid UTF-8 - close with 1007 per RFC 6455
                $self->_send_close_frame(1007, 'Invalid UTF-8');
                $self->_handle_disconnect_and_close('protocol_error');
                return;
            }
            # Check queue limit before adding (DoS protection)
            if (@{$self->{receive_queue}} >= $self->{max_receive_queue}) {
                $self->_send_close_frame(1008, 'Message queue overflow');
                $self->_handle_disconnect_and_close('queue_overflow');
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
                $self->_handle_disconnect_and_close('queue_overflow');
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
                $self->_handle_disconnect_and_close('protocol_error');
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
                    $self->_handle_disconnect_and_close('protocol_error');
                    return;
                }

                # RFC 6455: Close reason must be valid UTF-8
                if (length($reason) > 0) {
                    my $reason_copy = $reason;
                    my $decoded = eval { Encode::decode('UTF-8', $reason_copy, Encode::FB_CROAK) };
                    unless (defined $decoded) {
                        $self->_send_close_frame(1007, 'Invalid UTF-8 in close reason');
                        $self->_handle_disconnect_and_close('protocol_error');
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

            push @{$self->{receive_queue}}, {
                type   => 'websocket.disconnect',
                code   => $code,
                reason => $reason,
            };

            # This is the scope's one and only websocket.disconnect: mark
            # disconnect-handled now (the same guard _handle_disconnect
            # checks) so a later TCP close (on_closed -> client_closed) or
            # the app's own session-complete teardown finds the guard
            # already set and delivers nothing further. Without this, either
            # of those paths queues a second, ghost disconnect event that
            # nobody asked for and nobody drains (h2's equivalent guard is
            # ws_disconnect_delivered / _h2_ws_enqueue_disconnect).
            $self->{_disconnect_handled} = 1;
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
When an HTTP/1.1 SSE stream ends B<cleanly> -- the application returns, or it
sends C<sse.close> -- the server writes the chunked terminator, resets the
per-request state the stream accumulated, and hands the connection back to
ordinary keep-alive request handling, including serving any request already
pipelined in the read buffer. A pooled client (browser, C<Net::Async::HTTP>,
curl) can therefore reuse the same socket for its next request, which matters
for the short POST-SSE-exchange pattern used by fetch-event-source and
datastar.

Keep-alive yields to the usual overrides, each of which closes the connection
the same way it does outside SSE: a client C<Connection: close>, HTTP/1.0
semantics, server shutdown, an application exception, and any B<abnormal> end
(client disconnect, idle timeout, write error). An abnormal end is also the
only thing that delivers C<sse.disconnect> to the application; a clean end
never does.

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
