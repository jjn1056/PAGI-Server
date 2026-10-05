# PAGI-Server dependencies
# Install with: cpanm --installdeps .

requires 'perl', '5.018';

# Core async framework
requires 'IO::Socket::IP', '0.43'; # Without this IO::Async doesn't work as well
requires 'IO::Async', '0.802';  # Includes IO::Async::Function for worker pools
requires 'Future', '0.50';
requires 'Future::AsyncAwait', '0.66';

# Loop-agnostic async I/O (for apps - optional but recommended)
recommends 'Future::IO', '0.23';  # Provides sleep, read, write without loop coupling

# HTTP parsing

# WebSocket support
requires 'Protocol::WebSocket', '0.26';

# TLS support (optional - only needed for HTTPS)
recommends 'IO::Async::SSL', '0.25';
recommends 'IO::Socket::SSL', '2.074';
# To enable TLS/HTTPS support, install with:
#   cpanm IO::Async::SSL IO::Socket::SSL

# HTTP/2 support (optional - only needed for --http2)
recommends 'Net::HTTP2::nghttp2', '0.011';
# To enable HTTP/2 support, install with:
#   cpanm Net::HTTP2::nghttp2

# Utilities
requires 'URI::Escape', '5.09';

# JSON log lines (--log-format json, the production default) need the XS
# encoder; without it production logs stay text. Encode is core, but some
# distributions package it separately.
recommends 'Cpanel::JSON::XS';
requires 'Encode';

# Testing
on 'test' => sub {
    requires 'Test2::V0', '0.000159';
    requires 'Net::Async::HTTP', '0.49';
    requires 'Net::Async::WebSocket::Client', '0.14';
    requires 'URI', '1.60';
    requires 'JSON::MaybeXS', '1.004003';
    requires 'Time::HiRes', '1.9764';  # Core module, for timing-sensitive tests

    # No test here loads PAGI-Tools: the toolkit's behavior on a real server is
    # tested in PAGI-Tools' t/integration/. PAGI::Server::Runner's defaults
    # still name PAGI-Tools modules at run time (PAGI::App::Directory as the
    # default app, PAGI::Middleware::Lint in development); its tests use
    # stand-ins (t/lib/PAGITest/RunnerApp.pm).
};

# Development
on 'develop' => sub {
    requires 'Dist::Zilla', '6.030';
    requires 'Dist::Zilla::Plugin::MetaJSON';
    requires 'Dist::Zilla::Plugin::MetaResources';
    requires 'Dist::Zilla::Plugin::MetaNoIndex';
    requires 'Dist::Zilla::Plugin::Prereqs::FromCPANfile';
};
