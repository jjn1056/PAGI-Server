# 07 – Extension-Aware Streaming with FullFlush

Demonstrates how to:
- Check for extension support via `scope->{extensions}{fullflush}`
- Use `http.fullflush` event during streaming to force immediate TCP buffer flush
- Only send extension events when the server advertises support

The fullflush extension is useful for real-time streaming scenarios where you want each chunk delivered to the client immediately rather than waiting for TCP buffer fill or Nagle's algorithm.

Spec references: `docs/specs/main.mkdn` (extensions section) and `docs/extensions.mkdn` (fullflush example).
