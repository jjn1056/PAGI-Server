# 03 – Request Body Echo

Reads all `http.request` events, concatenates the body, and echoes it back. Demonstrates:
- Looping over `http.request` events with `more` handling.
- Building a response that reflects information from the request.

Spec references: `docs/specs/www.mkdn` (HTTP request/response events).
