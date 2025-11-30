# 01 – Hello HTTP

Demonstrates the minimum PAGI HTTP app: accept only `scope->{type} eq 'http'`, send `http.response.start`, then a single `http.response.body` event.

Relevant spec sections:
- Core scope & application contract – `docs/specs/main.mkdn`
- HTTP response events – `docs/specs/www.mkdn`

To run, load `app.pl` in a PAGI HTTP server and point a browser or curl at it.
