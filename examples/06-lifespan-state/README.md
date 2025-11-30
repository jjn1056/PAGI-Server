# 06 – Lifespan & Shared State

Single PAGI app that handles both `lifespan` and `http` scopes:
- Stores a greeting in `scope->{state}` during `lifespan.startup`.
- Reuses that state when handling HTTP requests.
- Responds to `lifespan.shutdown` cleanly.

Spec references: `docs/specs/lifespan.mkdn` and `docs/specs/www.mkdn`.
