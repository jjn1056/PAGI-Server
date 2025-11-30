# 08 – TLS Extension Introspection

Reads `scope->{extensions}{tls}` when present and reports certificate/version/cipher data back to the client. Falls back gracefully for non-TLS requests.

Spec references: `docs/specs/tls.mkdn` (TLS extension) and HTTP spec for response events.
