use strict;
use warnings;
use Test2::V0;

# Step 8: TLS Support
# Tests for examples/08-tls-introspection/app.pl

plan skip_all => 'Server implementation pending - Step 8';

# TODO: Implement tests when TLS is ready
#
# Test cases:
# 1. HTTPS connections work
# 2. TLS info is in scope.extensions.tls
# 3. Scheme is 'https' for TLS connections
# 4. Non-TLS has no tls extension
# 5. Client certs are captured

done_testing;
