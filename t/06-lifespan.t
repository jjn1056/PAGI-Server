use strict;
use warnings;
use Test2::V0;

# Step 6: Lifespan Protocol
# Tests for examples/06-lifespan-state/app.pl

plan skip_all => 'Server implementation pending - Step 6';

# TODO: Implement tests when lifespan is ready
#
# Test cases:
# 1. Lifespan startup runs before accepting connections
# 2. Shared state is available to HTTP requests
# 3. Graceful shutdown waits for cleanup
# 4. Failed startup prevents connection acceptance

done_testing;
