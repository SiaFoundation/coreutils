---
default: patch
---

# Don't ignore missing supplement in updateV2TransactionProofs

#265 by @lukechampine

The logic introduced in #193 caused a panic when a block supplement was missing: we would create an empty supplement, try to use it, then panic with out-of-range when trying to fetch stuff from the empty supplement.

In case you're wondering how we could end up in a situation where we have a block, but not a supplement: the supplement is only calculated when we fully validate and apply the block. If we receive a block that isn't on the best chain, we'll store it without also computing and storing the supplement. If someone then calls `V2TransactionSet` with a basis referencing that block, we'll attempt to use it.
