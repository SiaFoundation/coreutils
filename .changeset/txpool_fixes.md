---
default: patch
---

# TxPool fixes

#260 by @lukechampine

In the course of writing a test for full txpool behavior, I found a bug in the index handling: all of the transactions were being assigned an index of `len(txpool.txns)`, instead of sequential ones! This was almost certainly the result of copy-pasting the `m.txpool.indices[txid] = len(m.txpool.txns)` from `AddPoolTransactions`; it breaks in `revalidatePool` because we're appending to `filtered`, not `m.txpool.txns` directly.

This is probably the root cause of https://github.com/SiaFoundation/coreutils/issues/256. We had been blaming `parentMap` for that, so I ripped it out. I think it should stay ripped out, though, since benchmarks show that the operation it's optimizing is not particularly slow in the first place.

I also added an important validation step to `updateV2TransactionProofs`. This should prevent the panic seen in https://github.com/SiaFoundation/coreutils/pull/254.
