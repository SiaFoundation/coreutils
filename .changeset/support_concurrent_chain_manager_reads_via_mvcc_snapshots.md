---
default: major
---

# Support concurrent chain.Manager reads via MVCC snapshots

The Manager previously serialized all of its methods -- including read-only ones -- with a single mutex, so heavy read load (e.g. from the syncer or subscribers) contended with block processing. The Manager now follows an MVCC scheme: writes accumulate in the Store's "scratchpad" -- which includes the tip state -- and are committed by Flush, at which point they become visible to "snapshots": self-contained, read-only captures of the committed state. Read-only Manager methods operate on snapshots, so they proceed concurrently with each other and with block processing, and never stall writers. bbolt supports snapshots natively via read-only transactions; `MemDB` now implements them with pin-aware copy-on-flush generations (`CacheDB` delegates to its underlying DB).

This is a breaking change for implementors and consumers of the `chain.DB` and `chain.Store` interfaces:

- `Store` is now `Snapshot() (StoreSnapshot, func())` + `Scratchpad() StoreScratchpad`. `StoreSnapshot` carries the read methods plus `TipState`; `StoreScratchpad` embeds it and adds the write methods and `Flush`, with `ApplyBlock`/`RevertBlock` updating the scratchpad's tip. Implementations may flush autonomously between operations (e.g. to bound the size or age of the accumulated writes), handling errors internally; the Manager explicitly calls Flush to publish the new tip after processing blocks, before notifying OnReorg listeners.
- `DB` mirrors `Store`: `Snapshot() (DBSnapshot, release func())` + `Scratchpad() DBScratchpad`. The scratchpad carries `Bucket`/`CreateBucket`/`Flush`/`Cancel` and must not flush autonomously; snapshots are read-only and remain valid until released, even across concurrent Flushes. A single `DBBucket` interface serves both sides; its `Put`/`Delete` methods may return errors on read-only (snapshot) buckets.
- `NewManager` no longer takes a `consensus.State` parameter, and `NewDBStore`/`NewDBStoreAtCheckpoint` no longer return one; the tip state is provided by the Store itself (via a snapshot's or scratchpad's `TipState`).
- Reads reflect committed state. Pruned blocks remain visible to read-only methods until the next commit, and blocks added to non-best chains remain *invisible* to them until the next commit. Blocks that extend the best chain are always committed before `AddBlocks` returns and before `OnReorg` listeners fire, so subscribers observe the tip they are notified of.
