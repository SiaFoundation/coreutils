---
default: minor
---

# Implement parallel syncing

Replaces the old serial syncing algorithm with a parallel one. In brief:

```
Old algorithm:
1. For each unsynced peer, request blocks until the peer reports no further blocks

New algorithm:
1. In parallel, for each unsynced peer, request up to 10k headers
2. In parallel, request sections of the header chain from peers in batches of 100 blocks
3. Poll for new peers in the background and immediately assign them work
4. After assigning the final batch, redundantly assign any remaining in-progress batches to idle peers
5. Add batches of blocks to the Manager in a separate goroutine to avoid blocking peer I/O
6. When requesting v2 blocks, perform validation within each worker goroutine, rather than in the Manager goroutine
```

Empirically, the new algorithm is anywhere from 2x-10x faster, particularly once it reaches the v2 require height and can start validating in parallel as well.
