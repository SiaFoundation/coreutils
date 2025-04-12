---
default: patch
---

# More aggressive migration

#210 by @lukechampine

Previous piecemeal attempts at migration produced further discrepancies in the DB. Here we avoid this by taking a scorched-earth approach, nuking basically everything in the DB and resyncing from ourselves. Since sidechains cannot easily be fixed the same way, they are simply deleted. This produces a final state equivalent to resyncing from a good peer.

I have tested this with both Zen and Erravimus consensus.dbs, and it appears to successfully repair them to a valid state. (I reached block 113487 on Zen, and 3311 on Erravimus.) I suggest we deploy this to Erravimus, confirm that we can continue mining there, then deploy to Zen miners. Note that existing Zen blocks above 113487 will (eventually) be reorged.

Performance-wise, there is room for improvement here. The old migration code achieved significant speedups by caching DB writes in memory and sorting them before flushing. I am tempted to apply a similar strategy to the `DBStore` itself, in a more generic fashion -- but this is slightly risky and not critical, so if I do explore it, it'll be on a different branch.
