---
default: minor
---

# Add support for block pruning

#116 by @lukechampine

Been a long time coming. 😅 

The strategy here is quite naive, but I think it will be serviceable. Basically, when we apply a block `N`, we delete block `N-P`. `P` is therefore the "prune target," i.e. the maximum number of blocks you want to store.

In practice, this isn't exhaustive: it only deletes blocks from the best chain. It also won't dramatically shrink the size of an existing database. I think this is acceptable, because pruning is most important during the initial sync, and during the initial sync, you'll only be receiving blocks from one chain at a time. Also, we don't want to make pruning *too* easy; after all, we need a good percentage of nodes to be storing the full chain, so that others can sync to them.

I tested this out locally with a prune target of 1000, and after syncing 400,000 blocks, my `consensus.db` was around 18 GB. This is disappointing; it should be much smaller. With some investigation, I found that the Bolt database was only storing ~5 GB of data (most of which was the accumulator tree, which we can't prune until after v2). I think this is a combination of a) Bolt grows the DB capacity aggressively in response to writes, and b) Bolt never shrinks the DB capacity. So it's possible that we could reduce this number by tweaking our DB batching parameters. Alternatively, we could provide a tool that copies the DB to a new file. Not the most user-friendly, but again, I think I'm okay with that for now.

Depends on https://github.com/SiaFoundation/core/pull/228
