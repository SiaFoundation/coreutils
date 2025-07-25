---
default: major
---

# Removed deprecated V1 Syncer RPCs

Removed RPCSendBlocks and RPCSendBlk (made obsolete by RPCSendV2Blocks and RPCSendCheckpoint), as well as RPCRelayHeader and RPCSendTransactionSet (as no more v1 blocks or v1 transactions can be mined)