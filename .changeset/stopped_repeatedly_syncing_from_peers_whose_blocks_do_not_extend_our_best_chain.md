---
default: patch
---

# Stopped repeatedly syncing from peers whose blocks do not extend our best chain

The syncer now skips a peer if our tip is unchanged since the last batch it served. Previously a peer stuck on a fork that would never outweigh our chain was queried every sync interval, re-downloading its entire fork indefinitely.
