---
default: major
---

# Compared header chain work before downloading blocks from a peer

The syncer now walks a peer's headers until their chain either outweighs ours or runs out, and only downloads blocks once it knows the chain is worth adopting. Previously a peer stuck on a fork that would never outweigh our chain had its entire fork re-downloaded every sync interval.
