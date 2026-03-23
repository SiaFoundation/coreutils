---
default: patch
---

# Fixed excessive "peer relayed a v2 header with unknown parent" log spam by only relaying headers after sync when the synced blocks became the new tip.
