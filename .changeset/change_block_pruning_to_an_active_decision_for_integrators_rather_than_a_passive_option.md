---
default: major
---

# Changed block pruning to an active decision for integrators rather than a passive option

This fixes a race condition on some nodes when chain subscribers are slow where blocks would be removed from the store before they could be indexed. 
