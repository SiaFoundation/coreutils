---
default: major
---

# Remove persistence for locked UTXOs

Now that the tpool is persisted, UTXOs that have been broadcast will be readded on startup. This should mean that locked UTXOs can be ephemeral again since their primary purpose is to prevent double spends during RPCs.