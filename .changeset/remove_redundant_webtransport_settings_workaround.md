---
default: patch
---

# Remove redundant WebTransport SETTINGS workaround

webtransport-go v0.13.0 sends SETTINGS_WT_ENABLED and SETTINGS_WT_MAX_SESSIONS from ConfigureHTTP3Server, and clears the manually set WT flow control settings during server initialization, so the workaround no longer had any effect.
