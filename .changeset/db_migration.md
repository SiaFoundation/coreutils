---
default: CHANGE_TYPE
---

# DB migration

This adds a generic `MigrateDB` function for updating consensus databases, and a v1->v2 migration that recomputes the full element tree, which fixes Zen.