---
default: minor
---

# Run fuzzer after PRs are merged

#290 by @chris124567

The same workflow has been tested at https://github.com/SiaFoundation/fuzz/actions

The default mines 500 blocks with allow height at 250 and require height at 400.  For each random block we generate, we apply it, revert it, then apply again.  We check that the expiring contracts after the applications and before application/after revert are the same (we sort ExpiringFileContracts by ID before comparing).  The generated blocks are uploaded as an artifact for reproducibility (you can reproduce with `./fuzzer repro repro.json`).
