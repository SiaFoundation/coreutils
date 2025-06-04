---
default: major
---

# Improve locking behaviour by removing the tip from memory and returning it alongside the unspent elements from the store.

Changes the store interface for the `SingleAddressWallet` to return the tip. This must be atomic with the current state of the proofs stored in the database.
