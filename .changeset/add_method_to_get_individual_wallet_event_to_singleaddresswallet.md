---
default: minor
---

# Add method to get individual wallet event to `SingleAddressWallet`

#301 by @chris124567

Add `WalletEvent(id types.Hash256) (Event, error)` to `SingleAddressStore` and a similar `Event` method to the SingleAddressWallet.

Needed for https://github.com/SiaFoundation/indexd/issues/246
