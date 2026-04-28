---
default: major
---

# Add RHP4 pool RPC handlers, renter helpers, and Contractor interface methods for shared balance pools.

Hosts can now back accounts with shared balance pools so renters don't need to pre-fund and continually rebalance every account. A renter funds a pool once and attaches as many accounts as they want; debits drain the account's own balance first and fall through to attached pools, which keeps per-account allowances small and reduces the total capital sitting idle in account balances. This is the host-side companion to the new RHP4 pool RPCs.

Extends the RHP4 `Contractor` interface with `PoolBalances`, `CreditPoolsWithContract`, `AttachPools`, and `DetachPools` (existing implementations must be updated). Adds host-side handlers for the new pool RPCs and renter-side helpers `RPCFundPools`, `RPCReplenishPools`, `RPCAttachPools`, and `RPCDetachPools`. `DebitAccount` semantics are extended to drain the account's own balance first, then attached pools in attachment order.
