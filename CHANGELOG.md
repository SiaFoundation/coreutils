## 0.16.3 (2025-06-19)

### Features

- Require peers to support V2.

### Fixes

- Increase default MaxInflightRPCs
- prevent reverted coinbase transactions from spamming the transaction pool
- Update core to v0.14.0

## 0.16.2 (2025-06-17)

### Fixes

- Fixed an issue with a single syncing peer from taking over the sync loop.

#### Don't ignore missing supplement in updateV2TransactionProofs

##265 by @lukechampine

The logic introduced in #193 caused a panic when a block supplement was missing: we would create an empty supplement, try to use it, then panic with out-of-range when trying to fetch stuff from the empty supplement.

In case you're wondering how we could end up in a situation where we have a block, but not a supplement: the supplement is only calculated when we fully validate and apply the block. If we receive a block that isn't on the best chain, we'll store it without also computing and storing the supplement. If someone then calls `V2TransactionSet` with a basis referencing that block, we'll attempt to use it.

## 0.16.1 (2025-06-14)

### Fixes

- Added defer recover to prevent nodes from panicking due to uncaught bugs.
- Fixed a panic when attempting to update element proofs.
- Update core to v0.13.2

#### TxPool fixes

##260 by @lukechampine

In the course of writing a test for full txpool behavior, I found a bug in the index handling: all of the transactions were being assigned an index of `len(txpool.txns)`, instead of sequential ones! This was almost certainly the result of copy-pasting the `m.txpool.indices[txid] = len(m.txpool.txns)` from `AddPoolTransactions`; it breaks in `revalidatePool` because we're appending to `filtered`, not `m.txpool.txns` directly.

This is probably the root cause of https://github.com/SiaFoundation/coreutils/issues/256. We had been blaming `parentMap` for that, so I ripped it out. I think it should stay ripped out, though, since benchmarks show that the operation it's optimizing is not particularly slow in the first place.

I also added an important validation step to `updateV2TransactionProofs`. This should prevent the panic seen in https://github.com/SiaFoundation/coreutils/pull/254.

## 0.16.0 (2025-06-05)

### Breaking Changes

- Add txn broadcast methods to SingleAddressWallet which validate transactions and broadcast them to the network.

#### Improve locking behaviour by removing the tip from memory and returning it alongside the unspent elements from the store.

Changes the store interface for the `SingleAddressWallet` to return the tip. This must be atomic with the current state of the proofs stored in the database.

### Features

- Add a method for getting a fee recommendation from the SingleAddressWallet which caps the fee at a sane value.

### Fixes

- Fixed RHP4 webtransport rejecting cross-origin connections.
- Updated bootstrap peers.

## 0.15.2 (2025-05-29)

### Fixes

- Update core to v0.13.1

## 0.15.1 (2025-05-29)

### Fixes

- Fix race in FundV2Transaction
- Fixed an issue sending partial blocks to peers.

## 0.15.0 (2025-05-26)

### Breaking Changes

- Updated core with new miner committment hash

## 0.14.0 (2025-05-23)

### Breaking Changes

- Changed RPCAppendSectors and RPCFreeSectors methods to use ContractSigner instead of taking a private key directly
- The SingleAddressWalletStore now persists locked UTXOs.

### Features

- Return rhp4.ErrInvalidSignature consistently when we fail to validate the challenge signature.

### Fixes

- Removed invalid transaction set sticky error

## 0.13.6 (2025-05-14)

### Fixes

- Fixed v2 transaction events not including convenience fields

## 0.13.5 (2025-05-14)

### Fixes

- Update core to v0.12.3

## 0.13.4 (2025-05-14)

### Fixes

- Update core to v0.12.2

## 0.13.3 (2025-05-09)

### Fixes

- Fix deadlock in syncer.Close

## 0.13.2 (2025-04-28)

### Features

- Return error when transaction set relay fails

### Fixes

- Update coreutils to v0.12.0

#### Export 'missing block at index' error

##217 by @chris124567

The "missing block at index" error is used in multiple places to detect that a chain migration happened so that the app knows to reset whatever state it stored previously.  This PR exports it so that looking at the error string is not necessary.

https://github.com/SiaFoundation/hostd/blob/711ac3eb1da09e4d6a9ff17a2efe3555d70b8185/index/update.go#L36
https://github.com/SiaFoundation/walletd/blob/d91147b7ec52b2261779949905c8e0fd3c0ad5c7/wallet/manager.go#L733

And recently in explored:
https://github.com/SiaFoundation/explored/pull/212/files#diff-b973ebcbcd81d6f5eb333f7b87b7366e9886270ccdef647b80df03f94a1156dcR185

## 0.13.1 (2025-04-23)

### Fixes

- Update core to v0.11.0

## 0.13.0 (2025-04-23)

### Breaking Changes

#### DB migration

This adds a generic `MigrateDB` function for updating consensus databases, and a v1->v2 migration that recomputes the full element tree, which fixes Zen.

### Fixes

- Added a default deadline of 1m to the SiaMux dialer. If the context has a deadline, it will override the default.
- Return up to maxBlocks updates, not up to maxBlocks+1
- Fix ExpiringFileContracts ordering when reverting blocks

#### More aggressive migration

##210 by @lukechampine

Previous piecemeal attempts at migration produced further discrepancies in the DB. Here we avoid this by taking a scorched-earth approach, nuking basically everything in the DB and resyncing from ourselves. Since sidechains cannot easily be fixed the same way, they are simply deleted. This produces a final state equivalent to resyncing from a good peer.

I have tested this with both Zen and Erravimus consensus.dbs, and it appears to successfully repair them to a valid state. (I reached block 113487 on Zen, and 3311 on Erravimus.) I suggest we deploy this to Erravimus, confirm that we can continue mining there, then deploy to Zen miners. Note that existing Zen blocks above 113487 will (eventually) be reorged.

Performance-wise, there is room for improvement here. The old migration code achieved significant speedups by caching DB writes in memory and sorting them before flushing. I am tempted to apply a similar strategy to the `DBStore` itself, in a more generic fashion -- but this is slightly risky and not critical, so if I do explore it, it'll be on a different branch.

## 0.12.1 (2025-03-11)

### Fixes

- Fix RHP4 client not respecting timeouts

## 0.12.0 (2025-02-28)

### Breaking Changes

#### Separate RHP4 Transports

The SiaMux and QUIC transports are now separated into `go.sia.tech/rhp/v4/siamux` and `go.sia.tech/rhp/v4/quic` packages. Both packages define a `Dial` and `Serve` helper that can be used to either start a transport server or connect to a host using the transport.

### Features

#### Add RPCReplenishAccounts Implementation

Implements RPCReplenishAccounts in the RHP4 client and server enabling clients managing a large number of accounts to fund them quicker

### Fixes

- Fixes an issue where wallet redistributing would fail if the number of outputs created were less than requested
- Fix data race in EphemeralWalletStore.
- Fixed an issue with event confirmations not being correctly unmarshalled
- Increase default max streams from 100 to 1000 for QUIC transport
- Update core to v0.10.2 and mux to v1.4.0

## 0.11.1 (2025-02-10)

### Fixes

- Fixes an issue where redistribution of wallet outputs that require more than a single transaction would produce an invalid transaction set

## 0.11.0 (2025-02-06)

### Breaking Changes

- Add support for QUIC and WebTransport to RHP4

### Fixes

- Fixed data race in EphemeralContractor related to V2FileContractElement.
- Fixed syncer deadlocking when Connect is called after Close.
- Set 2 minute deadline for all incoming RPC in syncer

## 0.10.1 (2025-01-18)

### Fixes

- Update core to v0.9.1
- Use condition rather than polling to determine whether all peers are closed in 'Run'

## 0.10.0 (2025-01-15)

### Breaking Changes

#### Increased V2 Allow Height to 526,000

Delays activation of the v2 hardfork to June 6th, 2025 in response to concerns about the scope of updates necessary for partners to support v2

### Fixes

- Improve locking in SingleAddressWallet by avoiding acquiring the mutex before a db transaction
- Increased default UTXO reservation in SingleAddressWallet to 3 hours

## 0.9.1 (2025-01-12)

### Fixes

- Release locked host UTXOs if contract formation, renewal, or refresh fails
- Release locked UTXOs if contract formation, renewal, or refresh fails

## 0.9.0 (2024-12-19)

### Breaking Changes

#### Finalize V2 Hardfork Dates

The V2 hardfork is scheduled to modernize Sia's consensus protocol, which has been untouched since Sia's mainnet launch back in 2014, and improve accessibility of the storage network. To ensure a smooth transition from V1, it will be executed in two phases. Additional documentation on upgrading will be released in the near future.

#### V2 Highlights
- Drastically reduces blockchain size on disk
- Improves UTXO spend policies - including HTLC support for Atomic Swaps
- More efficient contract renewals - reducing lock up requirements for hosts and renters
- Improved transfer speeds - enables hot storage

#### Phase 1 - Allow Height
- **Activation Height:** `513400` (March 10th, 2025)
- **New Features:** V2 transactions, contracts, and RHP4
- **V1 Support:** Both V1 and V2 will be supported during this phase
- **Purpose:** This period gives time for integrators to transition from V1 to V2
- **Requirements:** Users will need to update to support the hardfork before this block height

#### Phase 2 - Require Height
- **Activation Height:** `526000` (June 6th, 2025)
- **New Features:** The consensus database can be trimmed to only store the Merkle proofs
- **V1 Support:** V1 will be disabled, including RHP2 and RHP3. Only V2 transactions will be accepted
- **Requirements:** Developers will need to update their apps to support V2 transactions and RHP4 before this block height

### Fixes

- Fix rhp4 server not returning ErrNotEnoughFunds when account has insufficient balance

## 0.8.0 (2024-12-13)

### Breaking Changes

#### Add revised and renewed fields to RPCLatestRevision

Adds two additional fields to the RPCLatestRevision response. The Revisable field indicates whether the host will accept further revisions to the contract. A host will not accept revisions too close to the proof window or revisions on contracts that have already been resolved. The Renewed field indicates whether the contract was renewed. If the contract was renewed, the renter can use FileContractID.V2RenewalID to get the ID of the new contract.

- Remove unused duration param from `rhp4.RPCWrite`

### Features

#### Add support for block pruning in v2

The chain manager can now automatically delete blocks after a configurable number of confirmations. Note that this does not apply retroactively.

### Fixes

- Return 0 balance for nonexistent accounts instead of an error
- Extended TestRPCRenew and TestRPCRefresh with an initial sector upload
- Fix panic when fetching block with empty block id from ChainManager
