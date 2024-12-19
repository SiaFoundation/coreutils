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
