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
