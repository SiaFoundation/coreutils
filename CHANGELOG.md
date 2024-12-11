## 0.7.1 (2024-12-11)

### Features

#### Add support for block pruning

The chain manager can now automatically delete blocks after a configurable number of confirmations. Note that this does not apply retroactively.

### Fixes

- remove duration param from RPCWrite impl
- Return 0 for nonexistent accounts
- Automate releases
- Fix panic when fetching block with empty block id from ChainManager
