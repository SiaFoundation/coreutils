# [![Sia Core](https://sia.tech/assets/banners/sia-banner-core.png)](http://sia.tech)

[![GoDoc](https://godoc.org/go.sia.tech/coreutils?status.svg)](https://godoc.org/go.sia.tech/coreutils)

`coreutils` provides the shared building blocks used to run a Sia node. Where
[`go.sia.tech/core`](https://github.com/SiaFoundation/core) defines the
consensus rules and wire types, `coreutils` implements the moving parts around
them: applying blocks and maintaining chain state, syncing with peers, managing
a wallet, and speaking the renter-host protocol. It is consumed by the reference
node implementations [`walletd`](https://github.com/SiaFoundation/walletd),
[`renterd`](https://github.com/SiaFoundation/renterd), and
[`hostd`](https://github.com/SiaFoundation/hostd).

## Installation

```sh
go get go.sia.tech/coreutils@latest
```

Requires Go 1.26 or later.

## Packages

### `chain`
Manages blockchain state. The `Manager` validates and applies blocks, tracks the
best chain across reorgs, maintains the transaction pool, and notifies
subscribers of changes. `NewDBStore` backs it with a pluggable key-value store
(`DB`); `chain.Mainnet()` and `chain.TestnetZen()` return the network parameters
and genesis block for the public networks.

```go
network, genesis := chain.Mainnet()
db, _ := coreutils.OpenBoltChainDB("consensus.db")
store, tipState, _ := chain.NewDBStore(db, network, genesis, nil)
cm := chain.NewManager(store, tipState)
```

### `syncer`
A P2P node that connects to peers, relays blocks and transactions, and keeps the
`chain.Manager` in sync with the network. It speaks the gateway protocol over a
`net.Listener` and persists peer data through a `PeerStore`.

### `wallet`
`SingleAddressWallet`, a wallet that derives a single address from a seed. It
tracks the address's UTXOs and events by following the chain, and selects and
signs inputs when funding transactions. `wallet.NewSeedPhrase` and the seed
helpers cover BIP-39 phrase generation and key derivation.

### `rhp/v4`
The renter-host protocol, version 4. It provides both the renter-side `RPC*`
client calls (form, renew, and refresh contracts; read, write, and manage
sectors; fund accounts) and the host-side `Server`. Two interchangeable
transports are included: `siamux` (the native mux over TCP) and `quic`.

### root package (`coreutils`)
Top-level helpers: `MineBlock` / `FindBlockNonce` for CPU mining (useful for
tests and dev networks) and `BoltChainDB`, a [bbolt](https://github.com/etcd-io/bbolt)-backed
implementation of the `chain.DB` interface.

### `threadgroup`
A small utility for coordinated shutdown — track in-flight goroutines and derive
contexts that cancel when the group is stopped.

### `testutil`
Helpers for writing tests against these packages: throwaway networks with early
hardfork activation, in-memory stores, block mining, and mock hosts.

## Contributing

Issues and pull requests are welcome. Please run `go test ./...` before
submitting changes.

## License

`coreutils` is released under the [MIT License](LICENSE).
