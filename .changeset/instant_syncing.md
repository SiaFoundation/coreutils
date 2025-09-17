---
default: minor
---

# Instant syncing

#332 by @lukechampine

Adds a new chain.DB constructor and a helper function for fetching checkpoints.

Example usage:

```go
func main() {
	var peer string
	var index types.ChainIndex
	flag.TextVar(&index, "index", types.ChainIndex{}, "trusted chain index to bootstrap from")
	flag.StringVar(&peer, "peer", "", "peer to bootstrap from")
	flag.Parse()
	n, genesisBlock := chain.Mainnet()
	cs, b, err := syncer.SendCheckpoint(context.Background(), peer, index, n, genesisBlock.ID())
	if err != nil {
		log.Fatal(err)
	}
	store, tipState, err := chain.NewDBStoreAtCheckpoint(chain.NewMemDB(), cs, b, nil)
	if err != nil {
		log.Fatal(err)
	}
	cm := chain.NewManager(store, tipState)
	fmt.Println("Initialized chain.Manager at", cm.Tip())
}
```

```
./example -peer "174.97.109.105:9981" -index "530000::0000000000000000abb98e3b587fba3a0c4e723ac1e078e9d6a4d13d1d131a2c"
Initialized chain.Manager at 530000::1d131a2c
```
