package chain

import (
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

func parseAddr(s string) types.Address {
	addr, err := types.ParseAddress(s)
	if err != nil {
		panic(err)
	}
	return addr
}

// Mainnet returns the network parameters and genesis block for the mainnet Sia
// blockchain.
func Mainnet() (*consensus.Network, types.Block) {
	n := &consensus.Network{
		Name: "mainnet",

		InitialCoinbase: types.Siacoins(300000),
		MinimumCoinbase: types.Siacoins(30000),
		InitialTarget:   types.BlockID{4: 32},
		BlockInterval:   10 * time.Minute,
		MaturityDelay:   144,
	}
	n.HardforkDevAddr.Height = 10000
	n.HardforkDevAddr.OldAddress = parseAddr("7d0c44f7664e2d34e53efde0661a6f628ec9264785ae8e3cd7c973e8d190c3c97b5e3ecbc567")
	n.HardforkDevAddr.NewAddress = parseAddr("f371c70bce9eb8979cd5099f599ec4e4fcb14e0afcf31f9791e03e6496a4c0b358c98279730b")

	n.HardforkTax.Height = 21000

	n.HardforkStorageProof.Height = 100000

	n.HardforkOak.Height = 135000
	n.HardforkOak.FixHeight = 139000
	n.HardforkOak.GenesisTimestamp = time.Unix(1433600000, 0) // June 6th, 2015 @ 2:13pm UTC

	n.HardforkASIC.Height = 179000
	n.HardforkASIC.OakTime = 120000 * time.Second
	n.HardforkASIC.OakTarget = types.BlockID{8: 32}

	n.HardforkFoundation.Height = 298000
	n.HardforkFoundation.PrimaryAddress = parseAddr("053b2def3cbdd078c19d62ce2b4f0b1a3c5e0ffbeeff01280efb1f8969b2f5bb4fdc680f0807")
	n.HardforkFoundation.FailsafeAddress = parseAddr("27c22a6c6e6645802a3b8fa0e5374657438ef12716d2205d3e866272de1b644dbabd53d6d560")

	n.HardforkV2.AllowHeight = 1000000   // TBD
	n.HardforkV2.RequireHeight = 1025000 // ~six months later

	b := types.Block{
		Timestamp: n.HardforkOak.GenesisTimestamp,
		Transactions: []types.Transaction{{
			SiafundOutputs: []types.SiafundOutput{
				{Address: parseAddr("0439e5bc7f14ccf5d3a7e882d040923e45625166dd077b64466bc771791ac6fcec1c01394436"), Value: 2},
				{Address: parseAddr("049e1d2a69772b058a48bebe65724ff3bdf8d0971ebbe994e1e91c9f13e84bf4cbfe00accf31"), Value: 6},
				{Address: parseAddr("080742fa194af76ca24fdc97cae4f10b828a0df8c1a788c5413feaaecdd847e60a82a3240411"), Value: 7},
				{Address: parseAddr("2c6aef338a66f213ccc5f8b2db7a98fb13143420af20049c4921a3a5deb8d9dad0132f50b83e"), Value: 8},
				{Address: parseAddr("2ca31fe94a673784e69f614e9593416ea4d369ad9e1dca2b55d9554b5325cddf0a9dce86b3ed"), Value: 3},
				{Address: parseAddr("33979254c7073b596face3c83e37a5fdeeba1c912f89c80f46c7bb7df368b3f0b3558938b515"), Value: 1},
				{Address: parseAddr("3576fde5fee51c83e99c6c3ac59811a04afc0b3170f04277286272fb0556e975db9d7c89f72a"), Value: 10},
				{Address: parseAddr("38db03321c03a65f8da3ca233cc7db0a97b0e461b085bd21d3ca53c51fd0fec1f15547ae6827"), Value: 50},
				{Address: parseAddr("44be8c5760e89620a1b1cc41e4df57d9865a1938332d486b810c1dca0607320d17e8d839d6dd"), Value: 75},
				{Address: parseAddr("450ec9c85a49f52d9a5ea113c7f1cb380d3f05dc79f5f734c2b5fc4c82067224b122c5e76e6b"), Value: 10},
				{Address: parseAddr("4880fdcfa930011aedcda966c4e02aba5f973be2cb88fbdfa526586e2fd579e07734971fb805"), Value: 10},
				{Address: parseAddr("4882a4e3da1c3c0f3897d4f24d83e8832a3984ad717642b7264f60b2696c1af78e4a9a422fee"), Value: 50},
				{Address: parseAddr("4ad23ae46f45fd7835c36e1a734cd3cac79fcc0e4e5c0e83fa168dec9a2c278716b8262bc763"), Value: 10},
				{Address: parseAddr("55c69a29c474e272ca5ed6935754f7a4c34f3a7b1a214441744fb5f1f1d0d7b84a9dc9c8570f"), Value: 15},
				{Address: parseAddr("57ef537d980e1316cb882ec0cb57e0be4dec7d128edf92461017fc1364455b6f51b1fa676f01"), Value: 121},
				{Address: parseAddr("5bc9650bbc28236fec851f7c61f68c888ff598ae6ff5bc7c157dbbc0cb5cfd392840fc664354"), Value: 222},
				{Address: parseAddr("6ef0eead4e8ab98ab3e3879936842e3ee2cecc23ae6b9c0f8e025d84a33c3259d13856c1e0dd"), Value: 10},
				{Address: parseAddr("723a932c404548b841b2d55e9d2c586a5c1f91c1d7c8d7e9637424c5a0464f99e3239e72af2b"), Value: 3},
				{Address: parseAddr("7b6ae565dcfc32cb26b78598faa7d29bfc66961dbb03b2350b918f21a673fa28af705f308973"), Value: 1},
				{Address: parseAddr("7c65cfaf3277cf1a3e0ff78d96ae49f7ee1c4dffde68a6f47056e350d72d458fb56774b79ac5"), Value: 5},
				{Address: parseAddr("82b8480fe34fd9cd78fe43450a314cc2de1ef23e58b333751ba68c060716deb9a9e0b6e57bff"), Value: 10},
				{Address: parseAddr("8689c6ac60362d0a64805be1e2868f6c1f46bbe436d446e5953940a6997beeb41ade41874fd4"), Value: 25},
				{Address: parseAddr("8ffd76e56db58de05b907ba0cbdd7768ac0d694dabb97a36e5a80682a082b6970c6f75ba9fe1"), Value: 1},
				{Address: parseAddr("936cf91024f96cb8c4d4f178db3f2db8563560cf8260d2fb8809c1a083c6ddb95ff49f2dcc2b"), Value: 8},
				{Address: parseAddr("9b4f591c4547efc6f602c6fe5c3bc0cde59824ba6e7ae9dd4c8f03ee59e7c0170f50b34bd466"), Value: 58},
				{Address: parseAddr("9c204c69d52e42321b5538096ac15091136554b191047d1c4ffc2b53766ecef779841cccf546"), Value: 2},
				{Address: parseAddr("9da98618fe163abc7757c9ee37a8c283581227a82502c6c25dca7492bd116c2c2e5a86444683"), Value: 23},
				{Address: parseAddr("9e336824f2724310a8e6046ff148050eb666a99c90dc6775df083abb7c66502c56b50ade1bbe"), Value: 10},
				{Address: parseAddr("a0af3b21df1e523c226e1ccbf95d0310da0cface8ae7554345bf44c6a0579a449147262278ed"), Value: 1},
				{Address: parseAddr("a35e33dc0e9053703e0a00ada1ead3b0ba5409bdfa6f21e77257644b48d90b1ae624efa81a35"), Value: 75},
				{Address: parseAddr("aa078a74cd1484c5a6fb4b5d45066df4d477ad72221219156fcbcbfd8a681b2401feb5794149"), Value: 3},
				{Address: parseAddr("ad788068ba56978cbf17e7c14df5f368c4379bf36f0f548b94bbad2f68458d2737e27e3ab0f1"), Value: 90},
				{Address: parseAddr("b3b9e4a68b5e0dc1ffe3ae6378696dddf7049bf3e5251a62de0c5b50df213d386b6c17b6b3d1"), Value: 20},
				{Address: parseAddr("c1316714aa87b65595129fc29878a2d0319edcbc724f01833e1b5639f42e40423fad6b983ec8"), Value: 5},
				{Address: parseAddr("c4472dde00150c79c5e065412839137770cda617025b4be7458fdd44f54b0734caecae6c80eb"), Value: 1},
				{Address: parseAddr("c4d6ecd3e3d8987fa402eb0eeb2e8ee753260783d01db3bd3e5881b4779ed661845aa2af4e21"), Value: 44},
				{Address: parseAddr("ce3a7294833157c55612d81a3e4f98af210484a06ce735c8304c7d5e9c552082ac1f789b0e3c"), Value: 23},
				{Address: parseAddr("c867877ec502cb3ff106f5c3dc661b4ae8f9c956cf22331ab497886c7038844822ada408c0a1"), Value: 80},
				{Address: parseAddr("c8f9f5da3afd4cfa587246ef0e02fa7b0ac0c63dbb9bf798a5aec6188e27b177f3bb2c91f98b"), Value: 2},
				{Address: parseAddr("d101c7b8ba39158921fcdbb8822620623ffcfa4f4692a94eb4a11d5d262dafb015701c1f3ad2"), Value: 1},
				{Address: parseAddr("d46be92bb98a4ffd0cedd611dbc6975c518111788b3a42777edc8488036c393a84e5e9d47013"), Value: 2},
				{Address: parseAddr("d6f492adad5021b91d854da7b90126176fb3689669a2781af53f727734012cdeb00112f1695a"), Value: 3},
				{Address: parseAddr("d9daac103586a0e22c8a5d35b53e04d1be1b005d6911a93d62918370793761b8ef4e7df47eb8"), Value: 1},
				{Address: parseAddr("dfa2ac3736c1258ec8d5e630ba91b8ce0fe1a713254626308757cd51bbedb5b4e0474feb510f"), Value: 1},
				{Address: parseAddr("f12e8b29283f2fa983ad7cf6e4d5662c64d93eed859af845e40224ce2ffaf9aacfea794fb954"), Value: 1},
				{Address: parseAddr("f132e5d3422073f17557b4ef4cf60e8169b5996969cbe5ed1782c1aa64c9264785a9b56481f6"), Value: 50},
				{Address: parseAddr("7d0c44f7664e2d34e53efde0661a6f628ec9264785ae8e3cd7c973e8d190c3c97b5e3ecbc567"), Value: 8841},
			},
		}},
	}

	return n, b
}

// TestnetZen returns the chain parameters and genesis block for the "Zen"
// testnet chain.
func TestnetZen() (*consensus.Network, types.Block) {
	n := &consensus.Network{
		Name: "zen",

		InitialCoinbase: types.Siacoins(300000),
		MinimumCoinbase: types.Siacoins(300000),
		InitialTarget:   types.BlockID{3: 1},
		BlockInterval:   10 * time.Minute,
		MaturityDelay:   144,
	}

	n.HardforkDevAddr.Height = 1
	n.HardforkDevAddr.OldAddress = types.Address{}
	n.HardforkDevAddr.NewAddress = types.Address{}

	n.HardforkTax.Height = 2

	n.HardforkStorageProof.Height = 5

	n.HardforkOak.Height = 10
	n.HardforkOak.FixHeight = 12
	n.HardforkOak.GenesisTimestamp = time.Unix(1673600000, 0) // January 13, 2023 @ 08:53 GMT

	n.HardforkASIC.Height = 20
	n.HardforkASIC.OakTime = 10000 * time.Second
	n.HardforkASIC.OakTarget = types.BlockID{3: 1}

	n.HardforkFoundation.Height = 30
	n.HardforkFoundation.PrimaryAddress = parseAddr("053b2def3cbdd078c19d62ce2b4f0b1a3c5e0ffbeeff01280efb1f8969b2f5bb4fdc680f0807")
	n.HardforkFoundation.FailsafeAddress = types.VoidAddress

	n.HardforkV2.AllowHeight = 100000   // TBD
	n.HardforkV2.RequireHeight = 102000 // ~two weeks later

	b := types.Block{
		Timestamp: n.HardforkOak.GenesisTimestamp,
		Transactions: []types.Transaction{{
			SiacoinOutputs: []types.SiacoinOutput{{
				Address: parseAddr("3d7f707d05f2e0ec7ccc9220ed7c8af3bc560fbee84d068c2cc28151d617899e1ee8bc069946"),
				Value:   types.Siacoins(1).Mul64(1e12),
			}},
			SiafundOutputs: []types.SiafundOutput{{
				Address: parseAddr("053b2def3cbdd078c19d62ce2b4f0b1a3c5e0ffbeeff01280efb1f8969b2f5bb4fdc680f0807"),
				Value:   10000,
			}},
		}},
	}

	return n, b
}

// TestnetAnagami returns the chain parameters and genesis block for the "Anagami"
// testnet chain.
func TestnetAnagami() (*consensus.Network, types.Block) {
	// use a modified version of Zen
	n, genesis := TestnetZen()

	n.Name = "anagami"
	n.HardforkOak.GenesisTimestamp = time.Date(2024, time.August, 22, 0, 0, 0, 0, time.UTC)
	n.HardforkV2.AllowHeight = 2016         // ~2 weeks in
	n.HardforkV2.RequireHeight = 2016 + 288 // ~2 days later

	n.HardforkFoundation.PrimaryAddress = parseAddr("241352c83da002e61f57e96b14f3a5f8b5de22156ce83b753ea495e64f1affebae88736b2347")
	n.HardforkFoundation.FailsafeAddress = types.VoidAddress

	// move the genesis airdrops for easier testing
	genesis.Transactions[0].SiacoinOutputs = []types.SiacoinOutput{{
		Address: parseAddr("241352c83da002e61f57e96b14f3a5f8b5de22156ce83b753ea495e64f1affebae88736b2347"),
		Value:   types.Siacoins(1).Mul64(1e12),
	}}
	genesis.Transactions[0].SiafundOutputs = []types.SiafundOutput{{
		Address: parseAddr("241352c83da002e61f57e96b14f3a5f8b5de22156ce83b753ea495e64f1affebae88736b2347"),
		Value:   10000,
	}}
	return n, genesis
}
