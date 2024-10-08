package chain

import (
	"testing"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

func TestForEachHostAnnouncement(t *testing.T) {
	sk := types.GeneratePrivateKey()
	ha := HostAnnouncement{
		NetAddress: "foo.bar:1234",
		PublicKey:  sk.PublicKey(),
	}
	b := types.Block{
		Transactions: []types.Transaction{
			{ArbitraryData: [][]byte{ha.ToArbitraryData(sk)}},
		},
		V2: &types.V2BlockData{
			Transactions: []types.V2Transaction{
				{Attestations: []types.Attestation{ha.ToAttestation(consensus.State{}, sk)}},
			},
		},
	}
	ForEachHostAnnouncement(b, func(a HostAnnouncement) {
		if a.PublicKey != sk.PublicKey() {
			t.Error("pubkey mismatch")
		} else if a.NetAddress != ha.NetAddress {
			t.Error("address mismatch:", a, ha)
		}
	})
}
