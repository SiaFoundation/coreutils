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
	}
	b := types.Block{
		Transactions: []types.Transaction{
			{ArbitraryData: [][]byte{ha.ToArbitraryData(sk)}},
		},
	}
	ForEachHostAnnouncement(b, func(pk types.PublicKey, a HostAnnouncement) {
		if pk != sk.PublicKey() {
			t.Error("pubkey mismatch")
		} else if a.NetAddress != ha.NetAddress {
			t.Error("address mismatch:", a, ha)
		}
	})
}

func TestForEachV2HostAnnouncement(t *testing.T) {
	sk := types.GeneratePrivateKey()
	ha := V2HostAnnouncement([]NetAddress{
		{Protocol: "tcp", Address: "foo.bar:1234"},
		{Protocol: "tcp6", Address: "baz.qux:5678"},
		{Protocol: "webtransport", Address: "quux.corge:91011"},
	})
	b := types.Block{
		V2: &types.V2BlockData{
			Transactions: []types.V2Transaction{
				{Attestations: []types.Attestation{ha.ToAttestation(consensus.State{}, sk)}},
			},
		},
	}
	ForEachV2HostAnnouncement(b, func(pk types.PublicKey, addresses []NetAddress) {
		if pk != sk.PublicKey() {
			t.Error("pubkey mismatch")
		} else if len(addresses) != len(ha) {
			t.Error("length mismatch")
		} else {
			for i := range addresses {
				if addresses[i] != ha[i] {
					t.Error("address mismatch:", addresses[i], ha[i])
				}
			}
		}
	})
}
