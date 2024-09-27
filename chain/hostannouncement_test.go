package chain

import (
	"encoding/binary"
	"math"
	"testing"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"lukechampine.com/frand"
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
	}
	ForEachHostAnnouncement(b, func(a HostAnnouncement) {
		if a.PublicKey != sk.PublicKey() {
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
	randomAttestation := types.Attestation{
		PublicKey: sk.PublicKey(),
		Key:       "foo",
		Value:     frand.Bytes(60),
	}
	cs := consensus.State{}
	randomAttestation.Signature = sk.SignHash(cs.AttestationSigHash(randomAttestation))

	invalidData := make([]byte, 100)
	binary.LittleEndian.PutUint64(invalidData, math.MaxUint64)
	extraBigAttestation := types.Attestation{
		PublicKey: sk.PublicKey(),
		Key:       attestationHostAnnouncement,
		Value:     invalidData,
	}
	extraBigAttestation.Signature = sk.SignHash(cs.AttestationSigHash(extraBigAttestation))

	b := types.Block{
		V2: &types.V2BlockData{
			Transactions: []types.V2Transaction{
				{Attestations: []types.Attestation{
					randomAttestation,
					extraBigAttestation,
					ha.ToAttestation(consensus.State{}, sk),
				}},
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
