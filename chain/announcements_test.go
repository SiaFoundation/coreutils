package chain

import (
	"bytes"
	"testing"

	"go.sia.tech/core/types"
)

func TestAnnouncementsSignature(t *testing.T) {
	t.Parallel()

	sk := types.GeneratePrivateKey()
	pk := sk.PublicKey()

	// create a v1 announcement
	v1Ann := V1Announcement{
		Specifier:  types.Specifier{0x01, 0x02, 0x03},
		NetAddress: "foo.bar:1234",
		PublicKey:  pk.UnlockKey(),
	}

	// sign it and verify signature
	v1Ann.Sign(sk)
	if v1Ann.Signature == (types.Signature{}) {
		t.Fatal("signature not set")
	} else if !v1Ann.VerifySignature() {
		t.Fatal("signature verification failed")
	}

	// change a field and verify signature fails
	v1Ann.NetAddress = "baz.qux:5678"
	if v1Ann.VerifySignature() {
		t.Fatal("signature verification succeeded")
	}
}

func TestForEachAnnon(t *testing.T) {
	t.Parallel()

	// create a v1 announcement
	sk := types.GeneratePrivateKey()
	pk := sk.PublicKey()
	v1Ann := V1Announcement{
		Specifier:  types.NewSpecifier(AnnouncementSpecifier),
		NetAddress: "foo.bar:1234",
		PublicKey:  pk.UnlockKey(),
	}
	v1Ann.Sign(sk)

	// encode it
	buf := new(bytes.Buffer)
	enc := types.NewEncoder(buf)
	v1Ann.EncodeTo(enc)
	enc.Flush()

	// create a block
	b := types.Block{
		Transactions: []types.Transaction{
			{
				ArbitraryData: [][]byte{buf.Bytes()},
			},
		},
		V2: &types.V2BlockData{
			Transactions: []types.V2Transaction{
				{
					Attestations: []types.Attestation{
						{
							PublicKey: pk,
							Key:       AnnouncementSpecifier,
							Value:     []byte(v1Ann.NetAddress),
							Signature: types.Signature{}, // not necessary
						},
					},
				},
			},
		},
	}

	// extract them
	var announcements []Announcement
	ForEachAnnouncement(b, func(a Announcement) {
		announcements = append(announcements, a)
	})
	if len(announcements) != 2 {
		t.Fatal("expected 1 announcement", len(announcements))
	}
	for _, a := range announcements {
		if a.NetAddress != v1Ann.NetAddress {
			t.Fatal("unexpected net address:", a.NetAddress)
		} else if uk := a.PublicKey.UnlockKey(); uk.Algorithm != v1Ann.PublicKey.Algorithm {
			t.Fatal("unexpected public key specifier:", uk.Algorithm)
		} else if !bytes.Equal(uk.Key, v1Ann.PublicKey.Key) {
			t.Fatal("unexpected public key:", uk.Key)
		}
	}
}
