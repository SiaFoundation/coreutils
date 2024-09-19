package chain

import (
	"bytes"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

const attestationHostAnnouncement = "HostAnnouncement"

var specifierHostAnnouncement = types.NewSpecifier("HostAnnouncement")

// A HostAnnouncement represents a signed announcement of a host's network
// address. Announcements may be made via arbitrary data (in a v1 transaction)
// or via attestation (in a v2 transaction).
type HostAnnouncement struct {
	PublicKey  types.PublicKey `json:"publicKey"`
	NetAddress string          `json:"netAddress"`
}

// ToAttestation encodes a host announcement as an attestation.
func (ha HostAnnouncement) ToAttestation(cs consensus.State, sk types.PrivateKey) types.Attestation {
	if ha.PublicKey != sk.PublicKey() {
		panic("key mismatch") // developer error
	}
	a := types.Attestation{
		PublicKey: ha.PublicKey,
		Key:       attestationHostAnnouncement,
		Value:     []byte(ha.NetAddress),
	}
	a.Signature = sk.SignHash(cs.AttestationSigHash(a))
	return a
}

// FromAttestation decodes a host announcement from an attestation.
func (ha *HostAnnouncement) FromAttestation(a types.Attestation) bool {
	if a.Key != attestationHostAnnouncement {
		return false
	}
	ha.PublicKey = a.PublicKey
	ha.NetAddress = string(a.Value)
	return true
}

// ToArbitraryData encodes a host announcement as arbitrary data.
func (ha HostAnnouncement) ToArbitraryData(sk types.PrivateKey) []byte {
	if ha.PublicKey != sk.PublicKey() {
		panic("key mismatch") // developer error
	}

	buf := new(bytes.Buffer)
	e := types.NewEncoder(buf)
	specifierHostAnnouncement.EncodeTo(e)
	e.WriteString(ha.NetAddress)
	ha.PublicKey.UnlockKey().EncodeTo(e)
	e.Flush()
	sk.SignHash(types.HashBytes(buf.Bytes())).EncodeTo(e)
	e.Flush()
	return buf.Bytes()
}

// FromArbitraryData decodes a host announcement from arbitrary data.
func (ha *HostAnnouncement) FromArbitraryData(arb []byte) bool {
	var s types.Specifier
	var uk types.UnlockKey
	var sig types.Signature
	d := types.NewBufDecoder(arb)
	s.DecodeFrom(d)
	addr := d.ReadString()
	uk.DecodeFrom(d)
	sig.DecodeFrom(d)
	if err := d.Err(); err != nil ||
		s != specifierHostAnnouncement ||
		uk.Algorithm != types.SpecifierEd25519 ||
		len(uk.Key) < 32 ||
		len(arb) < len(sig) ||
		!types.PublicKey(uk.Key).VerifyHash(types.HashBytes(arb[:len(arb)-len(sig)]), sig) {
		return false
	}
	ha.NetAddress = addr
	ha.PublicKey = types.PublicKey(uk.Key)
	return true
}

// ForEachHostAnnouncement calls fn on each host announcement in a block.
func ForEachHostAnnouncement(b types.Block, fn func(HostAnnouncement)) {
	for _, txn := range b.Transactions {
		for _, arb := range txn.ArbitraryData {
			var ha HostAnnouncement
			if ha.FromArbitraryData(arb) {
				fn(ha)
			}
		}
	}
	for _, txn := range b.V2Transactions() {
		for _, a := range txn.Attestations {
			var ha HostAnnouncement
			if ha.FromAttestation(a) {
				fn(ha)
			}
		}
	}
}
