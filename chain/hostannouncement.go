package chain

import (
	"bytes"
	"errors"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

const attestationHostAnnouncement = "HostAnnouncement"

var specifierHostAnnouncement = types.NewSpecifier("HostAnnouncement")

type (
	// A HostAnnouncement represents a signed announcement of a host's network
	// address. Announcements may be made via arbitrary data (in a v1 transaction)
	// or via attestation (in a v2 transaction).
	HostAnnouncement struct {
		PublicKey  types.PublicKey `json:"publicKey"`
		NetAddress string          `json:"netAddress"`
	}

	// A Protocol is a string identifying a network protocol that a host may be
	// reached on.
	Protocol string

	// A NetAddress is a pair of protocol and address that a host may be reached on
	NetAddress struct {
		Protocol Protocol `json:"protocol"`
		Address  string   `json:"address"`
	}

	// A V2HostAnnouncement lists all the network addresses a host may be reached on
	V2HostAnnouncement []NetAddress
)

// EncodeTo implements types.EncoderTo.
func (na NetAddress) EncodeTo(e *types.Encoder) {
	e.WriteString(string(na.Protocol))
	e.WriteString(na.Address)
}

// DecodeFrom implements types.DecoderFrom.
func (na *NetAddress) DecodeFrom(d *types.Decoder) {
	na.Protocol = Protocol(d.ReadString())
	na.Address = d.ReadString()
}

// ToAttestation encodes a host announcement as an attestation.
func (ha V2HostAnnouncement) ToAttestation(cs consensus.State, sk types.PrivateKey) types.Attestation {
	buf := bytes.NewBuffer(nil)
	e := types.NewEncoder(buf)
	types.EncodeSlice(e, ha)
	if err := e.Flush(); err != nil {
		panic(err) // should never happen
	}
	a := types.Attestation{
		PublicKey: sk.PublicKey(),
		Key:       attestationHostAnnouncement,
		Value:     buf.Bytes(),
	}
	a.Signature = sk.SignHash(cs.AttestationSigHash(a))
	return a
}

// FromAttestation decodes a host announcement from an attestation.
func (ha *V2HostAnnouncement) FromAttestation(a types.Attestation) error {
	if a.Key != attestationHostAnnouncement {
		return errors.New("not a host announcement")
	}
	d := types.NewBufDecoder(a.Value)
	types.DecodeSlice(d, (*[]NetAddress)(ha))
	return d.Err()
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
}

// ForEachV2HostAnnouncement calls fn on each v2 host announcement in a block.
func ForEachV2HostAnnouncement(b types.Block, fn func(types.PublicKey, []NetAddress)) {
	for _, txn := range b.V2Transactions() {
		for _, a := range txn.Attestations {
			var ha V2HostAnnouncement
			if err := ha.FromAttestation(a); err == nil {
				fn(a.PublicKey, ha)
			}
		}
	}
}
