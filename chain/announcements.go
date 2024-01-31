package chain

import (
	"bytes"

	"go.sia.tech/core/types"
)

// AnnouncementSpecifier is the specifier used in the arbitrary data section
// (for v1 txns) and the attestation value (for v2 txns) when a host announces
// its net address.
const AnnouncementSpecifier = "HostAnnouncement"

type (
	// Announcement represents a v1 or v2 host announcement after it has been
	// validated.
	Announcement struct {
		NetAddress string
		PublicKey  types.PublicKey
	}

	// V1Announcement represents a host announcement in v1 as stored in the
	// arbitrary data section of a transaction.
	V1Announcement struct {
		Specifier  types.Specifier
		NetAddress string
		PublicKey  types.UnlockKey
		Signature  types.Signature
	}
)

// DecodeFrom decodes a signed announcement.
func (a *V1Announcement) DecodeFrom(d *types.Decoder) {
	a.Specifier.DecodeFrom(d)
	a.NetAddress = d.ReadString()
	a.PublicKey.DecodeFrom(d)
	a.Signature.DecodeFrom(d)
}

// EncodeTo encodes a signed announcement.
func (a V1Announcement) EncodeTo(e *types.Encoder) {
	a.Specifier.EncodeTo(e)
	e.WriteString(a.NetAddress)
	a.PublicKey.EncodeTo(e)
	a.Signature.EncodeTo(e)
}

// VerifySignature verifies the signature of the announcement using its public
// key. This can only succeed if the PublicKey is a valid ed25519 public key.
func (a V1Announcement) VerifySignature() bool {
	sigHash := a.sigHash()
	var pk types.PublicKey
	copy(pk[:], a.PublicKey.Key)
	return pk.VerifyHash(sigHash, a.Signature)
}

// Sign signs the announcement using the given private key.
func (a *V1Announcement) Sign(sk types.PrivateKey) {
	sigHash := a.sigHash()
	a.Signature = sk.SignHash(sigHash)
}

// sigHash returns the hash that is signed by the announcement's signature.
func (a V1Announcement) sigHash() types.Hash256 {
	buf := new(bytes.Buffer)
	e := types.NewEncoder(buf)
	a.Specifier.EncodeTo(e)
	e.WriteString(a.NetAddress)
	a.PublicKey.EncodeTo(e)
	e.Flush()
	return types.HashBytes(buf.Bytes())
}

// ForEachAnnouncement calls fn on each host announcement in a block.
func ForEachAnnouncement(b types.Block, fn func(Announcement)) {
	for _, txn := range b.Transactions {
		for _, arb := range txn.ArbitraryData {
			// decode announcement
			var ha V1Announcement
			dec := types.NewBufDecoder(arb)
			ha.DecodeFrom(dec)
			if err := dec.Err(); err != nil {
				continue
			} else if ha.Specifier != types.NewSpecifier(AnnouncementSpecifier) {
				continue
			} else if !ha.VerifySignature() {
				continue
			}
			var pk types.PublicKey
			copy(pk[:], ha.PublicKey.Key)
			fn(Announcement{
				NetAddress: ha.NetAddress,
				PublicKey:  pk,
			})
		}
	}
	for _, txn := range b.V2Transactions() {
		for _, att := range txn.Attestations {
			if att.Key != AnnouncementSpecifier {
				continue
			}
			fn(Announcement{
				NetAddress: string(att.Value),
				PublicKey:  att.PublicKey,
			})
		}
	}
}
