package chain

import (
	"errors"
	"fmt"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

type oldSiacoinElement types.SiacoinElement

func (oldSiacoinElement) Cast() (sce types.SiacoinElement) { return }

func (sce *oldSiacoinElement) DecodeFrom(d *types.Decoder) {
	sce.ID.DecodeFrom(d)
	sce.StateElement.DecodeFrom(d)
	(*types.V2SiacoinOutput)(&sce.SiacoinOutput).DecodeFrom(d)
	sce.MaturityHeight = d.ReadUint64()
}

type oldSiafundElement types.SiafundElement

func (oldSiafundElement) Cast() (sfe types.SiafundElement) { return }

func (sfe *oldSiafundElement) DecodeFrom(d *types.Decoder) {
	sfe.ID.DecodeFrom(d)
	sfe.StateElement.DecodeFrom(d)
	(*types.V2SiafundOutput)(&sfe.SiafundOutput).DecodeFrom(d)
	(*types.V2Currency)(&sfe.ClaimStart).DecodeFrom(d)
}

type oldFileContractElement types.FileContractElement

func (oldFileContractElement) Cast() (fce types.FileContractElement) { return }

func (fce *oldFileContractElement) DecodeFrom(d *types.Decoder) {
	fce.ID.DecodeFrom(d)
	fce.StateElement.DecodeFrom(d)
	fce.FileContract.DecodeFrom(d)
}

type oldTransactionSupplement consensus.V1TransactionSupplement

func (oldTransactionSupplement) Cast() (ts consensus.V1TransactionSupplement) { return }

func (ts *oldTransactionSupplement) DecodeFrom(d *types.Decoder) {
	types.DecodeSliceCast[oldSiacoinElement](d, &ts.SiacoinInputs)
	types.DecodeSliceCast[oldSiafundElement](d, &ts.SiafundInputs)
	types.DecodeSliceCast[oldFileContractElement](d, &ts.RevisedFileContracts)
	types.DecodeSliceFn(d, &ts.StorageProofs, func(d *types.Decoder) (sp consensus.V1StorageProofSupplement) {
		(*oldFileContractElement)(&sp.FileContract).DecodeFrom(d)
		return
	})
}

type oldBlockSupplement consensus.V1BlockSupplement

func (oldBlockSupplement) Cast() (bs consensus.V1BlockSupplement) { return }

func (bs *oldBlockSupplement) DecodeFrom(d *types.Decoder) {
	types.DecodeSliceCast[oldTransactionSupplement](d, &bs.Transactions)
	types.DecodeSliceCast[oldFileContractElement](d, &bs.ExpiringFileContracts)
}

type oldSupplementedBlock supplementedBlock

func (sb *oldSupplementedBlock) DecodeFrom(d *types.Decoder) {
	if v := d.ReadUint8(); v != 2 {
		d.SetErr(fmt.Errorf("incompatible version (%d)", v))
	}
	var b types.Block
	(*types.V2Block)(&b).DecodeFrom(d)
	sb.Block = &b
	sb.Header = b.Header()
	types.DecodePtrCast[oldBlockSupplement](d, &sb.Supplement)
}

type versionedState consensus.State

func (vs *versionedState) DecodeFrom(d *types.Decoder) {
	if v := d.ReadUint8(); v != 2 {
		d.SetErr(fmt.Errorf("incompatible version (%d)", v))
	}
	(*consensus.State)(vs).DecodeFrom(d)
}

// MigrateDB upgrades the database to the latest version.
func MigrateDB(db DB, n *consensus.Network) error {
	if db.Bucket(bVersion) == nil {
		return nil // nothing to migrate
	}
	dbs := &DBStore{
		db: db,
		n:  n,
	}
	var err error
	rewrite := func(bucket []byte, key []byte, from types.DecoderFrom, to types.EncoderTo) {
		if err != nil {
			return
		}
		b := dbs.bucket(bucket)
		val := b.getRaw(key)
		if val == nil {
			return
		}
		d := types.NewBufDecoder(val)
		from.DecodeFrom(d)
		if d.Err() != nil {
			err = d.Err()
			return
		}
		b.put(key, to)
		if dbs.shouldFlush() {
			dbs.Flush()
		}
	}

	version := dbs.bucket(bVersion).getRaw(bVersion)
	if len(version) != 1 {
		return errors.New("invalid version")
	}
	switch version[0] {
	case 1:
		var sb supplementedBlock
		for _, key := range db.BucketKeys(bBlocks) {
			rewrite(bBlocks, key, (*oldSupplementedBlock)(&sb), &sb)
		}
		var cs consensus.State
		for _, key := range db.BucketKeys(bStates) {
			rewrite(bStates, key, (*versionedState)(&cs), &cs)
		}
		var sce types.SiacoinElement
		for _, key := range db.BucketKeys(bSiacoinElements) {
			rewrite(bSiacoinElements, key, (*oldSiacoinElement)(&sce), &sce)
		}
		var sfe types.SiafundElement
		for _, key := range db.BucketKeys(bSiafundElements) {
			rewrite(bSiafundElements, key, (*oldSiafundElement)(&sfe), &sfe)
		}
		var fce types.FileContractElement
		for _, key := range db.BucketKeys(bFileContractElements) {
			if len(key) == 32 {
				rewrite(bFileContractElements, key, (*oldFileContractElement)(&fce), &fce)
			}
		}
		if err != nil {
			return err
		}
		dbs.bucket(bVersion).putRaw(bVersion, []byte{2})
		dbs.Flush()
		fallthrough
	case 2:
		// up-to-date
		return nil
	default:
		return fmt.Errorf("unrecognized version (%d)", version[0])
	}
}
