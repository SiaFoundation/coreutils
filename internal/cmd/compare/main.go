package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"go.etcd.io/bbolt"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func encHeight(h uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, h)
	return buf
}

func bucketsEqual(ctx context.Context, ba, bb *bbolt.Bucket) (n int, err error) {
	if ba == nil {
		return 0, fmt.Errorf("bucket A is nil")
	} else if bb == nil {
		return 0, fmt.Errorf("bucket B is nil")
	}

	checked := make(map[string]bool)
	err = ba.ForEach(func(k []byte, va []byte) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		vb := bb.Get(k)
		if !bytes.Equal(va, vb) {
			return fmt.Errorf("mismatch: key=%s, valueA=%s, valueB=%s", string(k), string(va), string(vb))
		}
		checked[types.HashBytes(k).String()] = true
		return nil
	})
	if err != nil {
		return len(checked), fmt.Errorf("failed to iterate over bucket A: %w", err)
	}

	err = bb.ForEach(func(k []byte, vb []byte) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		va := ba.Get(k)
		if !bytes.Equal(va, vb) {
			return fmt.Errorf("mismatch: key=%s, valueA=%s, valueB=%s", string(k), string(va), string(vb))
		}
		checked[types.HashBytes(k).String()] = true
		return nil
	})
	if err != nil {
		return len(checked), fmt.Errorf("failed to iterate over bucket B: %w", err)
	}
	return len(checked), nil
}

func main() {
	var (
		githubRef string
		logLevel  zap.AtomicLevel
	)

	flag.StringVar(&githubRef, "github.ref", "", "GitHub ref for logging purposes")
	flag.TextVar(&logLevel, "log.level", zap.NewAtomicLevelAt(zap.InfoLevel), "log level")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.EncodeDuration = zapcore.StringDurationEncoder
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder

	cfg.StacktraceKey = ""
	cfg.CallerKey = ""
	encoder := zapcore.NewConsoleEncoder(cfg)

	log := zap.New(zapcore.NewCore(encoder, zapcore.Lock(os.Stdout), logLevel))
	defer log.Sync()

	if githubRef == "" {
		meta, err := getGitMeta()
		if err != nil {
			log.Panic("failed to get git meta", zap.Error(err))
		} else if meta.Tag != "" {
			githubRef = meta.Tag
		} else if meta.ShortCommit != "" {
			githubRef = meta.ShortCommit
		}
	}
	log = log.With(zap.String("commit", githubRef))
	zap.RedirectStdLog(log)

	switch flag.Arg(0) {
	case "tree":
		dbA, err := bbolt.Open(flag.Arg(1), 0600, nil)
		if err != nil {
			log.Panic("failed to open bolt chain db", zap.Error(err))
		}
		defer dbA.Close()

		dbB, err := bbolt.Open(flag.Arg(2), 0600, nil)
		if err != nil {
			log.Panic("failed to open bolt chain db", zap.Error(err))
		}
		defer dbB.Close()

		var (
			bucketMainChain = []byte("MainChain")
			bucketTree      = []byte("Tree")
			bucketStates    = []byte("States")
		)

		txA, err := dbA.Begin(false)
		if err != nil {
			log.Panic("failed to begin transaction", zap.Error(err))
		}
		defer txA.Rollback()

		txB, err := dbB.Begin(false)
		if err != nil {
			log.Panic("failed to begin transaction", zap.Error(err))
		}
		defer txB.Rollback()

		heightA := binary.BigEndian.Uint64(txA.Bucket(bucketMainChain).Get([]byte("Height")))
		heightB := binary.BigEndian.Uint64(txB.Bucket(bucketMainChain).Get([]byte("Height")))

		if heightA != heightB {
			log.Panic("heights do not match", zap.Uint64("heightA", heightA), zap.Uint64("heightB", heightB))
		}

		tipIDA := (types.BlockID)(txA.Bucket(bucketMainChain).Get(encHeight(heightA)))
		tipIDB := (types.BlockID)(txB.Bucket(bucketMainChain).Get(encHeight(heightB)))
		if tipIDA != tipIDB {
			log.Panic("tip IDs do not match", zap.String("tipIDA", tipIDA.String()), zap.String("tipIDB", tipIDB.String()))
		}

		log.Info("checking states")

		stateBucketA := txA.Bucket(bucketStates)
		stateBucketB := txB.Bucket(bucketStates)

		bufA := stateBucketA.Get(tipIDA[:])
		bufB := stateBucketB.Get(tipIDB[:])

		var stateA, stateB consensus.State
		dec := types.NewBufDecoder(bufA[1:]) // version prefix
		stateA.DecodeFrom(dec)
		if err := dec.Err(); err != nil {
			log.Panic("failed to decode state A", zap.Error(err))
		}

		dec = types.NewBufDecoder(bufB[1:]) // version prefix
		stateB.DecodeFrom(dec)
		if err := dec.Err(); err != nil {
			log.Panic("failed to decode state B", zap.Error(err))
		}

		if stateA != stateB {
			log.Panic("states do not match", zap.Any("stateA", stateA), zap.Any("stateB", stateB))
		}

		log.Info("finished checking states")

		log.Info("checking state tree")

		treeBucketA := txA.Bucket(bucketTree)
		treeBucketB := txB.Bucket(bucketTree)

		n, err := bucketsEqual(ctx, treeBucketA, treeBucketB)
		if err != nil {
			log.Panic("failed to compare state tree buckets", zap.Error(err), zap.Int("checked", n))
		}
		log.Info("state tree checked", zap.Int("leaves", n))
	default:
		log.Error("unknown command")
	}
}
