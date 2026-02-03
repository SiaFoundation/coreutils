package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func getTip(tipURL string) (index types.ChainIndex, err error) {
	resp, err := http.DefaultClient.Get(tipURL)
	if err != nil {
		return types.ChainIndex{}, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		r := io.LimitReader(resp.Body, 1024) // 1KiB
		msg, _ := io.ReadAll(r)
		return types.ChainIndex{}, fmt.Errorf("failed to get tip: %s", msg)
	}
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&index); err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to decode tip: %w", err)
	} else if index.Height == 0 || index.ID == (types.BlockID{}) {
		return types.ChainIndex{}, fmt.Errorf("invalid tip %q", index)
	}
	return index, nil
}

func defaultTipURL(network string) string {
	switch network {
	case "zen":
		return "https://api.siascan.com/zen/consensus/tip"
	case "anagami":
		return "https://api.siascan.com/anagami/consensus/tip"
	default:
		return "https://api.siascan.com/consensus/tip"
	}
}

func tryConnectToPeer(ctx context.Context, s *syncer.Syncer, peerAddr string, log *zap.Logger) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := s.Connect(ctx, peerAddr); err != nil {
		log.Warn("failed to connect to peer", zap.String("peer", peerAddr), zap.Error(err))
	} else {
		log.Debug("connected to peer", zap.String("peer", peerAddr))
	}
}

func main() {
	var (
		githubRef       string
		dir             string
		network         string
		tipURL          string
		updateFrequency time.Duration
		pruneTarget     uint64
		logLevel        zap.AtomicLevel
		cleanup         bool
	)

	flag.StringVar(&githubRef, "github.ref", "", "GitHub ref for logging purposes")
	flag.StringVar(&network, "network", "mainnet", "Network to sync")
	flag.StringVar(&dir, "dir", ".", "Directory to sync")
	flag.StringVar(&tipURL, "tip.url", "", "Tip URL to sync to")
	flag.DurationVar(&updateFrequency, "update.frequency", 10*time.Minute, "the time to wait for an update before failing")
	flag.Uint64Var(&pruneTarget, "prune.target", 0, "the target number of blocks to keep")
	flag.TextVar(&logLevel, "log.level", zap.NewAtomicLevelAt(zap.InfoLevel), "log level")
	flag.BoolVar(&cleanup, "cleanup", false, "cleanup the directory after syncing")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	var n *consensus.Network
	var genesis types.Block
	var bootstrapPeers []string
	switch network {
	case "mainnet":
		n, genesis = chain.Mainnet()
		bootstrapPeers = syncer.MainnetBootstrapPeers
	case "zen":
		n, genesis = chain.TestnetZen()
		bootstrapPeers = syncer.ZenBootstrapPeers
	default:
		log.Panic("unknown network", zap.String("network", network))
	}

	if tipURL == "" {
		tipURL = defaultTipURL(network)
	}

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

	dbPath := filepath.Join(dir, "consensus.db")
	db, err := coreutils.OpenBoltChainDB(dbPath)
	if err != nil {
		log.Panic("failed to open db", zap.Error(err))
	}
	defer db.Close()
	if cleanup {
		defer func() {
			os.Remove(dbPath)
		}()
	}

	store, tipState, err := chain.NewDBStore(db, n, genesis, chain.NewZapMigrationLogger(log.Named("migrate")))
	if err != nil {
		log.Panic("failed to create store", zap.Error(err))
	}
	cm := chain.NewManager(store, tipState, chain.WithLog(log.Named("chain")))
	log = log.With(zap.Stringer("start", cm.Tip()))

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Panic("failed to start listener", zap.Error(err))
	}
	defer l.Close()

	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		log.Panic("failed to split port", zap.Error(err))
	}

	s := syncer.New(l, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: "127.0.0.1:" + port,
	}, syncer.WithLogger(log.Named("syncer")))
	defer s.Close()
	go s.Run()

	go func(ctx context.Context) {
		for _, addr := range bootstrapPeers {
			select {
			case <-ctx.Done():
				return
			default:
				tryConnectToPeer(ctx, s, addr, log.Named("bootstrap"))
			}
		}
	}(ctx)

	log.Info("starting sync", zap.String("network", network), zap.String("dir", dir))

	reorgCh := make(chan types.ChainIndex, 1)
	var lastLog time.Time
	cm.OnReorg(func(ci types.ChainIndex) {
		if time.Since(lastLog) > 5*time.Second {
			// debounce
			log.Info("synced to", zap.Stringer("tip", ci))
			lastLog = time.Now()
		}
		reorgCh <- ci
	})

	for {
		select {
		case tip := <-reorgCh:
			// still syncing, prune blocks
			if tip.Height > 144 {
				cm.PruneBlocks(tip.Height - 144)
			}
			continue
		case <-ctx.Done():
			log.Info("shutting down")
			return
		case <-time.After(updateFrequency):
			// haven't received an update in a while, check if we're synced
			tip, err := getTip(tipURL)
			if err != nil {
				log.Panic("failed to get tip", zap.Error(err))
			} else if tip == cm.Tip() {
				log.Info("sync complete", zap.Stringer("tip", cm.Tip()))
				return
			} else {
				log.Panic("not synced", zap.Stringer("tip", cm.Tip()), zap.Stringer("expected", tip))
			}
		}
	}
}
