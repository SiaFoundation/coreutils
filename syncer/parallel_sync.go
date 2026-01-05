package syncer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

func (s *Syncer) parallelSync(ctx context.Context, cs consensus.State, headers []types.BlockHeader) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()

	type Req struct {
		base      types.ChainIndex
		tip       types.ChainIndex
		numBlocks uint64
	}

	type Resp struct {
		blocks []types.Block
		states []consensus.State
		req    Req
		peer   *Peer
		err    error
	}

	// divide headers among requests, max 100 blocks per request
	const blocksPerReq = 100
	reqs := make([]Req, (len(headers)+blocksPerReq-1)/blocksPerReq)
	for i := range reqs {
		off := uint64(i) * blocksPerReq
		numBlocks := min(blocksPerReq, uint64(len(headers[off:])))
		base := types.ChainIndex{
			ID:     headers[off].ParentID,
			Height: cs.Index.Height + off,
		}
		tip := types.ChainIndex{
			ID:     headers[off+numBlocks-1].ID(),
			Height: base.Height + numBlocks - 1,
		}
		reqs[i] = Req{base, tip, numBlocks}
	}

	workFn := func(p *Peer, req Req) (resp Resp) {
		resp.req = req
		resp.peer = p
		startTime := time.Now()
		if req.base.Height >= cs.Network.HardforkV2.RequireHeight {
			cs, b, err := p.SendCheckpoint(req.base, cs.Network, s.config.SendBlockTimeout)
			if err != nil {
				return Resp{req: req, peer: p, err: err}
			}
			cs, _ = consensus.ApplyBlock(cs, b, consensus.V1BlockSupplement{}, time.Time{})
			blocks, _, err := p.SendV2Blocks(ctx, []types.BlockID{cs.Index.ID}, req.numBlocks, s.config.SendBlocksTimeout)
			if err != nil {
				return Resp{req: req, peer: p, err: err}
			} else if uint64(len(blocks)) != req.numBlocks {
				return Resp{req: req, peer: p, err: errors.New("peer returned wrong number of blocks")}
			} else if blocks[len(blocks)-1].ID() != req.tip.ID {
				return Resp{req: req, peer: p, err: errors.New("peer returned wrong blocks")}
			}
			resp.blocks = blocks
			for _, b := range blocks {
				if err := consensus.ValidateBlock(cs, b, consensus.V1BlockSupplement{}); err != nil {
					s.ban(p, fmt.Errorf("sent invalid block %v: %w", b.ID(), err))
					return Resp{req: req, peer: p, err: err}
				}
				cs, _ = consensus.ApplyBlock(cs, b, consensus.V1BlockSupplement{}, time.Time{})
				resp.states = append(resp.states, cs)
			}
		} else {
			blocks, _, err := p.SendV2Blocks(ctx, []types.BlockID{req.base.ID}, req.numBlocks, s.config.SendBlocksTimeout)
			if err != nil {
				return Resp{req: req, peer: p, err: err}
			} else if uint64(len(blocks)) != req.numBlocks {
				return Resp{req: req, peer: p, err: errors.New("peer returned wrong number of blocks")}
			}
			// verify that blocks match headers
			headers := headers[req.base.Height-cs.Index.Height:][:req.numBlocks]
			for i := range blocks {
				if blocks[i].ID() != headers[i].ID() {
					// note: this is not necessarily a ban-worthy offense, as it could
					// be caused by a peer on a fork that could be valid.
					return Resp{req: req, peer: p, err: errors.New("peer returned blocks that do not match header chain")}
				}
			}
			resp.blocks = blocks
		}
		endTime := time.Now()
		s.pm.UpdatePeerInfo(p.t.Addr, func(info *PeerInfo) {
			info.SyncedBlocks += req.numBlocks
			info.SyncDuration += endTime.Sub(startTime)
		})
		return
	}

	reqChan := make(chan Req, 128) // surely a reasonable maximum number of peers
	respChan := make(chan Resp, 128)

	// process results in a separate goroutine
	resps := make([]*Resp, len(reqs))
	finishCh := make(chan []*Resp, len(reqs))
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for resps := range finishCh {
			for _, r := range resps {
				var err error
				if r.req.base.Height >= cs.Network.HardforkV2.RequireHeight {
					err = s.cm.AddValidatedV2Blocks(r.blocks, r.states)
				} else {
					err = s.cm.AddBlocks(r.blocks)
				}
				if err != nil {
					s.ban(r.peer, fmt.Errorf("peer sent invalid blocks: %w", err))
					errCh <- err
					return
				}
			}
		}
		errCh <- nil
	}()

	// orchestrate work
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	seen := make(map[gateway.UniqueID]bool)
	reqIndex := 0
	finished := 0
	queueRequest := func() {
		if reqIndex < len(reqs) {
			reqChan <- reqs[reqIndex]
			reqIndex++
			// if we've reached the end, duplicate any incomplete jobs
			if reqIndex == len(reqs) {
				for i := range resps {
					if resps[i] == nil {
						reqChan <- reqs[i]
					}
				}
			}
		}
	}

	var once sync.Once
	closeFinishCh := func() { close(finishCh) }
	for {
		select {
		case <-ctx.Done():
			close(reqChan)
			once.Do(closeFinishCh)
			wg.Wait()
			return ctx.Err()

		case <-ticker.C:
			// spawn worker goroutines for any new peers
			s.mu.Lock()
			for _, p := range s.peers {
				if p.Err() != nil || p.Synced() || seen[p.t.UniqueID] {
					continue
				}
				seen[p.t.UniqueID] = true
				wg.Add(1)
				go func(p *Peer) {
					defer wg.Done()
					for req := range reqChan {
						resp := workFn(p, req)
						respChan <- resp
						if resp.err != nil {
							return
						}
					}
				}(p)
				queueRequest()
			}
			s.mu.Unlock()

		case r := <-respChan:
			// each time a request finishes, send a new one
			if r.err != nil {
				s.log.Warn("failed to fetch blocks", zap.Error(r.err), zap.Stringer("base", r.req.base), zap.Stringer("tip", r.req.tip))
				reqChan <- r.req // reassign to a different worker
				continue
			}

			// send batch of responses to be finished
			off := (r.req.base.Height - cs.Index.Height) / blocksPerReq
			if resps[off] == nil {
				resps[off] = &r
			}
			i := finished
			for finished < len(resps) && resps[finished] != nil {
				finished++
			}
			if batch := resps[i:finished]; len(batch) > 0 {
				finishCh <- batch
				if finished == len(resps) {
					once.Do(closeFinishCh)
				}
			}
			queueRequest()

		case err := <-errCh:
			close(reqChan)
			cancel()
			wg.Wait()
			if err != nil {
				return err
			}
			elapsed := time.Since(start)
			s.log.Info("finished sync batch", zap.Int("blocks", len(headers)), zap.Duration("elapsed", elapsed.Round(time.Millisecond)), zap.Float64("blocks/sec", float64(len(headers))/elapsed.Seconds()))
			return nil
		}
	}
}
