package wallet

import (
	"time"

	"go.sia.tech/core/types"
)

// event types indicate the source of an event. Events can
// either be created by sending Siacoins between addresses or they can be
// created by consensus (e.g. a miner payout, a siafund claim, or a contract).
const (
	EventTypeMinerPayout       = "miner"
	EventTypeFoundationSubsidy = "foundation"

	EventTypeV1Transaction = "v1Transaction"
	EventTypeV1Contract    = "v1Contract"

	EventTypeV2Transaction = "v2Transaction"
	EventTypeV2Contract    = "v2Contract"
)

type (
	// An EventMinerPayout represents a miner payout from a block.
	EventMinerPayout struct {
		SiacoinElement types.SiacoinElement `json:"siacoinElement"`
	}

	// EventFoundationSubsidy represents a foundation subsidy from a block.
	EventFoundationSubsidy struct {
		SiacoinElement types.SiacoinElement `json:"siacoinElement"`
	}

	// An EventV1ContractPayout represents a file contract payout from a v1
	// contract.
	EventV1ContractPayout struct {
		FileContract   types.FileContractElement `json:"fileContract"`
		SiacoinElement types.SiacoinElement      `json:"siacoinElement"`
		Missed         bool                      `json:"missed"`
	}

	// An EventV2ContractPayout represents a file contract payout from a v2
	// contract.
	EventV2ContractPayout struct {
		FileContract   types.V2FileContractElement        `json:"fileContract"`
		Resolution     types.V2FileContractResolutionType `json:"resolution"`
		SiacoinElement types.SiacoinElement               `json:"siacoinElement"`
		Missed         bool                               `json:"missed"`
	}

	// An Event is a transaction or other event that affects the wallet including
	// miner payouts, siafund claims, and file contract payouts.
	Event struct {
		ID             types.Hash256    `json:"id"`
		Index          types.ChainIndex `json:"index"`
		Inflow         types.Currency   `json:"inflow"`
		Outflow        types.Currency   `json:"outflow"`
		Type           string           `json:"type"`
		Data           any              `json:"data"`
		MaturityHeight uint64           `json:"maturityHeight"`
		Timestamp      time.Time        `json:"timestamp"`
	}
)
