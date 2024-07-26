package wallet

import (
	"encoding/json"
	"fmt"
	"time"

	"go.sia.tech/core/types"
)

// event types indicate the source of an event. Events can
// either be created by sending Siacoins between addresses or they can be
// created by consensus (e.g. a miner payout, a siafund claim, or a contract).
const (
	EventTypeMinerPayout       = "miner"
	EventTypeFoundationSubsidy = "foundation"
	EventTypeSiafundClaim      = "siafundClaim"

	EventTypeV1Transaction        = "v1Transaction"
	EventTypeV1ContractResolution = "v1ContractResolution"

	EventTypeV2Transaction        = "v2Transaction"
	EventTypeV2ContractResolution = "v2ContractResolution"
)

type (
	// An EventPayout represents a miner payout, siafund claim, or foundation
	// subsidy.
	EventPayout struct {
		SiacoinElement types.SiacoinElement `json:"siacoinElement"`
	}

	// An EventV1ContractResolution represents a file contract resolution from a v1
	// contract.
	EventV1ContractResolution struct {
		Parent         types.FileContractElement `json:"parent"`
		SiacoinElement types.SiacoinElement      `json:"siacoinElement"`
		Missed         bool                      `json:"missed"`
	}

	// An EventV2ContractResolution represents a file contract payout from a v2
	// contract.
	EventV2ContractResolution struct {
		types.V2FileContractResolution
		SiacoinElement types.SiacoinElement `json:"siacoinElement"`
		Missed         bool                 `json:"missed"`
	}

	// EventV1Transaction is a transaction event that includes the transaction
	EventV1Transaction types.Transaction

	// EventV2Transaction is a transaction event that includes the transaction
	EventV2Transaction types.V2Transaction

	// EventData contains the data associated with an event.
	EventData interface {
		isEvent() bool
	}

	// An Event is a transaction or other event that affects the wallet including
	// miner payouts, siafund claims, and file contract payouts.
	Event struct {
		ID             types.Hash256    `json:"id"`
		Index          types.ChainIndex `json:"index"`
		Inflow         types.Currency   `json:"inflow"`
		Outflow        types.Currency   `json:"outflow"`
		Type           string           `json:"type"`
		Data           EventData        `json:"data"`
		MaturityHeight uint64           `json:"maturityHeight"`
		Timestamp      time.Time        `json:"timestamp"`
	}
)

func (EventPayout) isEvent() bool               { return true }
func (EventV1Transaction) isEvent() bool        { return true }
func (EventV1ContractResolution) isEvent() bool { return true }
func (EventV2Transaction) isEvent() bool        { return true }
func (EventV2ContractResolution) isEvent() bool { return true }

// UnmarshalJSON implements the json.Unmarshaler interface.
func (e *Event) UnmarshalJSON(b []byte) error {
	var je struct {
		ID             types.Hash256    `json:"id"`
		Index          types.ChainIndex `json:"index"`
		Inflow         types.Currency   `json:"inflow"`
		Outflow        types.Currency   `json:"outflow"`
		Type           string           `json:"type"`
		Data           json.RawMessage  `json:"data"`
		MaturityHeight uint64           `json:"maturityHeight"`
		Timestamp      time.Time        `json:"timestamp"`
	}
	if err := json.Unmarshal(b, &je); err != nil {
		return err
	}

	e.ID = je.ID
	e.Index = je.Index
	e.Inflow = je.Inflow
	e.Outflow = je.Outflow
	e.Type = je.Type
	// Data is unmarshaled based on the event type below
	e.MaturityHeight = je.MaturityHeight
	e.Timestamp = je.Timestamp

	var err error
	switch je.Type {
	case EventTypeMinerPayout, EventTypeFoundationSubsidy, EventTypeSiafundClaim:
		var data EventPayout
		err = json.Unmarshal(je.Data, &data)
		e.Data = data
	case EventTypeV1ContractResolution:
		var data EventV1ContractResolution
		err = json.Unmarshal(je.Data, &data)
		e.Data = data
	case EventTypeV2ContractResolution:
		var data EventV2ContractResolution
		err = json.Unmarshal(je.Data, &data)
		e.Data = data
	case EventTypeV1Transaction:
		var data EventV1Transaction
		err = json.Unmarshal(je.Data, &data)
		e.Data = data
	case EventTypeV2Transaction:
		var data EventV2Transaction
		err = json.Unmarshal(je.Data, &data)
		e.Data = data
	default:
		return fmt.Errorf("unknown event type: %v", je.Type)
	}
	return err
}
