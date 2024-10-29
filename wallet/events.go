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

	// An EventV1Transaction pairs a v1 transaction with its spent siacoin and
	// siafund elements.
	EventV1Transaction struct {
		Transaction types.Transaction `json:"transaction"`
		// v1 siacoin inputs do not describe the value of the spent utxo
		SpentSiacoinElements []types.SiacoinElement `json:"spentSiacoinElements,omitempty"`
		// v1 siafund inputs do not describe the value of the spent utxo
		SpentSiafundElements []types.SiafundElement `json:"spentSiafundElements,omitempty"`
	}

	// An EventV1ContractResolution represents a file contract payout from a v1
	// contract.
	EventV1ContractResolution struct {
		Parent         types.FileContractElement `json:"parent"`
		SiacoinElement types.SiacoinElement      `json:"siacoinElement"`
		Missed         bool                      `json:"missed"`
	}

	// An EventV2ContractResolution represents a file contract payout from a v2
	// contract.
	EventV2ContractResolution struct {
		Resolution     types.V2FileContractResolution `json:"resolution"`
		SiacoinElement types.SiacoinElement           `json:"siacoinElement"`
		Missed         bool                           `json:"missed"`
	}

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
		Confirmations  uint64           `json:"confirmations"`
		Type           string           `json:"type"`
		Data           EventData        `json:"data"`
		MaturityHeight uint64           `json:"maturityHeight"`
		Timestamp      time.Time        `json:"timestamp"`
		Relevant       []types.Address  `json:"relevant,omitempty"`
	}
)

func (EventPayout) isEvent() bool               { return true }
func (EventV1Transaction) isEvent() bool        { return true }
func (EventV1ContractResolution) isEvent() bool { return true }
func (EventV2Transaction) isEvent() bool        { return true }
func (EventV2ContractResolution) isEvent() bool { return true }

// SiacoinOutflow calculates the sum of Siacoins that were spent by relevant
// addresses
func (e *Event) SiacoinOutflow() types.Currency {
	relevant := make(map[types.Address]bool)
	for _, addr := range e.Relevant {
		relevant[addr] = true
	}

	switch data := e.Data.(type) {
	case EventPayout, EventV1ContractResolution, EventV2ContractResolution:
		// payout events cannot have outflows
		return types.ZeroCurrency
	case EventV1Transaction:
		var inflow types.Currency
		for _, se := range data.SpentSiacoinElements {
			if !relevant[se.SiacoinOutput.Address] {
				continue
			}
			inflow = inflow.Add(se.SiacoinOutput.Value)
		}
		return inflow
	case EventV2Transaction:
		var inflow types.Currency
		for _, se := range data.SiacoinInputs {
			if !relevant[se.Parent.SiacoinOutput.Address] {
				continue
			}
			inflow = inflow.Add(se.Parent.SiacoinOutput.Value)
		}
		return inflow
	default:
		panic(fmt.Errorf("unknown event type: %T", e.Data))
	}
}

// SiacoinInflow calculates the sum of Siacoins that were received by relevant
// addresses
func (e *Event) SiacoinInflow() types.Currency {
	relevant := make(map[types.Address]bool)
	for _, addr := range e.Relevant {
		relevant[addr] = true
	}

	switch data := e.Data.(type) {
	case EventPayout:
		return data.SiacoinElement.SiacoinOutput.Value
	case EventV1ContractResolution:
		return e.Data.(EventV1ContractResolution).SiacoinElement.SiacoinOutput.Value
	case EventV2ContractResolution:
		return e.Data.(EventV2ContractResolution).SiacoinElement.SiacoinOutput.Value
	case EventV1Transaction:
		var inflow types.Currency
		for _, se := range data.Transaction.SiacoinOutputs {
			if !relevant[se.Address] {
				continue
			}
			inflow = inflow.Add(se.Value)
		}
		return inflow
	case EventV2Transaction:
		var inflow types.Currency
		for _, se := range data.SiacoinOutputs {
			if !relevant[se.Address] {
				continue
			}
			inflow = inflow.Add(se.Value)
		}
		return inflow
	default:
		panic(fmt.Errorf("unknown event type: %T", e.Data))
	}
}

// SiafundOutflow calculates the sum of Siafunds that were spent by relevant
// addresses
func (e *Event) SiafundOutflow() uint64 {
	relevant := make(map[types.Address]bool)
	for _, addr := range e.Relevant {
		relevant[addr] = true
	}

	switch data := e.Data.(type) {
	case EventPayout, EventV1ContractResolution, EventV2ContractResolution:
		// payout events cannot have outflows
		return 0
	case EventV1Transaction:
		var inflow uint64
		for _, se := range data.SpentSiafundElements {
			if !relevant[se.SiafundOutput.Address] {
				continue
			}
			inflow += se.SiafundOutput.Value
		}
		return inflow
	case EventV2Transaction:
		var inflow uint64
		for _, se := range data.SiafundInputs {
			if !relevant[se.Parent.SiafundOutput.Address] {
				continue
			}
			inflow += se.Parent.SiafundOutput.Value
		}
		return inflow
	default:
		panic(fmt.Errorf("unknown event type: %T", e.Data))
	}
}

// SiafundInflow calculates the sum of Siafunds that were received by relevant
// addresses
func (e *Event) SiafundInflow() uint64 {
	relevant := make(map[types.Address]bool)
	for _, addr := range e.Relevant {
		relevant[addr] = true
	}

	switch data := e.Data.(type) {
	case EventPayout, EventV1ContractResolution, EventV2ContractResolution:
		// payout events cannot have siafund inflows
		return 0
	case EventV1Transaction:
		var outflow uint64
		for _, se := range data.Transaction.SiafundOutputs {
			if !relevant[se.Address] {
				continue
			}
			outflow += se.Value
		}
		return outflow
	case EventV2Transaction:
		var outflow uint64
		for _, se := range data.SiafundOutputs {
			if !relevant[se.Address] {
				continue
			}
			outflow += se.Value
		}
		return outflow
	default:
		panic(fmt.Errorf("unknown event type: %T", e.Data))
	}
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (e *Event) UnmarshalJSON(b []byte) error {
	var je struct {
		ID             types.Hash256    `json:"id"`
		Index          types.ChainIndex `json:"index"`
		Timestamp      time.Time        `json:"timestamp"`
		MaturityHeight uint64           `json:"maturityHeight"`
		Type           string           `json:"type"`
		Data           json.RawMessage  `json:"data"`
		Relevant       []types.Address  `json:"relevant,omitempty"`
	}
	if err := json.Unmarshal(b, &je); err != nil {
		return err
	}

	e.ID = je.ID
	e.Index = je.Index
	e.Timestamp = je.Timestamp
	e.MaturityHeight = je.MaturityHeight
	e.Type = je.Type
	e.Relevant = je.Relevant

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

// EncodeTo implements types.EncoderTo
func (ep EventPayout) EncodeTo(e *types.Encoder) {
	ep.SiacoinElement.EncodeTo(e)
}

// DecodeFrom implements types.DecoderFrom
func (ep *EventPayout) DecodeFrom(d *types.Decoder) {
	ep.SiacoinElement.DecodeFrom(d)
}

// EncodeTo implements types.EncoderTo
func (et EventV1Transaction) EncodeTo(e *types.Encoder) {
	et.Transaction.EncodeTo(e)
	types.EncodeSlice(e, et.SpentSiacoinElements)
	types.EncodeSlice(e, et.SpentSiafundElements)
}

// DecodeFrom implements types.DecoderFrom
func (et *EventV1Transaction) DecodeFrom(d *types.Decoder) {
	et.Transaction.DecodeFrom(d)
	types.DecodeSlice(d, &et.SpentSiacoinElements)
	types.DecodeSlice(d, &et.SpentSiafundElements)
}

// EncodeTo implements types.EncoderTo
func (et EventV2Transaction) EncodeTo(e *types.Encoder) {
	types.V2Transaction(et).EncodeTo(e)
}

// DecodeFrom implements types.DecoderFrom
func (et *EventV2Transaction) DecodeFrom(d *types.Decoder) {
	(*types.V2Transaction)(et).DecodeFrom(d)
}

// EncodeTo implements types.EncoderTo
func (er EventV1ContractResolution) EncodeTo(e *types.Encoder) {
	er.Parent.EncodeTo(e)
	er.SiacoinElement.EncodeTo(e)
	e.WriteBool(er.Missed)
}

// DecodeFrom implements types.DecoderFrom
func (er *EventV1ContractResolution) DecodeFrom(d *types.Decoder) {
	er.Parent.DecodeFrom(d)
	er.SiacoinElement.DecodeFrom(d)
	er.Missed = d.ReadBool()
}

// EncodeTo implements types.EncoderTo
func (er EventV2ContractResolution) EncodeTo(e *types.Encoder) {
	er.Resolution.EncodeTo(e)
	er.SiacoinElement.EncodeTo(e)
	e.WriteBool(er.Missed)
}

// DecodeFrom implements types.DecoderFrom
func (er *EventV2ContractResolution) DecodeFrom(d *types.Decoder) {
	er.Resolution.DecodeFrom(d)
	er.SiacoinElement.DecodeFrom(d)
	er.Missed = d.ReadBool()
}

// EncodeTo implements types.EncoderTo
func (ev *Event) EncodeTo(e *types.Encoder) {
	ev.ID.EncodeTo(e)
	ev.Index.EncodeTo(e)
	e.WriteUint64(ev.Confirmations)
	e.WriteString(ev.Type)
	switch data := ev.Data.(type) {
	case EventPayout:
		data.EncodeTo(e)
	case EventV1Transaction:
		data.EncodeTo(e)
	case EventV1ContractResolution:
		data.EncodeTo(e)
	case EventV2ContractResolution:
		data.EncodeTo(e)
	case EventV2Transaction:
		types.V2Transaction(data).EncodeTo(e)
	default:
		panic("unknown event type") // should never happen
	}
	e.WriteUint64(ev.MaturityHeight)
	e.WriteTime(ev.Timestamp)
	types.EncodeSlice(e, ev.Relevant)
}

// DecodeFrom implements types.DecoderFrom
func (ev *Event) DecodeFrom(d *types.Decoder) {
	ev.ID.DecodeFrom(d)
	ev.Index.DecodeFrom(d)
	ev.Confirmations = d.ReadUint64()
	ev.Type = d.ReadString()
	switch ev.Type {
	case EventTypeMinerPayout, EventTypeFoundationSubsidy, EventTypeSiafundClaim:
		var data EventPayout
		data.DecodeFrom(d)
		ev.Data = data
	case EventTypeV1Transaction:
		var data EventV1Transaction
		data.DecodeFrom(d)
		ev.Data = data
	case EventTypeV1ContractResolution:
		var data EventV1ContractResolution
		data.DecodeFrom(d)
		ev.Data = data
	case EventTypeV2ContractResolution:
		var data EventV2ContractResolution
		data.DecodeFrom(d)
		ev.Data = data
	case EventTypeV2Transaction:
		var data types.V2Transaction
		data.DecodeFrom(d)
		ev.Data = EventV2Transaction(data)
	default:
		d.SetErr(fmt.Errorf("unknown event type: %q", ev.Type))
		return
	}
	ev.MaturityHeight = d.ReadUint64()
	ev.Timestamp = d.ReadTime()
	types.DecodeSlice(d, &ev.Relevant)
}
