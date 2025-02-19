package wallet

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestEventsJSONRoundTrip(t *testing.T) {
	we := Event{
		ID:            frand.Entropy256(),
		Type:          EventTypeV1Transaction,
		Data:          EventV1Transaction{},
		Confirmations: frand.Uint64n(math.MaxUint64),
		Index: types.ChainIndex{
			ID:     frand.Entropy256(),
			Height: frand.Uint64n(math.MaxUint64),
		},
		Relevant: []types.Address{
			frand.Entropy256(),
			frand.Entropy256(),
			frand.Entropy256(),
		},
		MaturityHeight: frand.Uint64n(math.MaxUint64),
		Timestamp:      time.Unix(int64(frand.Intn(math.MaxInt32)), 0),
	}

	event1JSON, err := json.Marshal(we)
	if err != nil {
		t.Fatal(err)
	}

	var we2 Event
	if err = json.Unmarshal(event1JSON, &we2); err != nil {
		t.Fatal(err)
	}

	event2JSON, err := json.Marshal(we2)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(event1JSON, event2JSON) {
		t.Log(string(event1JSON))
		t.Log(string(event2JSON))
		t.Fatal("round-trip failed")
	}
}
