package wallet

import (
	"encoding/json"
	"math"
	"reflect"
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
		Timestamp: time.Unix(int64(frand.Intn(math.MaxInt32)), 0),
	}

	buf, err := json.Marshal(we)
	if err != nil {
		t.Fatal(err)
	}

	var we2 Event
	if err = json.Unmarshal(buf, &we2); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(we, we2) {
		t.Fatal("round-trip failed")
	}
}
