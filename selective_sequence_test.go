package journal

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/tinylib/msgp/msgp"
)

func sequenceReplay(stream []byte, optimized, consume bool, chunk int) ([]Data, string) {
	r := &selectiveChunks{b: stream, chunk: chunk}
	dec := &DataDecoder{reader: msgp.NewReaderSize(r, 65536)}
	acks := map[int64]bool{5: true, 7: true, 9: true}
	check := func(id int64) bool {
		ok := acks[id]
		if consume {
			delete(acks, id)
		}
		return ok
	}
	out := []Data{}
	for {
		d := Data{ID: 777, Data: map[string]interface{}{"caller": "sentinel"}}
		for {
			var skip bool
			var err error
			if optimized {
				skip, err = dec.readWithAcknowledgement(&d, check)
			} else {
				err = dec.Read(&d)
				if err == nil {
					skip = check(d.ID)
				}
			}
			if err != nil {
				if err == io.EOF {
					return out, "EOF"
				}
				if incompleteRecord(err) {
					return out, "incomplete"
				}
				return out, "invalid"
			}
			if !skip {
				out = append(out, d)
				break
			}
		}
	}
}

func sequenceFixture(t testing.TB, text string, mask uint8) []byte {
	t.Helper()
	var stream []byte
	for _, id := range []int64{5, 7, 9} {
		stream = append(stream, selectiveWire(t, id, map[string]interface{}{"origin": text, "id": id}, mask&1 != 0)...)
	}
	var tail bytes.Buffer
	w := msgp.NewWriter(&tail)
	count := uint32(0)
	if mask&2 != 0 {
		count++
	}
	if mask&4 != 0 {
		count++
	}
	if mask&8 != 0 {
		count++
	}
	if err := w.WriteMapHeader(count); err != nil {
		t.Fatal(err)
	}
	if mask&2 != 0 {
		if err := w.WriteString("Data"); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteIntf(map[string]interface{}{"tail": text}); err != nil {
			t.Fatal(err)
		}
	}
	if mask&4 != 0 {
		if err := w.WriteString("ID"); err != nil {
			t.Fatal(err)
		}
		id := int64(10)
		if mask&16 != 0 {
			id = 9
		}
		if err := w.WriteInt64(id); err != nil {
			t.Fatal(err)
		}
	}
	if mask&8 != 0 {
		if err := w.WriteString("unknown"); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteIntf([]interface{}{true, text}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	b := tail.Bytes()
	if mask&32 != 0 && len(b) > 0 {
		b = b[:len(b)-1]
	}
	stream = append(stream, b...)
	return stream
}

func TestSelectiveSequencesPreserveFallbackState(t *testing.T) {
	for _, consume := range []bool{false, true} {
		for _, chunk := range []int{1, 17, 65536} {
			t.Run(fmt.Sprintf("consume=%t/chunk=%d", consume, chunk), func(t *testing.T) {
				for m := 0; m < 64; m++ {
					stream := sequenceFixture(t, "owned 世界", uint8(m))
					want, we := sequenceReplay(stream, false, consume, chunk)
					got, ge := sequenceReplay(stream, true, consume, chunk)
					if we != ge || !reflect.DeepEqual(want, got) {
						t.Fatalf("sequence %d changed results: old=%#v/%s new=%#v/%s", m, want, we, got, ge)
					}
				}
			})
		}
	}
}

func FuzzSelectiveSequenceState(f *testing.F) {
	for m := 0; m < 64; m++ {
		f.Add("owned 世界", uint8(m))
	}
	f.Fuzz(func(t *testing.T, text string, mask uint8) {
		if len(text) > 2048 {
			return
		}
		stream := sequenceFixture(t, text, mask)
		chunk := 65536
		if mask&64 != 0 {
			chunk = 17
		}
		consume := mask&128 != 0
		want, we := sequenceReplay(stream, false, consume, chunk)
		got, ge := sequenceReplay(stream, true, consume, chunk)
		if we != ge || !reflect.DeepEqual(want, got) {
			t.Fatalf("stateful sequence changed: old=%#v/%s new=%#v/%s", want, we, got, ge)
		}
	})
}
