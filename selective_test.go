package journal

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tinylib/msgp/msgp"
)

func selectiveWire(t testing.TB, id int64, payload map[string]interface{}, reverse bool) []byte {
	t.Helper()
	d := Data{ID: id, Data: payload}
	if !reverse {
		b, e := d.MarshalMsg(nil)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	var b bytes.Buffer
	w := msgp.NewWriter(&b)
	if e := w.WriteMapHeader(2); e != nil {
		t.Fatal(e)
	}
	if e := w.WriteString("ID"); e != nil {
		t.Fatal(e)
	}
	if e := w.WriteInt64(id); e != nil {
		t.Fatal(e)
	}
	if e := w.WriteString("Data"); e != nil {
		t.Fatal(e)
	}
	if e := w.WriteIntf(payload); e != nil {
		t.Fatal(e)
	}
	if e := w.Flush(); e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}

func TestSelectiveRecognizesOnlyValidatedEnvelopes(t *testing.T) {
	payload := map[string]interface{}{"text": strings.Repeat("世界 ", 4096), "n": int64(math.MinInt64), "u": uint64(math.MaxUint64), "bytes": []byte{0, 255}, "list": []interface{}{nil, true, false, float32(1.5), float64(2.5), map[string]interface{}{"nested": "value"}}}
	for _, reverse := range []bool{false, true} {
		b := selectiveWire(t, 7, payload, reverse)
		id, size, ok := inspectReplayRecord(append(append([]byte(nil), b...), 0xc1))
		if !ok || id != 7 || size != len(b) {
			t.Fatalf("complete record not recognized: %d %d %v", id, size, ok)
		}
		for n := 0; n < len(b); n++ {
			if _, _, ok := inspectReplayRecord(b[:n]); ok {
				t.Fatalf("accepted incomplete prefix %d/%d", n, len(b))
			}
		}
	}
	// Full DecodeMsg, not the fast path, remains responsible for unusual legacy
	// envelopes and extension-specific validators, including invalid extensions.
	plain := selectiveWire(t, 9, map[string]interface{}{"x": "value"}, false)
	unknown := append([]byte(nil), plain...)
	unknown[0] = 0x83
	unknown = msgp.AppendString(unknown, "future")
	unknown = msgp.AppendInt64(unknown, 1)
	duplicate := append([]byte(nil), plain...)
	duplicate[0] = 0x83
	duplicate = msgp.AppendString(duplicate, "ID")
	duplicate = msgp.AppendInt64(duplicate, 10)
	missing := msgp.AppendString([]byte{0x81}, "ID")
	missing = msgp.AppendInt64(missing, 8)
	extension := selectiveWire(t, 9, map[string]interface{}{"time": time.Unix(123, 456)}, false)
	badMap := []byte{0x82, 0xa4, 'D', 'a', 't', 'a', 0x81, 1, 0xa1, 'x', 0xa2, 'I', 'D', 9}
	badNested := []byte{0x82, 0xa4, 'D', 'a', 't', 'a', 0x81, 0xa1, 'x', 0x81, 1, 0xc0, 0xa2, 'I', 'D', 9}
	badID := msgp.AppendString([]byte{0x82}, "Data")
	badID = append(badID, 0x80)
	badID = msgp.AppendString(badID, "ID")
	badID = msgp.AppendUint64(badID, math.MaxUint64)
	for name, b := range map[string][]byte{"unknown": unknown, "duplicate": duplicate, "missing": missing, "extension": extension, "bad-map": badMap, "bad-nested": badNested, "bad-id": badID} {
		t.Run(name, func(t *testing.T) {
			if _, _, ok := inspectReplayRecord(b); ok {
				t.Fatal("unsupported shape used selective path")
			}
			old := Data{}
			oldErr := old.DecodeMsg(msgp.NewReader(bytes.NewReader(b)))
			dec := &DataDecoder{reader: msgp.NewReader(bytes.NewReader(b))}
			out := Data{}
			calls := 0
			skipped, err := dec.readWithAcknowledgement(&out, func(id int64) bool { calls++; return true })
			if (oldErr == nil) != (err == nil) {
				t.Fatalf("acceptance changed: old %v new %v", oldErr, err)
			}
			if err != nil && calls != 0 {
				t.Fatal("consumed ACK before malformed record was rejected")
			}
			if err == nil && (!skipped || calls != 1 || !reflect.DeepEqual(old, out)) {
				t.Fatalf("fallback changed: %#v %#v calls=%d", old, out, calls)
			}
		})
	}
}

type selectiveChunks struct {
	b            []byte
	chunk, calls int
	final        error
}

func (r *selectiveChunks) Read(p []byte) (int, error) {
	r.calls++
	if len(r.b) == 0 {
		if r.final != nil {
			return 0, r.final
		}
		return 0, io.EOF
	}
	n := len(p)
	if n > r.chunk {
		n = r.chunk
	}
	if n > len(r.b) {
		n = len(r.b)
	}
	copy(p, r.b[:n])
	r.b = r.b[n:]
	return n, nil
}

func TestSelectiveSparseACKsAndOwnedPendingValues(t *testing.T) {
	for _, chunk := range []int{1, 17, 65536, 4 << 20} {
		for _, reversed := range []bool{false, true} {
			t.Run(fmt.Sprintf("chunk%d/reverse%v", chunk, reversed), func(t *testing.T) {
				var stream []byte
				originals := map[int64]map[string]interface{}{}
				// ACKs are deliberately sparse and include a duplicate record. An ACK is
				// consumed once, as before; it is not a permanent maximum-ID watermark.
				for _, id := range []int64{9, 1, 7, 9, 4, 3} {
					p := map[string]interface{}{"text": strings.Repeat(fmt.Sprint(id)+"世界", 8192), "array": []interface{}{int64(id), "held"}}
					originals[id] = p
					stream = append(stream, selectiveWire(t, id, p, reversed)...)
				}
				dec := &DataDecoder{reader: msgp.NewReaderSize(&selectiveChunks{b: stream, chunk: chunk}, 65536)}
				acks := map[int64]bool{9: true, 7: true, 4: true}
				check := func(id int64) bool { ok := acks[id]; delete(acks, id); return ok }
				var held []*Data
				var got []int64
				for {
					d := new(Data)
					skip, err := dec.readWithAcknowledgement(d, check)
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if skip {
						continue
					}
					got = append(got, d.ID)
					held = append(held, d)
				}
				if !reflect.DeepEqual(got, []int64{1, 9, 3}) {
					t.Fatalf("sparse/duplicate ACK semantics changed: %v", got)
				}
				for _, d := range held {
					if !reflect.DeepEqual(d.Data, originals[d.ID]) {
						t.Fatalf("pending payload aliased a later read: %d", d.ID)
					}
				}
			})
		}
	}
}

func TestSelectiveDoesNotReadPastBufferedRecordOrHideReadError(t *testing.T) {
	sentinel := errors.New("late storage failure")
	wire := selectiveWire(t, 17, map[string]interface{}{"body": "complete"}, false)
	for _, ack := range []bool{false, true} {
		r := &selectiveChunks{b: wire, chunk: len(wire), final: sentinel}
		d := &DataDecoder{reader: msgp.NewReaderSize(r, len(wire)+1024)}
		skip, err := d.readWithAcknowledgement(new(Data), func(int64) bool { return ack })
		if err != nil || skip != ack || r.calls != 1 {
			t.Fatalf("extra lookahead or changed outcome: calls%d skip%v err%v", r.calls, skip, err)
		}
		calls := 0
		_, err = d.readWithAcknowledgement(new(Data), func(int64) bool { calls++; return true })
		if !errors.Is(err, sentinel) || calls != 0 {
			t.Fatalf("lost storage failure: err=%v calls=%d", err, calls)
		}
	}
}

func TestSelectiveCompressedAndOversizedRecords(t *testing.T) {
	for _, gz := range []bool{false, true} {
		for _, size := range []int{0, 16384, 131071, 262144} {
			t.Run(fmt.Sprintf("gzip%v/size%d", gz, size), func(t *testing.T) {
				var stream []byte
				for i := int64(1); i <= 4; i++ {
					stream = append(stream, selectiveWire(t, i, map[string]interface{}{"text": strings.Repeat("x", size)}, i%2 == 0)...)
				}
				p := filepath.Join(t.TempDir(), "segment")
				if gz {
					var out bytes.Buffer
					z := gzip.NewWriter(&out)
					z.Write(stream)
					if err := z.Close(); err != nil {
						t.Fatal(err)
					}
					stream = out.Bytes()
				}
				if err := os.WriteFile(p, stream, 0600); err != nil {
					t.Fatal(err)
				}
				f, err := os.Open(p)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				dec, err := NewDataDecoder(f, gz)
				if err != nil {
					t.Fatal(err)
				}
				var ids []int64
				for {
					d := new(Data)
					skip, err := dec.readWithAcknowledgement(d, func(id int64) bool { return id == 2 || id == 4 })
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if skip {
						continue
					}
					ids = append(ids, d.ID)
					if d.Data["text"] != strings.Repeat("x", size) {
						t.Fatal("payload changed")
					}
				}
				if !reflect.DeepEqual(ids, []int64{1, 3}) {
					t.Fatalf("missing pending: %v", ids)
				}
			})
		}
	}
}

func TestSelectiveJournalRestartSparseACKs(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			open := func() *Journal {
				j, err := NewJournal(WithBufDirPath(dir), WithIsCompress(gz), WithIsAggresiveGC(false), WithRotateDuration(time.Hour), WithRotateCheckInterval(time.Hour), WithFlushInterval(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				if err = j.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				return j
			}
			j := open()
			for i := int64(1); i <= 100; i++ {
				if err := j.WriteData(&Data{ID: i, Data: map[string]interface{}{"body": strings.Repeat(fmt.Sprint(i)+"世界", 1024)}}); err != nil {
					t.Fatal(err)
				}
				if i%3 != 0 {
					if err := j.WriteId(i); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := j.Sync(); err != nil {
				t.Fatal(err)
			}
			j.Close()
			for restart := 0; restart < 2; restart++ {
				j = open()
				if !j.LockLegacy() {
					t.Fatal("replay lease refused")
				}
				count := 0
				for {
					d := new(Data)
					err := j.LoadLegacyBuf(d)
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if d.ID%3 != 0 || d.Data["body"] != strings.Repeat(fmt.Sprint(d.ID)+"世界", 1024) {
						t.Fatalf("replay corrupted: %d", d.ID)
					}
					count++
					if err = j.WriteData(d); err != nil {
						t.Fatal(err)
					}
					if err = j.Sync(); err != nil {
						t.Fatal(err)
					}
				}
				j.UnLockLegacy()
				if count != 33 {
					t.Fatalf("lost sparse pending at restart%d: %d", restart, count)
				}
				j.Close()
			}
		})
	}
}

func FuzzSelectiveAcceptedFrames(f *testing.F) {
	for _, reverse := range []bool{false, true} {
		f.Add(selectiveWire(f, 9, map[string]interface{}{"x": "hello 世界", "nested": []interface{}{int64(3), true, nil}}, reverse))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > maxSelectiveRecordBytes {
			return
		}
		id, n, ok := inspectReplayRecord(b)
		if !ok {
			return
		}
		reader := msgp.NewReader(bytes.NewReader(b))
		var d Data
		if err := d.DecodeMsg(reader); err != nil {
			t.Fatalf("fast path accepted invalid frame: %v", err)
		}
		if d.ID != id {
			t.Fatalf("ID differs: %d vs%d", d.ID, id)
		}
		// DecodeMsg consumes exactly the inspected first record, not a later frame.
		rest, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(rest, b[n:]) {
			t.Fatal("fast path boundary differs from generated decoder")
		}
	})
}

func BenchmarkSelectiveAcknowledgedReplay(b *testing.B) {
	for _, fraction := range []int{0, 50, 100} {
		for _, fast := range []bool{false, true} {
			b.Run(fmt.Sprintf("ack%d/selective%v", fraction, fast), func(b *testing.B) {
				var raw []byte
				for i := int64(0); i < 64; i++ {
					raw = append(raw, selectiveWire(b, i, map[string]interface{}{"text": strings.Repeat("x", 16384)}, false)...)
				}
				r := bytes.NewReader(raw)
				d := &DataDecoder{reader: msgp.NewReaderSize(r, 4<<20)}
				check := func(id int64) bool { return fraction == 100 || (fraction == 50 && id%2 == 0) }
				var out Data
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r.Reset(raw)
					d.reader.Reset(r)
					for n := 0; n < 64; n++ {
						if fast {
							if _, err := d.readWithAcknowledgement(&out, check); err != nil {
								b.Fatal(err)
							}
						} else {
							if err := d.Read(&out); err != nil {
								b.Fatal(err)
							}
							check(out.ID)
						}
					}
				}
			})
		}
	}
}
