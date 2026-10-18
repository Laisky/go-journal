package journal

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/ugorji/go/codec"
	"testing"
)

// The former TestCodec could call EOF success on missing records. These
// deterministic controls distinguish buffering/fixture errors from codec bugs.
func TestCodecBufferedCompletenessControls(t *testing.T) {
	var backing bytes.Buffer
	buffered := bufio.NewWriterSize(&backing, 1<<20)
	encoder := codec.NewEncoder(buffered, NewCodec())
	for i := 0; i < 100; i++ {
		if e := encoder.Encode(map[string]interface{}{"id": i, "message": fmt.Sprintf("synthetic-%03d", i)}); e != nil {
			t.Fatal(e)
		}
	}
	count := func(raw []byte) (int, error) {
		d := codec.NewDecoder(bytes.NewReader(raw), NewCodec())
		n := 0
		for d.NumBytesRead() < len(raw) {
			var record map[string]interface{}
			e := d.Decode(&record)
			if e != nil {
				return n, e
			}
			var message string
			switch v := record["message"].(type) {
			case string:
				message = v
			case []byte:
				message = string(v)
			}
			if fmt.Sprint(record["id"]) != fmt.Sprint(n) || message != fmt.Sprintf("synthetic-%03d", n) {
				return n, fmt.Errorf("decoded record incomplete")
			}
			n++
		}
		return n, nil
	}
	n, e := count(backing.Bytes())
	if e != nil || n != 0 {
		t.Fatal("unflushed control did not expose missing bytes", n, e)
	}
	if e = buffered.Flush(); e != nil {
		t.Fatal(e)
	}
	raw := append([]byte(nil), backing.Bytes()...)
	if n, e = count(raw); e != nil || n != 100 {
		t.Fatal("flushed codec roundtrip", n, e)
	}
	if n, e = count(raw[:len(raw)-1]); e == nil || n >= 100 {
		t.Fatal("truncated stream falsely complete", n, e)
	}
}
