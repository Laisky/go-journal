package journal_test

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	journal "github.com/Laisky/go-journal"
)

func TestBehaviorFractionalTTLDoesNotExpireEarly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := journal.NewInt64SetWithTTL(context.Background(), 500*time.Millisecond)
		defer s.Close()
		synctest.Wait()
		time.Sleep(400 * time.Millisecond)
		s.AddInt64(73)
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		// 200 ms old, with a caller-requested lifetime of 500 ms. A rotation is not
		// permission to round its expiry down to an earlier whole second.
		if !s.CheckAndRemove(73) {
			t.Fatal("confirmation expired before its requested TTL")
		}
		s.AddInt64(73)
		time.Sleep(450 * time.Millisecond)
		synctest.Wait()
		if !s.CheckAndRemove(73) {
			t.Fatal("refreshed confirmation expired early")
		}
		if !s.CheckAndRemove(73) {
			t.Fatal("lookup consumed confirmation")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if s.CheckAndRemove(73) {
			t.Fatal("unrefreshed confirmation never expired")
		}
	})
}
func TestBehaviorBitmapPreservesUint32Boundary(t *testing.T) {
	s := journal.NewUint32Set()
	s.AddInt64(0)
	s.AddInt64(math.MaxUint32)
	if s.GetLen() != 2 {
		t.Fatal("largest uint32 aliased zero")
	}
	if !s.CheckAndRemoveUint32(math.MaxUint32) || !s.CheckAndRemoveInt64(0) {
		t.Fatal("boundary identity lost")
	}
}
func TestBehaviorBitmapDecodeRejectsUnrepresentableIDs(t *testing.T) {
	fp, err := os.Create(filepath.Join(t.TempDir(), "acks"))
	behaviorCheck(t, err)
	defer fp.Close()
	enc, err := journal.NewIdsEncoder(fp, false)
	behaviorCheck(t, err)
	behaviorCheck(t, enc.Write(int64(math.MaxUint32)+1))
	behaviorCheck(t, enc.Close())
	_, err = fp.Seek(0, 0)
	behaviorCheck(t, err)
	dec, err := journal.NewIdsDecoder(fp, false)
	behaviorCheck(t, err)
	if _, err := dec.ReadAllToBmap(); err == nil {
		t.Fatal("64-bit ACK silently narrowed into a 32-bit false identity")
	}
}
func TestBehaviorMalformedIDStreamIsRejected(t *testing.T) {
	for name, words := range map[string][]uint64{"negativeBase": {math.MaxUint64}, "underflow": {0, math.MaxUint64}, "overflow": {math.MaxInt64, 1}} {
		t.Run(name, func(t *testing.T) {
			fp, err := os.Create(filepath.Join(t.TempDir(), "ids"))
			behaviorCheck(t, err)
			defer fp.Close()
			for _, word := range words {
				behaviorCheck(t, binary.Write(fp, binary.BigEndian, word))
			}
			_, err = fp.Seek(0, 0)
			behaviorCheck(t, err)
			dec, err := journal.NewIdsDecoder(fp, false)
			behaviorCheck(t, err)
			if _, err = dec.LoadMaxId(); err == nil {
				t.Fatal("invalid decoded acknowledgement identity accepted")
			}
		})
	}
}
func TestBehaviorReplayAfterCloseReturnsError(t *testing.T) {
	j := behaviorStart(t, t.TempDir(), false)
	behaviorCheck(t, j.WriteData(behaviorData(1)))
	behaviorCheck(t, j.Rotate(context.Background()))
	if !j.LockLegacy() {
		t.Fatal("lease")
	}
	j.Close()
	if behaviorNoPanic(t, func() error { return j.LoadLegacyBuf(new(journal.Data)) }) == nil {
		t.Fatal("closed journal accepted replay")
	}
}
