package journal

import (
	"github.com/tinylib/msgp/msgp"
)

// Inspect only bytes already buffered by the ordinary reader. This is an
// optimization bound, not a record-size limit. Larger/cross-buffer records and
// less common encodings retain the original DecodeMsg path.
const maxSelectiveRecordBytes = 128 << 10

// readWithAcknowledgement avoids constructing the payload of an acknowledged
// record only after validating a complete, supported envelope. The callback has
// the same exact-ID, consume-once semantics as LegacyLoader's previous check;
// it is never a high-water mark. No byte slice escapes the reader.
func (dec *DataDecoder) readWithAcknowledgement(data *Data, acknowledged func(int64) bool) (bool, error) {
	r := dec.reader.R
	if r.Buffered() == 0 {
		// Prime exactly as ReadMapHeader would. Do not discard a read error and then
		// retry: Peek consumes its stored error, including gzip checksum failures.
		if _, err := r.Peek(1); err != nil {
			return false, err
		}
	}
	n := r.Buffered()
	if n > maxSelectiveRecordBytes {
		n = maxSelectiveRecordBytes
	}
	b, err := r.Peek(n) // no further I/O: n is already buffered
	if err != nil {
		return false, err
	}
	// The generated decoder preserves fields absent from a later envelope.
	// Materialize the last record before an unfamiliar or cross-buffer record
	// (and before a segment boundary), so fallback sees exactly the state the
	// old decoder would have left. Only skip when the next complete canonical
	// envelope is already buffered and will overwrite both fields. Check this
	// BEFORE consuming an acknowledgement: consume-once callbacks cannot be
	// queried optimistically and queried again after choosing the slow path.
	if id, size, ok := inspectReplayRecord(b); ok && hasIndependentSuccessor(b[size:]) && acknowledged(id) {
		// size has been proved <= Buffered; Skip cannot perform I/O here.
		_, err = r.Skip(size)
		return true, err
	}
	if err = dec.Read(data); err != nil {
		return false, err
	}
	// Recheck after normal decoding: an ACK may have arrived during the read.
	return acknowledged(data.ID), nil
}

// No reads, discarded errors, borrowed state or retained payload are needed
// for lookahead. An unrecognized successor is merely an optimization miss.
func hasIndependentSuccessor(b []byte) bool {
	_, _, ok := inspectReplayRecord(b)
	return ok
}

// inspectReplayRecord recognizes exactly one Data map and one integer ID in
// either order. Unknown, duplicate or missing envelope fields, extensions and
// unrecognized shapes fall back rather than changing historical acceptance or
// suppressing a decoding error. The current writer's Data-before-ID layout is
// supported without changing the on-disk format.
func inspectReplayRecord(b []byte) (id int64, size int, ok bool) {
	count, rest, err := msgp.ReadMapHeaderBytes(b)
	if err != nil || count != 2 {
		return 0, 0, false
	}
	var haveData, haveID bool
	for i := 0; i < 2; i++ {
		var key []byte
		key, rest, err = msgp.ReadStringZC(rest)
		if err != nil {
			return 0, 0, false
		}
		switch string(key) {
		case "Data":
			if haveData {
				return 0, 0, false
			}
			haveData = true
			rest, ok = scanReplayMap(rest, 0)
			if !ok {
				return 0, 0, false
			}
		case "ID":
			if haveID {
				return 0, 0, false
			}
			haveID = true
			id, rest, err = msgp.ReadInt64Bytes(rest)
			if err != nil {
				return 0, 0, false
			}
		default:
			return 0, 0, false
		}
	}
	return id, len(b) - len(rest), haveData && haveID
}

// The small allocation-free validator intentionally handles a subset of
// ReadIntf. In particular, maps must have string keys (Skip alone would accept
// malformed non-string keys) and extension values retain normal decoding so
// registered extension validators are never bypassed. Deeper values also use
// normal decoding; this limit does not reject previously accepted records.
func scanReplayMap(b []byte, depth int) ([]byte, bool) {
	if depth > 64 {
		return nil, false
	}
	n, rest, err := msgp.ReadMapHeaderBytes(b)
	if err != nil || uint64(n)*2 > uint64(len(rest)) {
		return nil, false
	}
	for ; n > 0; n-- {
		_, rest, err = msgp.ReadStringZC(rest)
		if err != nil {
			return nil, false
		}
		var ok bool
		rest, ok = scanReplayValue(rest, depth+1)
		if !ok {
			return nil, false
		}
	}
	return rest, true
}

func scanReplayValue(b []byte, depth int) ([]byte, bool) {
	if len(b) == 0 || depth > 64 {
		return nil, false
	}
	c := b[0]
	switch {
	case c <= 0x7f || c >= 0xe0 || c == 0xc0 || c == 0xc2 || c == 0xc3:
		return b[1:], true
	case c >= 0xa0 && c <= 0xbf || c == 0xd9 || c == 0xda || c == 0xdb:
		_, rest, err := msgp.ReadStringZC(b)
		return rest, err == nil
	case c == 0xc4 || c == 0xc5 || c == 0xc6:
		_, rest, err := msgp.ReadBytesZC(b)
		return rest, err == nil
	case c >= 0x80 && c <= 0x8f || c == 0xde || c == 0xdf:
		return scanReplayMap(b, depth)
	case c >= 0x90 && c <= 0x9f || c == 0xdc || c == 0xdd:
		n, rest, err := msgp.ReadArrayHeaderBytes(b)
		if err != nil || uint64(n) > uint64(len(rest)) {
			return nil, false
		}
		for ; n > 0; n-- {
			var ok bool
			rest, ok = scanReplayValue(rest, depth+1)
			if !ok {
				return nil, false
			}
		}
		return rest, true
	default:
		size := 0
		switch c {
		case 0xcc, 0xd0:
			size = 2
		case 0xcd, 0xd1:
			size = 3
		case 0xca, 0xce, 0xd2:
			size = 5
		case 0xcb, 0xcf, 0xd3:
			size = 9
		}
		if size == 0 || len(b) < size {
			return nil, false
		}
		return b[size:], true
	}
}
