package journal

import (
	"bufio"
	"encoding/binary"
	"errors"
)

// bufferedACKMaximum consumes only complete delta words ALREADY in r. The
// caller has established the nonnegative absolute base with the ordinary
// readID path. It performs no I/O, retains no borrowed slice, and stops exactly
// after an invalid word, like readID. A partial trailing word and pending
// Reader error remain for the ordinary io.ReadFull path to handle.
func bufferedACKMaximum(r *bufio.Reader, base int64) (int64, error) {
	n := r.Buffered() &^ 7
	if n == 0 {
		return 0, nil
	}
	words, err := r.Peek(n) // n <= Buffered: cannot fill or consume a pending error
	if err != nil {
		return 0, err
	}
	var maximum int64
	for offset := 0; offset < n; offset += 8 {
		id := int64(binary.BigEndian.Uint64(words[offset:])) + base
		if id < 0 {
			// Match the original cursor on failure; do not consume valid words
			// after the bad delta. Discard is bounded by the existing buffer.
			if _, err := r.Discard(offset + 8); err != nil {
				return 0, err
			}
			return 0, errors.New("acknowledgement ID underflow or overflow")
		}
		if id > maximum {
			maximum = id
		}
	}
	_, err = r.Discard(n)
	return maximum, err
}
