package journal

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"github.com/tinylib/msgp/msgp"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type recoveryPrefix struct {
	path string
	size int64
	info os.FileInfo
}

// ClosedRecoveryArchive owns an existing stopped journal's lock and retained
// directory capability. It creates, synchronizes, acknowledges and deletes
// nothing. Callers may validate then replay the same captured file prefixes.
type ClosedRecoveryArchive struct {
	mu        sync.RWMutex
	root      *os.Root
	directory *rootedDirectory
	disk      journalFS
	lock      *os.File
	files     []recoveryPrefix
	maxRecord int64
	closed    bool
}

func OpenClosedRecoveryArchive(root *os.Root, maxRecordBytes int64) (a *ClosedRecoveryArchive, err error) {
	if root == nil || maxRecordBytes < 4096 || maxRecordBytes > 64<<20 {
		return nil, errors.New("invalid closed archive reader limits")
	}
	owned, e := root.OpenRoot(".")
	if e != nil {
		return nil, e
	}
	a = &ClosedRecoveryArchive{root: owned, maxRecord: maxRecordBytes}
	a.disk, a.directory = newRootedFS(owned)
	ownedArchive := a
	defer func() {
		if err != nil {
			ownedArchive.Close()
		}
	}()
	// O_CREAT is deliberately absent: never initialize a new ownership domain.
	a.lock, err = a.disk.OpenFile(filepath.Join(owned.Name(), ".journal.lock"), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err = lockJournalFile(a.lock); err != nil {
		return nil, err
	}
	entries, e := a.disk.ReadDir(owned.Name())
	if e != nil {
		return nil, e
	}
	if len(entries) > 65536 {
		return nil, errors.New("closed archive file budget exceeded")
	}
	for _, entry := range entries {
		if !dataFileNameReg.MatchString(entry.Name()) {
			continue
		}
		path := filepath.Join(owned.Name(), entry.Name())
		st, e := a.disk.Stat(path)
		if e != nil {
			return nil, e
		}
		if !st.Mode().IsRegular() {
			return nil, errors.New("invalid closed archive segment")
		}
		if st.Size() > 0 {
			a.files = append(a.files, recoveryPrefix{path, st.Size(), st})
		}
	}
	return a, nil
}
func (a *ClosedRecoveryArchive) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	var err error
	if a.lock != nil {
		err = a.lock.Close()
	}
	if a.directory != nil {
		err = errors.Join(err, a.directory.Close())
	}
	if a.root != nil {
		err = errors.Join(err, a.root.Close())
	}
	return err
}

type boundedRecoveryRecord struct {
	buffer bytes.Buffer
	limit  int64
}

func (w *boundedRecoveryRecord) Write(p []byte) (int, error) {
	if int64(len(p)) > w.limit-int64(w.buffer.Len()) {
		return 0, errors.New("closed archive record budget exceeded")
	}
	return w.buffer.Write(p)
}

// WalkRaw returns exact MessagePack record bytes alongside the decoded identity.
// A successful callback transfers no ownership of persistent state. It must not
// reenter this reader or mutate the supplied bytes while another call uses them.
func (a *ClosedRecoveryArchive) WalkRaw(ctx context.Context, visit func(*Data, []byte) error) (int, error) {
	if ctx == nil || visit == nil {
		return 0, errors.New("closed archive callback required")
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.closed {
		return 0, os.ErrClosed
	}
	total := 0
	for _, prefix := range a.files {
		if e := ctx.Err(); e != nil {
			return total, e
		}
		fp, e := a.disk.Open(prefix.path)
		if e != nil {
			return total, e
		}
		e = func() error {
			defer fp.Close()
			st, e := fp.Stat()
			if e != nil {
				return e
			}
			if !os.SameFile(st, prefix.info) || st.Size() != prefix.size {
				return errors.New("closed archive segment changed")
			}
			var source io.Reader = io.NewSectionReader(fp, 0, prefix.size)
			if isFileGZ(prefix.path) {
				gz, e := gzip.NewReader(source)
				if e != nil {
					return e
				}
				defer gz.Close()
				source = gz
			}
			reader := msgp.NewReader(source)
			for {
				if e := ctx.Err(); e != nil {
					return e
				}
				raw := &boundedRecoveryRecord{limit: a.maxRecord}
				n, e := reader.CopyNext(raw)
				if e == io.EOF && n == 0 {
					return nil
				}
				if e != nil {
					return e
				}
				var data Data
				if e = msgp.Decode(bytes.NewReader(raw.buffer.Bytes()), &data); e != nil {
					return e
				}
				if e = visit(&data, raw.buffer.Bytes()); e != nil {
					return e
				}
				total++
			}
		}()
		if e != nil {
			return total, e
		}
	}
	return total, nil
}
