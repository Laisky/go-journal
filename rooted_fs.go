package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// journalFS keeps the public path-based helpers compatible while Journal uses
// a retained directory capability for every operation, including recovery and
// cleanup. A Root.Name is a diagnostic label, never an unrooted I/O capability.
type journalFS interface {
	Open(string) (*os.File, error)
	OpenFile(string, int, os.FileMode) (*os.File, error)
	ReadDir(string) ([]os.DirEntry, error)
	Stat(string) (os.FileInfo, error)
	Remove(string) error
	Link(string, string) error
}

type pathFS struct{}

func (pathFS) Open(name string) (*os.File, error) { return os.Open(name) }
func (pathFS) OpenFile(name string, flags int, mode os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flags, mode)
}
func (pathFS) ReadDir(name string) ([]os.DirEntry, error) { return os.ReadDir(name) }
func (pathFS) Stat(name string) (os.FileInfo, error)      { return os.Stat(name) }
func (pathFS) Remove(name string) error                   { return os.Remove(name) }
func (pathFS) Link(old, next string) error                { return os.Link(old, next) }

// filesystem supplies the historical path semantics only for standalone
// helpers. Journal always installs a non-nil rootedFS before taking its lock.
func filesystem(optional ...journalFS) journalFS {
	if len(optional) != 0 && optional[0] != nil {
		return optional[0]
	}
	return pathFS{}
}

type rootedFS struct {
	root      *os.Root
	directory *rootedDirectory
}

// A fixed directory descriptor, derived from the retained Root. The lock pins
// its lifetime across each kernel open; Close cannot recycle the descriptor.
type rootedDirectory struct {
	mu     sync.RWMutex
	file   *os.File
	fd     uintptr
	closed bool
}

func (d *rootedDirectory) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return os.ErrClosed
	}
	d.closed = true
	return d.file.Close()
}

func newRootedFS(root *os.Root) (rootedFS, *rootedDirectory) {
	directory := retainedOpenDirectory(root)
	return rootedFS{root: root, directory: directory}, directory
}

// All journal files live directly in this directory. Do not turn an arbitrary
// absolute/parent pathname into a basename: that could alias another identity.
func (f rootedFS) relative(name string) (string, error) {
	rel, err := filepath.Rel(filepath.Clean(f.root.Name()), filepath.Clean(name))
	if err != nil || rel == ".." || (!filepath.IsLocal(rel)) || (rel != "." && filepath.Base(rel) != rel) {
		return "", fmt.Errorf("journal path is not a direct child of its owned directory")
	}
	return rel, nil
}
func (f rootedFS) Open(name string) (*os.File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}
func (f rootedFS) OpenFile(name string, flags int, mode os.FileMode) (*os.File, error) {
	rel, err := f.relative(name)
	if err != nil {
		return nil, err
	}
	return openJournalRootFile(f.root, f.directory, name, rel, flags, mode)
}
func (f rootedFS) Stat(name string) (os.FileInfo, error) {
	rel, err := f.relative(name)
	if err != nil {
		return nil, err
	}
	return f.root.Stat(rel)
}
func (f rootedFS) ReadDir(name string) ([]os.DirEntry, error) {
	rel, err := f.relative(name)
	if err != nil {
		return nil, err
	}
	// Root eagerly confines DirEntry.Info as well as the directory open.
	// os.NewFile lacks that internal Root metadata policy.
	file, err := f.root.Open(rel)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	entries, err := file.ReadDir(-1)
	// Match os.ReadDir's sorted snapshot contract (also on partial errors).
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, err
}
func (f rootedFS) Remove(name string) error {
	rel, err := f.relative(name)
	if err != nil {
		return err
	}
	if rel == "." {
		return fmt.Errorf("cannot remove the owned journal directory")
	}
	return f.root.Remove(rel)
}
func (f rootedFS) Link(old, next string) error {
	a, err := f.relative(old)
	if err != nil {
		return err
	}
	b, err := f.relative(next)
	if err != nil {
		return err
	}
	return f.root.Link(a, b)
}
