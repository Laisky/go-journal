//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package journal

import (
	"os"
	"syscall"

	"github.com/coreos/etcd/pkg/fileutil"
)

func lockJournalFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == syscall.EWOULDBLOCK {
		return fileutil.ErrLocked
	}
	return err
}
