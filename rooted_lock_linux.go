//go:build linux

package journal

import (
	"os"
	"syscall"

	"github.com/coreos/etcd/pkg/fileutil"
)

// Historical etcd versions can select either flock or Linux OFD locks. These
// lock namespaces are independent on local filesystems. Hold both so an old
// and new process (or two opens in one process) cannot own the same WAL.
func lockJournalFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if err == syscall.EWOULDBLOCK {
			return fileutil.ErrLocked
		}
		return err
	}
	lock := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: 0, Len: 0}
	const ofdSetLK = 37
	err := syscall.FcntlFlock(f.Fd(), ofdSetLK, &lock)
	if err == syscall.EINVAL || err == syscall.ENOSYS || err == syscall.EOPNOTSUPP {
		// Older kernels/filesystems without OFD locks use flock in etcd too.
		return nil
	}
	if err == syscall.EWOULDBLOCK {
		return fileutil.ErrLocked
	}
	return err // Caller closes f on any failure, releasing the first lock too.
}
