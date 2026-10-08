//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd

package journal

import (
	"fmt"
	"os"
)

func lockJournalFile(*os.File) error {
	return fmt.Errorf("directory-relative journal locking is unsupported on this platform")
}
