package journal

import (
	"os"
)

const (
	// FileMode requests private creation; the process umask can further restrict it.
	FileMode os.FileMode = 0600
	// DirMode requests private directory creation without changing the umask.
	DirMode = os.FileMode(0700) | os.ModeDir

	// BufSize default buf file size
	BufSize = 1024 * 1024 * 4 // 4 MB
)
