package journal

import (
	"os"
)

const (
	// FileMode default file mode
	FileMode os.FileMode = 0600
	// DirMode default directory mode
	DirMode = os.FileMode(0700) | os.ModeDir

	// BufSize default buf file size
	BufSize = 1024 * 1024 * 4 // 4 MB
)
