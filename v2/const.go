package journal

import (
	"os"
)

const (
	// FileMode default file mode
	FileMode os.FileMode = 0664
	// DirMode default directory mode
	DirMode = os.FileMode(0775) | os.ModeDir

	// BufSize default buf file size
	BufSize = 1024 * 1024 * 4 // 4 MB
)
