package journal

import "fmt"

var (
	// ErrDuringRotate rotate error
	ErrDuringRotate = fmt.Errorf("during rotating")
)
