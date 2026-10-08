//go:build !linux

package journal

import "os"

func retainedOpenDirectory(*os.Root) *rootedDirectory { return nil }

func openJournalRootFile(root *os.Root, _ *rootedDirectory, _, relative string, flags int, mode os.FileMode) (*os.File, error) {
	return root.OpenFile(relative, flags, mode)
}
