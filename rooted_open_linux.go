//go:build linux

package journal

import (
	"os"
	"syscall"
)

func retainedOpenDirectory(root *os.Root) *rootedDirectory {
	fp, err := root.Open(".")
	if err != nil {
		return nil
	}
	return &rootedDirectory{file: fp, fd: fp.Fd()}
}

func openJournalRootFile(root *os.Root, directory *rootedDirectory, label, relative string, flags int, mode os.FileMode) (*os.File, error) {
	// relative has already been checked to be "." or exactly one child name.
	// No parent component can redirect this open, and O_NOFOLLOW atomically
	// prevents a replaced child from redirecting it through a symbolic link.
	// Root handles all symlinks, including safe relative links, with its full
	// confinement algorithm. Never substitute an unconstrained following open.
	const supported = os.O_RDWR | os.O_WRONLY | os.O_CREATE | os.O_EXCL | os.O_TRUNC
	if directory == nil || flags & ^supported != 0 || mode & ^os.ModePerm != 0 {
		return root.OpenFile(relative, flags, mode)
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	if directory.closed {
		return nil, &os.PathError{Op: "openat", Path: label, Err: os.ErrClosed}
	}
	var fd int
	var err error
	for {
		fd, err = syscall.Openat(int(directory.fd), relative,
			flags|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, uint32(mode.Perm()))
		if err != syscall.EINTR {
			break
		}
	}
	if err == syscall.ELOOP || err == syscall.ENOSYS {
		return root.OpenFile(relative, flags, mode)
	}
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: label, Err: err}
	}
	fp := os.NewFile(uintptr(fd), label)
	if fp == nil {
		syscall.Close(fd)
		return nil, &os.PathError{Op: "openat", Path: label, Err: os.ErrInvalid}
	}
	return fp, nil
}
