package sdb

import (
	"os"
	"syscall"
	"unsafe"
)

// The implementation bellow is adapted from: github.com/natefinch/atomic

const (
	moveFileReplaceExisting = 0x1
	moveFileWriteThrough    = 0x8
)

// renameFile atomically replaces the destination file or directory with the
// source. It is guaranteed to either replace the target file entirely, or not
// change either file.
func renameFile(source, destination string) error {
	src, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return &os.LinkError{"replace", source, destination, err}
	}
	dest, err := syscall.UTF16PtrFromString(destination)
	if err != nil {
		return &os.LinkError{"replace", source, destination, err}
	}

	// see http://msdn.microsoft.com/en-us/library/windows/desktop/aa365240(v=vs.85).aspx
	err = moveFileEx(src, dest, moveFileReplaceExisting|moveFileWriteThrough)
	if err != nil {
		return &os.LinkError{"replace", source, destination, err}
	}
	return nil
}

var (
	modkernel32     = syscall.NewLazyDLL("kernel32.dll")
	procMoveFileExW = modkernel32.NewProc("MoveFileExW")
)

func moveFileEx(
	lpExistingFileName *uint16, lpNewFileName *uint16, dwFlags uint32,
) (err error) {
	r1, _, e1 := syscall.Syscall(
		procMoveFileExW.Addr(),
		3,
		uintptr(unsafe.Pointer(lpExistingFileName)),
		uintptr(unsafe.Pointer(lpNewFileName)),
		uintptr(dwFlags),
	)
	if r1 == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}
