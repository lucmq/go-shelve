//go:build !windows

package sdb

import "os"

// renameFile atomically replaces the destination file or directory with the
// source. It is guaranteed to either replace the target file entirely, or not
// change either file.
func renameFile(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}
