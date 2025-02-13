//go:build !windows
// +build !windows

package sdb

import (
	"fmt"
	"os"
	"os/exec"
)

// renameFile atomically replaces the destination file or directory with the
// source. It is guaranteed to either replace the target file entirely, or not
// change either file.
func renameFile(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// _setMaxOpenFiles sets the system's max open files limit on Unix-like systems
// using sysctl.
func _setMaxOpenFiles(limit int) error {
	cmd := exec.Command("sysctl", fmt.Sprintf("fs.file-max=%d", limit))
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("set sysctl fs.file-max=%d: %w", limit, err)
	}
	return nil
}
