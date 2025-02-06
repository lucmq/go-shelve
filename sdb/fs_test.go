package sdb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

var (
	TestDirPermissions = os.FileMode(0700)
)

func TestAtomicWriter_WriteFile(t *testing.T) {
	tests := []struct {
		name   string
		writer *atomicWriter
	}{
		{
			name:   "Sync Write",
			writer: newAtomicWriter(true),
		},
		{
			name:   "Async Write",
			writer: newAtomicWriter(false),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runAtomicWriterWriteFileTests(t, test.writer)
		})
	}
}

func runAtomicWriterWriteFileTests(t *testing.T, writer *atomicWriter) {
	// Set up a temporary directory for testing.
	tmpDir, err := os.MkdirTemp("", "test_atomic_writer")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("excl=false", func(t *testing.T) {
		path := filepath.Join(tmpDir, "test_file.txt")
		data := []byte("Hello, world!")

		err := writer.WriteFile(path, data, false)
		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}

		// Check if the file was created and has the correct contents.
		fileData, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("Failed to read file: %v", err)
		}
		if !bytes.Equal(fileData, data) {
			t.Errorf("Expected file contents to be %q, but got %q",
				data, fileData)
		}
	})

	t.Run("excl=true when file exists", func(t *testing.T) {
		path := filepath.Join(tmpDir, "test_file.txt")
		data := []byte("Hello, world!")

		// On Windows, the os.O_EXCL flag does not work reliably with os.WriteFile.
		// Instead, we create a new file without closing it because it will remain
		// locked as long as it stays open.
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, defaultPermissions)
		if err != nil {
			t.Fatalf("Failed to create existing file: %v", err)
		}
		_, err = f.Write([]byte("Existing data"))

		// Attempt to write with exclusive mode enabled
		err = writer.WriteFile(path, data, true)
		if err == nil {
			t.Errorf("Expected an error, but got nil")
		}

		// Check if the file contents are unchanged.
		fileData, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("Failed to read file: %v", err)
		}
		if string(fileData) != "Existing data" {
			t.Errorf("Expected file contents to remain unchanged")
		}
	})

	t.Run("Large file", func(t *testing.T) {
		path := filepath.Join(tmpDir, "large_file.txt")
		data := make([]byte, 10*1024*1024) // 10MB

		err := writer.WriteFile(path, data, false)
		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}

		// Check if the file was created and has the correct contents.
		fileData, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("Failed to read file: %v", err)
		}
		if len(fileData) != len(data) {
			t.Errorf("Expected file size to be %d, but got %d",
				len(data), len(fileData))
		}
	})
}

func TestMkdirs(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		paths := []string{
			filepath.Join(os.TempDir(), "test-1234"),
			filepath.Join(os.TempDir(), "test-1234", "data"),
			filepath.Join(os.TempDir(), "test-1234", "metadata"),
		}
		if err := mkdirs(paths, TestDirPermissions); err != nil {
			t.Fatalf("mkdirs: %s", err)
		}
		// Quick check (not recursive)
		for _, path := range paths {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("stat: %s", err)
			}
		}
	})

	t.Run("Error", func(t *testing.T) {
		paths := []string{""}
		err := mkdirs(paths, TestDirPermissions)
		if err == nil {
			t.Fatalf("expected error")
		}
	})
}

func TestReadDir(t *testing.T) {
	// Prepare
	dirs := map[string]string{
		"empty": filepath.Join(os.TempDir(), "test-dir-empty"),
		"test":  filepath.Join(os.TempDir(), "test-dir"),
	}
	files := []string{
		filepath.Join(dirs["test"], "file1.txt"),
		filepath.Join(dirs["test"], "file2.txt"),
	}
	t.Cleanup(func() {
		for _, dir := range dirs {
			_ = os.RemoveAll(dir)
		}
	})
	for _, dir := range dirs {
		_ = os.MkdirAll(dir, TestDirPermissions)
	}
	for _, file := range files {
		_ = os.WriteFile(file, []byte("test"), TestDirPermissions)
	}

	// Run tests
	t.Run("Empty directory", func(t *testing.T) {
		err := readDir(dirs["empty"], func(name string) (bool, error) {
			return false, errors.New("should not be called")
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Directory exists", func(t *testing.T) {
		err := readDir(dirs["test"], func(name string) (bool, error) {
			if name != "file1.txt" && name != "file2.txt" {
				return false, errors.New("unexpected file name")
			}
			return true, nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Directory does not exist", func(t *testing.T) {
		dir := filepath.Join(os.TempDir(), "does-not-exist")

		err := readDir(dir, func(name string) (bool, error) {
			return false, errors.New("should not be called")
		})

		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected 'file does not exist' error, got: %v", err)
		}
	})

	t.Run("Callback error", func(t *testing.T) {
		err := readDir(dirs["test"], func(name string) (bool, error) {
			return false, TestError
		})
		if !errors.Is(err, TestError) {
			t.Errorf("expected 'test error' error, got: %v", err)
		}
	})

	t.Run("Stop iteration", func(t *testing.T) {
		err := readDir(dirs["test"], func(name string) (bool, error) {
			return false, nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
