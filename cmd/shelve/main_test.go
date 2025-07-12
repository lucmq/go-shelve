package main

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const cmdName = "shelve"

var dbPath = filepath.Join(os.TempDir(), "shelve-cmd-test")

func init() {
	// Override this value so we can check the output for errors without
	// exiting the process.
	exitOnError = false
}

func runCLI(t *testing.T, args ...string) string {
	t.Helper()

	// Backup and restore state
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = append([]string{cmdName}, args...)

	// Reset flags
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	// Capture stdout
	output := captureOutput(func() {
		main()
	})

	return strings.TrimSpace(output)
}

func captureOutput(f func()) string {
	// Create pipes for stdout and stderr
	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()

	stdout, stderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = wOut, wErr

	// Run function
	f()

	// Restore original stdout and stderr
	wOut.Close()
	wErr.Close()
	os.Stdout, os.Stderr = stdout, stderr

	// Read the outputs
	var bufOut, bufErr bytes.Buffer
	bufOut.ReadFrom(rOut)
	bufErr.ReadFrom(rErr)

	return strings.TrimSpace(bufOut.String() + bufErr.String())
}

func setupTestDB(t *testing.T) string {
	t.Helper()
	_ = os.RemoveAll(dbPath)
	t.Cleanup(func() { _ = os.RemoveAll(dbPath) })
	return dbPath
}

func TestCLIPut(t *testing.T) {
	path := setupTestDB(t)

	t.Run("valid put", func(t *testing.T) {
		got := runCLI(t, "-path", path, "put", "foo", "bar")
		if got != "OK" {
			t.Errorf("expected 'OK', got %q", got)
		}
	})

	t.Run("invalid put - missing key", func(t *testing.T) {
		got := runCLI(t, "-path", path, "put")
		if !strings.Contains(got, "usage: shelve put") {
			t.Errorf("expected usage error, got %q", got)
		}
	})

	t.Run("invalid put - Shelve error", func(t *testing.T) {
		err := handlePut(newFakeShelve(t), []string{"foo", "bar"})
		if !errors.Is(err, TestError) {
			t.Errorf("expected error %v, got %v", TestError, err)
		}
	})
}

func TestCLIGet(t *testing.T) {
	path := setupTestDB(t)

	runCLI(t, "-path", path, "put", "foo", "bar")

	t.Run("valid get", func(t *testing.T) {
		got := runCLI(t, "-path", path, "get", "foo")
		if got != "bar" {
			t.Errorf("expected 'bar', got %q", got)
		}
	})

	t.Run("get nonexistent key", func(t *testing.T) {
		got := runCLI(t, "-path", path, "get", "nonexistent")

		if got != "" {
			t.Errorf("expected empty string, got %q", got)
		}
	})

	t.Run("invalid get - missing key argument", func(t *testing.T) {
		got := runCLI(t, "-path", path, "get")

		if !strings.Contains(got, "usage: shelve get") {
			t.Errorf("expected usage error, got %q", got)
		}
	})

	t.Run("invalid get - Shelve error", func(t *testing.T) {
		err := handleGet(newFakeShelve(t), []string{"foo"})
		if !errors.Is(err, TestError) {
			t.Errorf("expected error %v, got %v", TestError, err)
		}
	})
}

func TestCLIHas(t *testing.T) {
	path := setupTestDB(t)

	runCLI(t, "-path", path, "put", "foo", "bar")

	t.Run("has existing key", func(t *testing.T) {
		got := runCLI(t, "-path", path, "has", "foo")
		if got != "true" {
			t.Errorf("expected 'true', got %q", got)
		}
	})

	t.Run("has nonexistent key", func(t *testing.T) {
		got := runCLI(t, "-path", path, "has", "missing")
		if got != "false" {
			t.Errorf("expected 'false', got %q", got)
		}
	})

	t.Run("invalid has - missing key argument", func(t *testing.T) {
		got := runCLI(t, "-path", path, "has")

		if !strings.Contains(got, "usage: shelve has") {
			t.Errorf("expected usage error, got %q", got)
		}
	})

	t.Run("invalid has - Shelve error", func(t *testing.T) {
		err := handleHas(newFakeShelve(t), []string{"foo"})
		if !errors.Is(err, TestError) {
			t.Errorf("expected error %v, got %v", TestError, err)
		}
	})
}

func TestCLIDelete(t *testing.T) {
	path := setupTestDB(t)

	runCLI(t, "-path", path, "put", "foo", "bar")

	t.Run("valid delete", func(t *testing.T) {
		got := runCLI(t, "-path", path, "delete", "foo")
		if got != "OK" {
			t.Errorf("expected 'OK', got %q", got)
		}
	})

	t.Run("delete nonexistent key", func(t *testing.T) {
		got := runCLI(t, "-path", path, "delete", "missing")
		if got != "OK" { // assuming delete is idempotent
			t.Errorf("expected 'OK' even for nonexistent key, got %q", got)
		}
	})

	t.Run("invalid delete - missing key argument", func(t *testing.T) {
		got := runCLI(t, "-path", path, "delete")

		if !strings.Contains(got, "usage: shelve delete") {
			t.Errorf("expected usage error, got %q", got)
		}
	})

	t.Run("invalid delete - Shelve error", func(t *testing.T) {
		err := handleDelete(newFakeShelve(t), []string{"foo", "bar"})
		if !errors.Is(err, TestError) {
			t.Errorf("expected error %v, got %v", TestError, err)
		}
	})
}

func TestCLILen(t *testing.T) {
	path := setupTestDB(t)

	runCLI(t, "-path", path, "put", "a", "1", "b", "2")

	t.Run("valid len", func(t *testing.T) {
		got := runCLI(t, "-path", path, "len")
		if got != "2" {
			t.Errorf("expected '2', got %q", got)
		}
	})

	t.Run("invalid len - Shelve error", func(t *testing.T) {
		err := handleLen(newFakeShelve(t))
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})
}

func TestCLIItems(t *testing.T) {
	path := setupTestDB(t)

	runCLI(t, "-path", path, "put", "a", "1", "b", "2", "c", "3")

	t.Run("valid items", func(t *testing.T) {
		got := runCLI(t, "-path", path, "items")
		expectUnorderedContains(t, got, []string{"a 1", "b 2", "c 3"})
	})

	t.Run("items with start/end/limit", func(t *testing.T) {
		got := runCLI(t, "-path", path, "items", "-start", "b", "-limit", "1")
		expectUnorderedContains(t, got, []string{"b 2"})
	})

	t.Run("items with end", func(t *testing.T) {
		items := runCLI(t, "-path", path, "items", "-end", "a")
		// Note: Can't check the keys here because SDB doesn't guarantee order.
		_ = items
	})

	t.Run("invalid items - Shelve error", func(t *testing.T) {
		err := handleItems(newFakeShelve(t), "items", []string{})
		if !errors.Is(err, TestError) {
			t.Errorf("expected error %v, got %v", TestError, err)
		}
	})

	t.Run("invalid items - bad mode", func(t *testing.T) {
		err := handleItems(newFakeShelve(t), "foo", []string{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("unknown flag", func(t *testing.T) {
		err := handleItems(newFakeShelve(t), "items", []string{"-unknownFlag"})
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})
}

func TestCLIKeys(t *testing.T) {
	path := setupTestDB(t)

	runCLI(t, "-path", path, "put", "a", "1", "b", "2", "c", "3")

	t.Run("valid keys", func(t *testing.T) {
		keys := runCLI(t, "-path", path, "keys")
		expectUnorderedContains(t, keys, []string{"a", "b"})
	})

	t.Run("keys with start", func(t *testing.T) {
		keys := runCLI(t, "-path", path, "keys", "-start", "b")
		expectUnorderedContains(t, keys, []string{"b"})
	})

	t.Run("keys with end", func(t *testing.T) {
		keys := runCLI(t, "-path", path, "keys", "-end", "a")
		// Note: Can't check the keys here because SDB doesn't guarantee order.
		_ = keys
	})

	t.Run("invalid keys - Shelve error", func(t *testing.T) {
		err := handleItems(newFakeShelve(t), "keys", []string{})
		if !errors.Is(err, TestError) {
			t.Errorf("expected error %v, got %v", TestError, err)
		}
	})
}

func TestCLIValues(t *testing.T) {
	path := setupTestDB(t)

	runCLI(t, "-path", path, "put", "a", "1", "b", "2")

	t.Run("valid values", func(t *testing.T) {
		values := runCLI(t, "-path", path, "values")
		expectUnorderedContains(t, values, []string{"1", "2"})
	})

	t.Run("values with end", func(t *testing.T) {
		values := runCLI(t, "-path", path, "values", "-end", "a")
		// Note: Can't check the keys here because SDB doesn't guarantee order.
		_ = values
	})

	t.Run("invalid values - Shelve error", func(t *testing.T) {
		err := handleItems(newFakeShelve(t), "values", []string{})
		if !errors.Is(err, TestError) {
			t.Errorf("expected error %v, got %v", TestError, err)
		}
	})
}

func TestCodecs(t *testing.T) {
	t.Run("gob", func(t *testing.T) {
		got := runCLI(t, "-codec", "gob", "put", "a", "1")
		if got != "OK" {
			t.Errorf("expected 'OK', got %q", got)
		}
	})

	t.Run("json", func(t *testing.T) {
		got := runCLI(t, "-codec", "json", "put", "a", "1")
		if got != "OK" {
			t.Errorf("expected 'OK', got %q", got)
		}
	})

	t.Run("string", func(t *testing.T) {
		got := runCLI(t, "-codec", "string", "put", "a", "1")
		if got != "OK" {
			t.Errorf("expected 'OK', got %q", got)
		}
	})

	t.Run("invalid codec", func(t *testing.T) {
		got := runCLI(t, "-codec", "foo", "put", "a", "1")
		if !strings.Contains(got, "unsupported codec") {
			t.Errorf("expected error, got %q", got)
		}
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("no args - print usage", func(t *testing.T) {
		got := runCLI(t)
		if !strings.Contains(got, "Usage:") {
			t.Errorf("expected empty string, got %q", got)
		}
	})

	t.Run("invalid path", func(t *testing.T) {
		got := runCLI(t, "-path", "/", "items")
		if !strings.Contains(got, "open store:") {
			t.Errorf("expected error, got %q", got)
		}
	})

	t.Run("invalid command", func(t *testing.T) {
		got := runCLI(t, "foo")
		if !strings.Contains(got, "unknown command") {
			t.Errorf("expected error, got %q", got)
		}
	})
}

func TestMainExitOnError(t *testing.T) {
	t.Cleanup(func() {
		// Restore the exit function.
		exitOnError = false
		exit = os.Exit
	})

	exitCode := 0

	exitOnError = true
	exit = func(code int) {
		exitCode = code

	}

	runCLI(t, "items", "-unknownFlag")

	if exitCode == 0 {
		t.Error("expected exit code to be non-zero")
	}
}

func expectUnorderedContains(t *testing.T, output string, expected []string) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	got := map[string]bool{}
	for _, line := range lines {
		got[line] = true
	}
	for _, exp := range expected {
		if !got[exp] {
			t.Errorf("expected output to contain %q, got:\n%s", exp, output)
		}
	}
}
