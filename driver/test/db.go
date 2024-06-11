package shelvetest

import (
	"errors"

	"github.com/lucmq/go-shelve/shelve"
)

// TestError is the error used in tests.
var TestError = errors.New("test error")

// TDB matches the shelve.DB interface.
type TDB = shelve.DB
