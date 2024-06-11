package shelvetest

import (
	"errors"
	"go-shelve/shelve"
)

var TestError = errors.New("test error")

// TDB matches the shelve.DB interface.
type TDB = shelve.DB
