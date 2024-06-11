package shelvetest

import (
	"github.com/lucmq/go-shelve/shelve"
)

// Codec is an interface for encoding and decoding. It is an alias for the
// shelve.Codec interface.
type Codec = shelve.Codec

// TestStruct is used to test the codec with a struct that contains different
// types.
type TestStruct struct {
	U64 uint64
	S   string
	F64 float64
	I   int
	B   bool
}

// MakeTestStruct creates a new TestStruct.
func MakeTestStruct() TestStruct {
	return TestStruct{
		U64: 1,
		S:   "test",
		F64: 1.0,
		I:   1,
		B:   true,
	}
}
