// Package msgpack provides a MessagePack driver for go-shelve.
package msgpack

import (
	"github.com/vmihailenco/msgpack/v5"

	"github.com/lucmq/go-shelve/shelve"
)

// Codec is a codec for msgpack. Codec implements the shelve.Codec interface.
type Codec struct{}

// Assert Codec implements shelve.Codec
var _ shelve.Codec = (*Codec)(nil)

// NewDefault creates a new Codec with default values.
func NewDefault() *Codec {
	return &Codec{}
}

// Encode returns the msgpack Codec encoding of v as a byte slice.
func (e *Codec) Encode(value any) ([]byte, error) {
	return msgpack.Marshal(value)
}

// Decode parses the encoded msgpack data and stores the result in the value
// pointed to by v. It is the inverse of Encode.
func (e *Codec) Decode(data []byte, value any) error {
	return msgpack.Unmarshal(data, value)
}
