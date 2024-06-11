package shelve

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strconv"
)

// Codec is the interface for encoding and decoding data stored by Shelf.
//
// The go-shelve module natively supports the following codecs:
//   - [GobCodec]: Returns a Codec for the [gob] format.
//   - [JSONCodec]: Returns a Codec for the JSON format.
//   - [StringCodec]: Returns a Codec for values that can be represented as
//     strings.
//
// Additional codecs are provided by the packages in [driver/encoding].
//
// [driver/encoding]: https://pkg.go.dev/github.com/lucmq/go-shelve/driver/encoding
type Codec interface {
	// Encode returns the Codec encoding of v as a byte slice.
	Encode(v any) ([]byte, error)

	// Decode parses the encoded data and stores the result in the value
	// pointed to by v. It is the inverse of Encode.
	Decode(data []byte, v any) error
}

// GobCodec Returns a Codec for the [gob] format, a self-describing
// serialization format native to Go.
//
// Gob is a binary format and is more compact than text-based formats like
// JSON.
func GobCodec() Codec { return gobCodec{} }

// JSONCodec Returns a Codec for the JSON format.
func JSONCodec() Codec { return jsonCodec{} }

// StringCodec Returns a Codec for values that can be represented as strings.
func StringCodec() Codec { return stringCodec{} }

// Gob Codec

type gobCodec struct{}

func (gobCodec) Encode(value any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode gob: %w", err)
	}
	return buf.Bytes(), nil
}

func (gobCodec) Decode(data []byte, value any) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(value)
	if err != nil {
		return fmt.Errorf("decode gob: %w", err)
	}
	return nil
}

// Json Codec

type jsonCodec struct{}

func (jsonCodec) Encode(value any) ([]byte, error) {
	return json.MarshalIndent(value, "", "  ")
}

func (jsonCodec) Decode(data []byte, value any) error {
	return json.Unmarshal(data, value)
}

// String Codec

type stringCodec struct{}

func (stringCodec) Encode(value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case int:
		return []byte(strconv.Itoa(v)), nil
	case int64:
		return []byte(strconv.FormatInt(v, 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(v, 10)), nil
	default:
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}

func (stringCodec) Decode(data []byte, value any) error {
	switch v := value.(type) {
	case *string:
		*v = string(data)
		return nil
	case *int:
		i, err := strconv.Atoi(string(data))
		*v = i
		return err
	case *int64:
		i, err := strconv.ParseInt(string(data), 10, 64)
		*v = i
		return err
	case *uint64:
		u, err := strconv.ParseUint(string(data), 10, 64)
		*v = u
		return err
	default:
		_, err := fmt.Sscanf(string(data), "%v", value)
		return err
	}
}
