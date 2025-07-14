package shelve

import (
	"bytes"
	"encoding"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

// Codec is the interface for encoding and decoding data stored by Shelf.
//
// The go-shelve module natively supports the following codecs:
//   - [GobCodec]: Returns a Codec for the [gob] format.
//   - [JSONCodec]: Returns a Codec for the [json] format.
//   - [TextCodec]: Returns a Codec for values that can be represented as
//     plain text.
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

// TextCodec Returns a Codec for values that can be represented as plain text.
func TextCodec() Codec { return textCodec{} }

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

// Text Codec

// textCodec encodes scalar values, fixed-size byte arrays, and types that
// implement encoding.TextMarshaler.
// It supports strings, booleans, integers, floats, and [N]byte arrays (encoded
// as hex).
type textCodec struct{}

func (textCodec) Encode(value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case int:
		return []byte(strconv.Itoa(v)), nil
	case int8:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(v, 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint8:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(v, 10)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(v), 'g', -1, 32)), nil
	case float64:
		return []byte(strconv.FormatFloat(v, 'g', -1, 64)), nil
	case bool:
		return []byte(strconv.FormatBool(v)), nil
	case encoding.TextMarshaler:
		return v.MarshalText()
	default:
		if encoded, ok := encodeFixedByteArray(value); ok {
			return encoded, nil
		}
		return nil, fmt.Errorf("textCodec: unsupported type %T", value)
	}
}

func (textCodec) Decode(data []byte, value any) error {
	str := string(data)

	switch v := value.(type) {
	case *string:
		*v = str
		return nil
	case *int:
		i, err := strconv.Atoi(str)
		*v = i
		return err
	case *int8:
		i, err := strconv.ParseInt(str, 10, 8)
		*v = int8(i)
		return err
	case *int16:
		i, err := strconv.ParseInt(str, 10, 16)
		*v = int16(i)
		return err
	case *int32:
		i, err := strconv.ParseInt(str, 10, 32)
		*v = int32(i)
		return err
	case *int64:
		i, err := strconv.ParseInt(str, 10, 64)
		*v = i
		return err
	case *uint:
		u, err := strconv.ParseUint(str, 10, 0)
		*v = uint(u)
		return err
	case *uint8:
		u, err := strconv.ParseUint(str, 10, 8)
		*v = uint8(u)
		return err
	case *uint16:
		u, err := strconv.ParseUint(str, 10, 16)
		*v = uint16(u)
		return err
	case *uint32:
		u, err := strconv.ParseUint(str, 10, 32)
		*v = uint32(u)
		return err
	case *uint64:
		u, err := strconv.ParseUint(str, 10, 64)
		*v = u
		return err
	case *float32:
		f, err := strconv.ParseFloat(str, 32)
		*v = float32(f)
		return err
	case *float64:
		f, err := strconv.ParseFloat(str, 64)
		*v = f
		return err
	case *bool:
		b, err := strconv.ParseBool(str)
		*v = b
		return err
	default:
		if u, ok := value.(encoding.TextUnmarshaler); ok {
			return u.UnmarshalText(data)
		}
		if err := decodeFixedByteArray(data, value); err == nil {
			return nil
		}
		return fmt.Errorf("textCodec: unsupported decode target %T", value)
	}
}

func encodeFixedByteArray(v any) ([]byte, bool) {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Array && val.Type().Elem().Kind() == reflect.Uint8 {
		n := val.Len()
		buf := make([]byte, n)
		for i := 0; i < n; i++ {
			buf[i] = byte(val.Index(i).Uint())
		}
		return []byte(hex.EncodeToString(buf)), true
	}
	return nil, false
}

func decodeFixedByteArray(data []byte, out any) error {
	val := reflect.ValueOf(out)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Array || val.Elem().Type().Elem().Kind() != reflect.Uint8 {
		return fmt.Errorf("unsupported decode target: %T", out)
	}

	arr := val.Elem()
	expectedLen := arr.Len()
	decoded, err := hex.DecodeString(string(data))
	if err != nil {
		return fmt.Errorf("hex decode failed: %w", err)
	}
	if len(decoded) != expectedLen {
		return fmt.Errorf("invalid hex length: got %d bytes, want %d", len(decoded), expectedLen)
	}

	for i := 0; i < expectedLen; i++ {
		arr.Index(i).SetUint(uint64(decoded[i]))
	}
	return nil
}
