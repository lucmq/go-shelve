package shelve

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"
)

type testTextMarshaler struct {
	content string
}

func (m testTextMarshaler) MarshalText() ([]byte, error) {
	return []byte("marshaled:" + m.content), nil
}

// Gob

func TestGobCodec_Encode(t *testing.T) {
	var codec gobCodec

	// Run the tests in the suite
	EncodeTest(t, &codec)

	t.Run("Encode Error", func(t *testing.T) {
		fn := func() {}
		_, err := codec.Encode(fn)
		if err == nil {
			t.Errorf("Encode() failed: got %v, want %v", err, "error")
		}
	})
}

func TestGobCodec_Decode(t *testing.T) {
	var codec gobCodec

	// Run the tests in the suite
	DecodeTest(t, &codec)

	t.Run("Decode Empty", func(t *testing.T) {
		var s struct{}

		err := codec.Decode([]byte{}, &s)

		if err == nil {
			t.Errorf("Decode() failed: got %v, want %v", err, "error")
		}
		if s != struct{}{} {
			t.Errorf("Decode() failed: got %v, want %v", s, struct{}{})
		}
	})

	t.Run("Decode Error", func(t *testing.T) {
		var s struct{}
		err := codec.Decode([]byte{0x00, 0x01}, &s)
		if err == nil {
			t.Errorf("Decode() failed: got %v, want %v", err, "error")
		}
	})
}

// JSON

func TestJsonCodec_Encode(t *testing.T) {
	var codec jsonCodec

	// Run the tests in the suite
	EncodeTest(t, &codec)

	t.Run("Encode Error", func(t *testing.T) {
		fn := func() {}
		_, err := codec.Encode(fn)
		if err == nil {
			t.Errorf("Encode() failed: got %v, want %v", err, "error")
		}
	})
}

func TestJsonCodec_Decode(t *testing.T) {
	var codec jsonCodec

	// Run the tests in the suite
	DecodeTest(t, &codec)

	t.Run("Decode Empty", func(t *testing.T) {
		var s struct{}

		err := codec.Decode([]byte{}, &s)

		if err == nil {
			t.Errorf("Decode() failed: got %v, want %v", err, "error")
		}
		if s != struct{}{} {
			t.Errorf("Decode() failed: got %v, want %v", s, struct{}{})
		}
	})

	t.Run("Decode Error", func(t *testing.T) {
		var s struct{}
		err := codec.Decode([]byte{0x00, 0x01}, &s)
		if err == nil {
			t.Errorf("Decode() failed: got %v, want %v", err, "error")
		}
	})
}

// String

func TestTextCodec_Encode(t *testing.T) {
	now := time.Date(2023, 10, 20, 15, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		input any
		want  []byte
	}{
		{name: "Encode Bool True", input: true, want: []byte("true")},
		{name: "Encode Bool False", input: false, want: []byte("false")},

		{name: "Encode Int", input: int(42), want: []byte("42")},
		{name: "Encode Int8", input: int8(42), want: []byte("42")},
		{name: "Encode Int16", input: int16(42), want: []byte("42")},
		{name: "Encode Int32", input: int32(42), want: []byte("42")},
		{name: "Encode Int64", input: int64(42), want: []byte("42")},

		{name: "Encode Uint", input: uint(42), want: []byte("42")},
		{name: "Encode Uint8", input: uint8(42), want: []byte("42")},
		{name: "Encode Uint16", input: uint16(42), want: []byte("42")},
		{name: "Encode Uint32", input: uint32(42), want: []byte("42")},
		{name: "Encode Uint64", input: uint64(42), want: []byte("42")},

		{name: "Encode Float32", input: float32(3.14), want: []byte("3.14")},
		{name: "Encode Float64", input: float64(3.14), want: []byte("3.14")},

		{name: "Encode String", input: "hello", want: []byte("hello")},

		// encoding.TextMarshaler
		{name: "Encode time.Time", input: now, want: []byte(now.Format(time.RFC3339))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var codec textCodec
			data, err := codec.Encode(tc.input)

			if err != nil {
				t.Errorf("Encode() failed: %v", err)
			}
			if !reflect.DeepEqual(data, tc.want) {
				t.Errorf("Encode() output = %q, want %q", data, tc.want)
			}
		})
	}
}

func TestTextCodec_EncodeError(t *testing.T) {
	var codec textCodec
	_, err := codec.Encode(func() {})
	if err == nil {
		t.Errorf("Encode() failed: got %v, want %v", err, "error")
	}
}

func TestTextCodec_Decode(t *testing.T) {
	now := time.Date(2023, 10, 20, 15, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		input     any
		wantError bool
	}{
		// Scalars
		{name: "Bool True", input: true},
		{name: "Bool False", input: false},

		{name: "Int", input: int(42)},
		{name: "Int8", input: int8(42)},
		{name: "Int16", input: int16(42)},
		{name: "Int32", input: int32(42)},
		{name: "Int64", input: int64(42)},

		{name: "Uint", input: uint(42)},
		{name: "Uint8", input: uint8(42)},
		{name: "Uint16", input: uint16(42)},
		{name: "Uint32", input: uint32(42)},
		{name: "Uint64", input: uint64(42)},

		{name: "Float32", input: float32(3.14)},
		{name: "Float64", input: float64(3.14)},

		{name: "String", input: "hello"},

		// encoding.TextMarshaler / TextUnmarshaler
		{name: "Time", input: now},

		// Unsupported
		{name: "Unsupported struct", input: testTextMarshaler{content: "foo"}, wantError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var codec textCodec

			// Encode
			data, err := codec.Encode(tc.input)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Allocate zero value of the same type (by pointer)
			ptr := reflect.New(reflect.TypeOf(tc.input)).Interface()

			// Decode into it
			err = codec.Decode(data, ptr)

			if tc.wantError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected decode error: %v", err)
				return
			}

			got := reflect.ValueOf(ptr).Elem().Interface()
			if !reflect.DeepEqual(got, tc.input) {
				t.Errorf("Decode() = %v (%T), want %v (%T)", got, got, tc.input, tc.input)
			}
		})
	}
}

// Other

func TestDecodeFixedByteArray_EdgeCases(t *testing.T) {
	t.Run("Invalid hex input", func(t *testing.T) {
		var out [12]byte
		// "zz" is not valid hex
		err := decodeFixedByteArray([]byte("zzzzzzzzzzzzzzzzzzzzzzzz"), &out)
		if err == nil || !strings.Contains(err.Error(), "hex decode failed") {
			t.Errorf("Expected hex decode error, got: %v", err)
		}
	})

	t.Run("Wrong decoded length", func(t *testing.T) {
		var out [12]byte
		// Only 10 bytes in hex = 20 hex chars, but we expect 12 bytes (24 hex chars)
		hexStr := hex.EncodeToString([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) // 10 bytes
		err := decodeFixedByteArray([]byte(hexStr), &out)
		if err == nil || !strings.Contains(err.Error(), "invalid hex length") {
			t.Errorf("Expected invalid hex length error, got: %v", err)
		}
	})
}
