package shelve

import (
	"reflect"
	"testing"
)

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

func TestStringCodec_Encode(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  []byte
	}{
		{name: "Encode Bool", input: true, want: []byte("true")},
		{name: "Encode Int", input: 42, want: []byte("42")},
		{name: "Encode Int64", input: int64(42), want: []byte("42")},
		{name: "Encode Uint64", input: uint64(42), want: []byte("42")},
		{name: "Encode Float", input: 3.14, want: []byte("3.14")},
		{name: "Encode String", input: "hello", want: []byte("hello")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var codec stringCodec
			data, err := codec.Encode(tc.input)

			if err != nil {
				t.Errorf("Encode() failed: %v", err)
			}
			if !reflect.DeepEqual(data, tc.want) {
				t.Errorf("Encode() failed: got %v, want %v", data, tc.want)
			}
		})
	}
}

func TestStringCodec_Decode(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		wantError bool
	}{
		{name: "Decode Bool", input: true},
		{name: "Decode Int", input: 42},
		{name: "Decode Int64", input: int64(42)},
		{name: "Decode Uint64", input: uint64(42)},
		{name: "Decode Float", input: 3.14},
		{name: "Decode String", input: "hello"},
		{name: "Decode Error", input: MakeTestStruct(), wantError: true}, // Not scannable
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			var codec stringCodec
			data, err := codec.Encode(tc.input)
			if err != nil {
				t.Fatalf("Expected no error, but got %v", err)
			}
			if len(data) == 0 {
				t.Fatal("Expected data to be non-empty")
			}

			result, err := decodeAny(&codec, data, tc.input)

			if tc.wantError {
				if err == nil {
					t.Errorf("Decode() failed: got %v, want %v", err, "error")
				}
			} else {
				if err != nil {
					t.Errorf("Decode() failed: %v", err)
				}
				if !reflect.DeepEqual(result, tc.input) {
					t.Errorf("Decode() failed: got %v, want %v", result, tc.input)
				}
			}
		})
	}
}
