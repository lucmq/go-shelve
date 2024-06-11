package shelve

import (
	"reflect"
	"testing"
)

// Test Suite

// EncodeTest runs the Encode tests in the suite. It checks the encoding of
// different types and structs.
func EncodeTest(t *testing.T, codec Codec) {
	tests := []struct {
		name  string
		input any
	}{
		{
			name:  "Encode Empty Struct",
			input: struct{}{},
		},
		{
			name:  "Encode Int",
			input: 1,
		},
		{
			name:  "Encode Uint",
			input: uint(1),
		},
		{
			name:  "Encode Float",
			input: 1.0,
		},
		{
			name:  "Encode Bool",
			input: true,
		},
		{
			name:  "Encode String",
			input: "hello",
		},
		{
			name:  "Encode TestStruct",
			input: MakeTestStruct(),
		},
		{
			name: "Encode Map[string]string",
			input: map[string]string{
				"key-1": "value-1", "key-2": "value-2",
				"key-3": "value-3", "key-4": "value-4",
			},
		},
		{
			name:  "Encode Slice[string]",
			input: []string{"key-1", "key-2", "key-3", "key-4"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := codec.Encode(tc.input)
			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
			if len(data) == 0 {
				t.Errorf("Expected data to be non-empty")
			}
		})
	}
}

// DecodeTest runs the Decode tests in the suite. It checks the decoding of
// different types and structs.
func DecodeTest(t *testing.T, codec Codec) {
	tests := []struct {
		name  string
		input any
	}{
		{
			name:  "Decode Empty Struct",
			input: struct{}{},
		},
		{
			name:  "Decode Int",
			input: 1,
		},
		{
			name:  "Decode Uint",
			input: uint(1),
		},
		{
			name:  "Decode Float",
			input: 1.0,
		},
		{
			name:  "Decode Bool",
			input: true,
		},
		{
			name:  "Decode String",
			input: "hello",
		},
		{
			name:  "Decode TestStruct",
			input: MakeTestStruct(),
		},
		{
			name: "Decode Map[string]string",
			input: map[string]string{
				"key-1": "value-1", "key-2": "value-2",
				"key-3": "value-3", "key-4": "value-4",
			},
		},
		{
			name:  "Decode Slice[string]",
			input: []string{"key-1", "key-2", "key-3", "key-4"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			data, err := codec.Encode(tc.input)
			if err != nil {
				t.Fatalf("Expected no error, but got %v", err)
			}
			if len(data) == 0 {
				t.Fatal("Expected data to be non-empty")
			}

			// Act
			result, err := decodeAny(codec, data, tc.input)

			// Assert
			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
			if !reflect.DeepEqual(result, tc.input) {
				t.Errorf("Expected %v, but got %v", tc.input, result)
			}
		})
	}
}

func decodeAny(codec Codec, data []byte, instance any) (result any, err error) {
	// Create an instance with the same underlying type as
	// the input
	x := reflect.New(reflect.TypeOf(instance)).Interface()

	err = codec.Decode(data, x)
	// Dereference `x` to get the actual value, but then get
	// an interface{} from that.
	result = reflect.ValueOf(x).Elem().Interface()

	return result, err
}
