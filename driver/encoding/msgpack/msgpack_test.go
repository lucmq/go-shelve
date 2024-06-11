package msgpack

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	shelvetest "go-shelve/driver/test"
	"log"
	"testing"
)

type Product struct {
	ID       uint64
	Name     string
	Price    float64
	Quantity int
	Active   bool
}

func (p *Product) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.UseInternedStrings(true)
	enc.UseCompactFloats(true)
	enc.UseCompactInts(true)
	return enc.EncodeMulti(p.ID, p.Name, p.Price, p.Quantity, p.Active)
}

func (p *Product) DecodeMsgpack(dec *msgpack.Decoder) error {
	dec.UseInternedStrings(true)
	dec.UsePreallocateValues(true)
	return dec.DecodeMulti(&p.ID, &p.Name, &p.Price, &p.Quantity, &p.Active)
}

func Example() {
	product := Product{
		ID:       1,
		Name:     "Apple",
		Price:    1.0,
		Quantity: 1,
		Active:   true,
	}

	codec := NewDefault()
	b, err := codec.Encode(&product)
	if err != nil {
		log.Printf("encode msgpack: %s", err)
		return
	}

	err = codec.Decode(b, &product)
	if err != nil {
		log.Printf("decode msgpack: %s", err)
		return
	}

	fmt.Println(product.Name, product.Price)

	// Output: Apple 1
}

// Tests

func TestMsgpackEncode(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		enc := NewDefault()

		type TestStruct struct {
			Field1 string
			Field2 int
		}

		testStruct := TestStruct{
			Field1: "test",
			Field2: 123,
		}

		encoded, err := enc.Encode(&testStruct)
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		var decoded TestStruct
		err = msgpack.Unmarshal(encoded, &decoded)
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		if testStruct != decoded {
			t.Errorf("Expected %v, but got %v", testStruct, decoded)
		}
	})

	t.Run("Suite", func(t *testing.T) {
		shelvetest.EncodeTest(t, NewDefault())
	})
}

func TestMsgpackDecode(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		enc := NewDefault()

		type TestStruct struct {
			Field1 string
			Field2 int
		}

		testStruct := TestStruct{
			Field1: "test",
			Field2: 123,
		}

		encoded, err := msgpack.Marshal(testStruct)
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		var decoded TestStruct
		err = enc.Decode(encoded, &decoded)
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		if testStruct != decoded {
			t.Errorf("Expected %v, but got %v", testStruct, decoded)
		}
	})

	t.Run("Suite", func(t *testing.T) {
		shelvetest.DecodeTest(t, NewDefault())
	})
}

// Benchmarks

func BenchmarkEncode(b *testing.B) {
	for _, bench := range []struct {
		name   string
		encode func(any) ([]byte, error)
	}{
		{
			name:   "msgpack",
			encode: msgpack.Marshal,
		},
		{
			name: "gob",
			encode: func(v any) ([]byte, error) {
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				err := enc.Encode(v)
				return buf.Bytes(), err
			},
		},
	} {
		var data []byte

		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				data, err = bench.encode(&Product{
					ID:       1,
					Name:     "foo",
					Price:    1.0,
					Quantity: 1,
					Active:   true,
				})
				if err != nil {
					b.Errorf("Expected no error, but got %v", err)
				}
			}
		})

		b.Logf("%s: %d bytes", bench.name, len(data))
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, bench := range []struct {
		name   string
		encode func(any) ([]byte, error)
		decode func([]byte, any) error
	}{
		{
			name:   "msgpack",
			encode: msgpack.Marshal,
			decode: msgpack.Unmarshal,
		},
		{
			name: "gob",
			encode: func(v any) ([]byte, error) {
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				err := enc.Encode(v)
				return buf.Bytes(), err
			},
			decode: func(data []byte, v any) error {
				dec := gob.NewDecoder(bytes.NewReader(data))
				return dec.Decode(v)
			},
		},
	} {
		// Initialize the data
		data, err := bench.encode(&Product{
			ID:       1,
			Name:     "foo",
			Price:    1.0,
			Quantity: 1,
			Active:   true,
		})
		if err != nil {
			b.Errorf("Expected no error, but got %v", err)
		}

		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := bench.decode(data, &Product{})
				if err != nil {
					b.Errorf("Expected no error, but got %v", err)
				}
			}
		})

		b.Logf("%s: %d bytes", bench.name, len(data))
	}
}
