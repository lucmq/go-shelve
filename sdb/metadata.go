package sdb

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
)

type metadata struct {
	Version      uint64
	TotalEntries uint64
	Generation   uint64
	Checkpoint   uint64
}

func makeMetadata() metadata {
	return metadata{
		Version:      version,
		TotalEntries: 0,
		Generation:   0,
		Checkpoint:   0,
	}
}

func (*metadata) FilePath() string {
	return filepath.Join(metadataDirectory, "meta.gob")
}

func (m *metadata) Validate() error {
	if m.Version != version {
		return fmt.Errorf("version mismatch: expected %d, got %d",
			version, m.Version)
	}
	return nil
}

func (m *metadata) Load(path string) error {
	data, err := os.ReadFile(filepath.Join(path, m.FilePath()))
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}
	return m.Decode(data)
}

func (m *metadata) Save(path string) error {
	data, err := m.Encode()
	if err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}
	w := newAtomicWriter(false)
	return w.WriteFile(
		filepath.Join(path, m.FilePath()),
		data,
		false,
	)
}

func (m *metadata) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(m)
	if err != nil {
		return nil, fmt.Errorf("encode gob: %w", err)
	}
	return buf.Bytes(), nil
}

func (m *metadata) Decode(data []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	return dec.Decode(m)
}
