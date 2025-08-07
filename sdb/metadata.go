package sdb

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/fs"
	"path/filepath"
)

type metadata struct {
	Version      string
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

func (m *metadata) Validate() error {
	if m.Version != version {
		return fmt.Errorf("version mismatch: expected %s, got %s",
			version, m.Version)
	}
	return nil
}

type metadataStore struct {
	fs        fileSystem
	root      string // absolute path to DB root
	writer    *atomicWriter
	marshalFn func(v any) ([]byte, error)
}

func newMetadataStore(fsys fileSystem, root string) *metadataStore {
	return &metadataStore{
		fs:        fsys,
		root:      root,
		writer:    newAtomicWriter(fsys, false),
		marshalFn: gobEncode,
	}
}

func (s *metadataStore) FilePath() string {
	return filepath.Join(s.root, metadataDirectory, metadataFilename)
}

func (s *metadataStore) Load() (metadata, error) {
	var m metadata
	data, err := fs.ReadFile(s.fs, s.FilePath())
	if err != nil {
		return m, fmt.Errorf("read file: %w", err)
	}
	return s.unmarshal(data)
}

func (s *metadataStore) Save(m metadata) error {
	data, err := s.marshal(m)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	return s.writer.WriteFile(s.FilePath(), data, false)
}

func (s *metadataStore) marshal(m metadata) ([]byte, error) {
	return s.marshalFn(m)
}

func gobEncode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(v)
	return buf.Bytes(), err
}

func (*metadataStore) unmarshal(data []byte) (metadata, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	var m metadata
	err := dec.Decode(&m)
	return m, err
}
