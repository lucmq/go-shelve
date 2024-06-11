package shelve

// MockDB is a mock implementation of the DB interface.
type MockDB struct {
	CloseFunc  func() error
	LenFunc    func() int64
	SyncFunc   func() error
	HasFunc    func(key []byte) (bool, error)
	GetFunc    func(key []byte) ([]byte, error)
	PutFunc    func(key []byte, value []byte) error
	DeleteFunc func(key []byte) error

	ItemsFunc func(
		start []byte,
		order int,
		fn func(key, value []byte) (bool, error),
	) error
}

// Assert that MockDB implements the DB interface.
var _ DB = (*MockDB)(nil)

// Close mocks the Close method of the DB interface.
func (m *MockDB) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

// Len mocks the Len method of the DB interface.
func (m *MockDB) Len() int64 {
	if m.LenFunc != nil {
		return m.LenFunc()
	}
	return 0
}

// Sync mocks the Sync method of the DB interface.
func (m *MockDB) Sync() error {
	if m.SyncFunc != nil {
		return m.SyncFunc()
	}
	return nil
}

// Has mocks the Has method of the DB interface.
func (m *MockDB) Has(key []byte) (bool, error) {
	if m.HasFunc != nil {
		return m.HasFunc(key)
	}
	return false, nil
}

// Get mocks the Get method of the DB interface.
func (m *MockDB) Get(key []byte) ([]byte, error) {
	if m.GetFunc != nil {
		return m.GetFunc(key)
	}
	return nil, nil
}

// Put mocks the Put method of the DB interface.
func (m *MockDB) Put(key, value []byte) error {
	if m.PutFunc != nil {
		return m.PutFunc(key, value)
	}
	return nil
}

// Delete mocks the Delete method of the DB interface.
func (m *MockDB) Delete(key []byte) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(key)
	}
	return nil
}

// Items mocks the Items method of the DB interface.
func (m *MockDB) Items(
	start []byte,
	order int,
	fn func(key, value []byte) (bool, error),
) error {
	if m.ItemsFunc != nil {
		return m.ItemsFunc(start, order, fn)
	}
	return nil
}

// MockCodec is a mock implementation of the Codec interface.
type MockCodec struct {
	EncodeFunc func(value any) ([]byte, error)
	DecodeFunc func(data []byte, value any) error
}

// Assert that MockCodec implements the Codec interface.
var _ Codec = (*MockCodec)(nil)

// Encode mocks the Encode method of the Codec interface.
func (m *MockCodec) Encode(value any) ([]byte, error) {
	if m.EncodeFunc != nil {
		return m.EncodeFunc(value)
	}
	return nil, nil
}

// Decode mocks the Decode method of the Codec interface.
func (m *MockCodec) Decode(data []byte, value any) error {
	if m.DecodeFunc != nil {
		return m.DecodeFunc(data, value)
	}
	return nil
}
