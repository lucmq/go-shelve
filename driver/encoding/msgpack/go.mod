module github.com/lucmq/go-shelve/driver/encoding/msgpack

go 1.22

require (
	github.com/lucmq/go-shelve v0.0.0-00010101000000-000000000000
	github.com/lucmq/go-shelve/driver v0.0.0-00010101000000-000000000000
	github.com/vmihailenco/msgpack/v5 v5.4.1
)

require github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect

replace github.com/lucmq/go-shelve => ../../..

replace github.com/lucmq/go-shelve/driver => ../..
