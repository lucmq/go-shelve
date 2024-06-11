module go-shelve/driver/encoding/msgpack

go 1.22

require (
	github.com/vmihailenco/msgpack/v5 v5.4.1
	go-shelve v0.0.0-00010101000000-000000000000
	go-shelve/driver v0.0.0-00010101000000-000000000000
)

require github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect

replace go-shelve => ../../..

replace go-shelve/driver => ../..
