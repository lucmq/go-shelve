module go-shelve/driver/db/diskv

go 1.22

require (
	github.com/peterbourgon/diskv/v3 v3.0.1
	github.com/lucmq/go-shelve v0.0.0-00010101000000-000000000000
	github.com/lucmq/go-shelve/driver v0.0.0-00010101000000-000000000000
)

require github.com/google/btree v1.0.0 // indirect

replace github.com/lucmq/go-shelve => ../../..

replace github.com/lucmq/go-shelve/driver => ../..
