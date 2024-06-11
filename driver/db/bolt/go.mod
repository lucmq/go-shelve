module go-shelve/driver/db/bolt

go 1.22

require (
	github.com/boltdb/bolt v1.3.1
	github.com/lucmq/go-shelve v0.0.0-00010101000000-000000000000
	github.com/lucmq/go-shelve/driver v0.0.0-00010101000000-000000000000
)

require golang.org/x/sys v0.20.0 // indirect

replace github.com/lucmq/go-shelve => ../../..

replace github.com/lucmq/go-shelve/driver => ../..
