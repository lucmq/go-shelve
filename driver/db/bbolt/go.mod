module github.com/lucmq/go-shelve/driver/db/bbolt

go 1.22

require (
	github.com/lucmq/go-shelve v0.0.0-00010101000000-000000000000
	github.com/lucmq/go-shelve/driver v0.0.0-00010101000000-000000000000
	go.etcd.io/bbolt v1.3.10
)

require golang.org/x/sys v0.4.0 // indirect

replace github.com/lucmq/go-shelve => ../../..

replace github.com/lucmq/go-shelve/driver => ../..
