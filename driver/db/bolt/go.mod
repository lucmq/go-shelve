module go-shelve/driver/db/bolt

go 1.22

require (
	github.com/boltdb/bolt v1.3.1
	go-shelve v0.0.0-00010101000000-000000000000
	go-shelve/driver v0.0.0-00010101000000-000000000000
)

require golang.org/x/sys v0.20.0 // indirect

replace go-shelve => ../../..

replace go-shelve/driver => ../..
