module go-shelve/driver/db/bbolt

go 1.22

require (
	go-shelve v0.0.0-00010101000000-000000000000
	go-shelve/driver v0.0.0-00010101000000-000000000000
	go.etcd.io/bbolt v1.3.10
)

require golang.org/x/sys v0.4.0 // indirect

replace go-shelve => ../../..

replace go-shelve/driver => ../..
