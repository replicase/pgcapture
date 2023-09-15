package main

import (
	"github.com/replicase/pgcapture/example"
)

func main() {
	if err := example.SrcDB.RandomData(example.TestTable); err != nil {
		panic(err)
	}
}
