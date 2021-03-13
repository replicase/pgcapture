package main

import (
	"github.com/rueian/pgcapture/example"
)

func main() {
	if err := example.SrcDB.RandomData(example.TestTable); err != nil {
		panic(err)
	}
}
