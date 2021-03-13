package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/rueian/pgcapture/example"
)

func main() {
	req, _ := http.NewRequest(http.MethodDelete, "http://localhost:8080/admin/v2/persistent/public/default/"+example.TestDBSrc, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
	} else {
		bs, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode > 299 {
			fmt.Println(string(bs))
		}
	}

	if err := example.DefaultDB.Exec(fmt.Sprintf("select pg_drop_replication_slot('%s')", example.TestDBSrc)); err != nil {
		fmt.Println(err)
	}
	if err := example.DefaultDB.Exec("drop database " + example.TestDBSrc); err != nil {
		fmt.Println(err)
	}
	if err := example.DefaultDB.Exec("drop database " + example.TestDBSink); err != nil {
		fmt.Println(err)
	}
}
