package sql

import (
	"testing"

	"github.com/rueian/pgcapture/pkg/pb"
)

func TestInsertQuery(t *testing.T) {
	q := InsertQuery("public", "my_table", []*pb.Field{{Name: "f1"}, {Name: "f2"}, {Name: "f3"}})
	if q != `insert into "public"."my_table"("f1","f2","f3") values ($0,$1,$2)` {
		t.Fatalf("not expected %q", q)
	}
}

func TestDeleteQuery(t *testing.T) {
	q := DeleteQuery("public", "my_table", []*pb.Field{{Name: "f1"}, {Name: "f2"}, {Name: "f3"}})
	if q != `delete from "public"."my_table" where "f1"=$1 and "f2"=$2 and "f3"=$3` {
		t.Fatalf("not expected %q", q)
	}
}

func TestUpdateQuery(t *testing.T) {
	q := UpdateQuery("public", "my_table", []*pb.Field{{Name: "f1"}, {Name: "f2"}}, []*pb.Field{{Name: "f3"}, {Name: "f4"}})
	if q != `update "public"."my_table" set "f1"=$1,"f2"=$2 where "f3"=$3 and "f4"=$4` {
		t.Fatalf("not expected %q", q)
	}
}
