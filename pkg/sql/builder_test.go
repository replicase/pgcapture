package sql

import (
	"testing"

	"github.com/rueian/pgcapture/pkg/pb"
)

func TestInsertQuery(t *testing.T) {
	q := InsertQuery("public", "my_table", nil, []*pb.Field{{Name: "f1"}, {Name: "f2"}}, 4)
	if q != `insert into "public"."my_table"("f1","f2") values ($1,$2),($3,$4),($5,$6),($7,$8)` {
		t.Fatalf("not expected %q", q)
	}
}

func TestInsertQueryConflick(t *testing.T) {
	q := InsertQuery("public", "my_table", []string{"id", "name"}, []*pb.Field{{Name: "f1"}, {Name: "f2"}}, 4)
	if q != `insert into "public"."my_table"("f1","f2") values ($1,$2),($3,$4),($5,$6),($7,$8) ON CONFLICT (id,name) DO NOTHING` {
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
