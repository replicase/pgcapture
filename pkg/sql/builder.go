package sql

import (
	"github.com/rueian/pgcapture/pkg/pb"
	"strconv"
	"strings"
)

func DeleteQuery(namespace, table string, fields []*pb.Field) string {
	var query strings.Builder
	query.WriteString("delete from \"")
	query.WriteString(namespace)
	query.WriteString("\".\"")
	query.WriteString(table)
	query.WriteString("\" where \"")

	for i, field := range fields {
		query.WriteString(field.Name)
		query.WriteString("\"=$" + strconv.Itoa(i+1))
		if i != len(fields)-1 {
			query.WriteString(" and \"")
		}
	}
	return query.String()
}

func UpdateQuery(namespace, table string, sets, keys []*pb.Field) string {
	var query strings.Builder
	query.WriteString("update \"")
	query.WriteString(namespace)
	query.WriteString("\".\"")
	query.WriteString(table)
	query.WriteString("\" set \"")

	var j int
	for ; j < len(sets); j++ {
		field := sets[j]
		query.WriteString(field.Name)
		query.WriteString("\"=$" + strconv.Itoa(j+1))
		if j != len(sets)-1 {
			query.WriteString(",\"")
		}
	}

	query.WriteString(" where \"")

	for i := 0; i < len(keys); i++ {
		j = i + j
		field := keys[i]

		query.WriteString(field.Name)
		query.WriteString("\"=$" + strconv.Itoa(j+1))
		if i != len(keys)-1 {
			query.WriteString(" and \"")
		}
	}

	return query.String()
}

func InsertQuery(namespace, table string, fields []*pb.Field) string {
	var query strings.Builder
	query.WriteString("insert into \"")
	query.WriteString(namespace)
	query.WriteString("\".\"")
	query.WriteString(table)
	query.WriteString("\"(\"")
	for i, field := range fields {
		query.WriteString(field.Name)
		if i == len(fields)-1 {
			query.WriteString("\") values (")
		} else {
			query.WriteString("\",\"")
		}
	}
	for i := range fields {
		query.WriteString("$" + strconv.Itoa(i+1))
		if i == len(fields)-1 {
			query.WriteString(")")
		} else {
			query.WriteString(",")
		}
	}
	return query.String()
}
