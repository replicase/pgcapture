package decode

import (
	"github.com/rueian/pgcapture/pkg/pb"
	"testing"
)

func TestIsDDL(t *testing.T) {
	if !IsDDL(&pb.Change{Schema: ExtensionSchema, Table: ExtensionDDLLogs}) {
		t.Error("unexpected")
	}
	if IsDDL(&pb.Change{Schema: ExtensionSchema, Table: "other"}) {
		t.Error("unexpected")
	}
	if IsDDL(&pb.Change{Schema: "other", Table: ExtensionDDLLogs}) {
		t.Error("unexpected")
	}
}

func TestIgnore(t *testing.T) {
	if !Ignore(&pb.Change{Schema: ExtensionSchema, Table: ExtensionSources}) {
		t.Error("unexpected")
	}
	if Ignore(&pb.Change{Schema: ExtensionSchema, Table: "other"}) {
		t.Error("unexpected")
	}
	if Ignore(&pb.Change{Schema: "other", Table: ExtensionSources}) {
		t.Error("unexpected")
	}
}
