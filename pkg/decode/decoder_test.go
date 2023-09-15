package decode

import (
	"testing"

	"github.com/replicase/pgcapture/pkg/pb"
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
