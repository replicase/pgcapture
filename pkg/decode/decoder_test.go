package decode

import (
	"github.com/rueian/pgcapture/pkg/pb"
	"testing"
)

func TestIsDDL(t *testing.T) {
	if !IsDDL(&pb.Change{Namespace: ExtensionNamespace, Table: ExtensionDDLLogs}) {
		t.Error("unexpected")
	}
	if IsDDL(&pb.Change{Namespace: ExtensionNamespace, Table: "other"}) {
		t.Error("unexpected")
	}
	if IsDDL(&pb.Change{Namespace: "other", Table: ExtensionDDLLogs}) {
		t.Error("unexpected")
	}
}

func TestIgnore(t *testing.T) {
	if !Ignore(&pb.Change{Namespace: ExtensionNamespace, Table: ExtensionSources}) {
		t.Error("unexpected")
	}
	if Ignore(&pb.Change{Namespace: ExtensionNamespace, Table: "other"}) {
		t.Error("unexpected")
	}
	if Ignore(&pb.Change{Namespace: "other", Table: ExtensionSources}) {
		t.Error("unexpected")
	}
}
