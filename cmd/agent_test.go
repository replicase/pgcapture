package main

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

func TestExtract_EmptyParams(t *testing.T) {
	actual, err := extract(&structpb.Struct{}, "a", "b", "c")
	if actual != nil {
		t.Errorf("actual: %v, want: nil", actual)
	}
	if err.Error() != "a key is required in parameters" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtract_MissingRequiredParams(t *testing.T) {
	params, err := structpb.NewStruct(map[string]interface{}{
		"a": "foo",
		"b": "bar",
	})
	if err != nil {
		t.Fatal(err)
	}

	actual, err := extract(params, "a", "b", "c")
	if actual != nil {
		t.Errorf("actual: %v, want: nil", actual)
	}
	if err.Error() != "c key is required in parameters" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtract_WithRequiredParams(t *testing.T) {
	params, err := structpb.NewStruct(map[string]interface{}{
		"a": "foo",
		"b": 123,
		"c": true,
	})
	if err != nil {
		t.Fatal(err)
	}

	actual, err := extract(params, "a", "b", "c")
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string]*structpb.Value{"a": structpb.NewStringValue("foo"), "b": structpb.NewNumberValue(123), "c": structpb.NewBoolValue(true)}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("actual: %v, want: %v", actual, expected)
	}
}

func TestExtract_MissingOptionalParams(t *testing.T) {
	params, err := structpb.NewStruct(map[string]interface{}{
		"a": "foo",
		"b": "bar",
		"c": "",
	})
	if err != nil {
		t.Fatal(err)
	}

	actual, err := extract(params, "a", "b", "?c", "?d")
	if err != nil {
		t.Fatal(err)
	}

	// c is zero value, but it's optional, so it's value is an empty string
	// d is not present, but it's optional, so it's value is nil
	expected := map[string]*structpb.Value{"a": structpb.NewStringValue("foo"), "b": structpb.NewStringValue("bar"), "c": structpb.NewStringValue(""), "d": nil}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("actual: %v, want: %v", actual, expected)
	}

}
