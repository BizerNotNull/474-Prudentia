package adminv1

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestUnsafeCapacityDebtOverrideContract(t *testing.T) {
	file := File_api_admin_v1_capacity_debt_admin_proto
	service := file.Services().ByName("CapacityDebtAdminService")
	if service == nil {
		t.Fatal("CapacityDebtAdminService descriptor is missing")
	}
	if service.Methods().Len() != 1 {
		t.Fatalf("CapacityDebtAdminService method count = %d, want 1", service.Methods().Len())
	}
	method := service.Methods().Get(0)
	if method.Name() != "UnsafeOverrideCapacityDebt" {
		t.Fatalf("admin method = %q, want UnsafeOverrideCapacityDebt", method.Name())
	}
	if got := method.Output().FullName(); got != "google.protobuf.Empty" {
		t.Fatalf("admin response = %q, want google.protobuf.Empty", got)
	}

	request := file.Messages().ByName("UnsafeOverrideCapacityDebtRequest")
	want := map[protoreflect.Name]protoreflect.FieldNumber{
		"debt_id":                 1,
		"expected_pod_uid":        2,
		"expected_endpoint_epoch": 3,
		"confirmation":            4,
		"ticket":                  5,
		"reason":                  6,
		"schema_version":          7,
	}
	got := make(map[protoreflect.Name]protoreflect.FieldNumber, request.Fields().Len())
	for i := range request.Fields().Len() {
		field := request.Fields().Get(i)
		got[field.Name()] = field.Number()
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("admin request fields = %v, want %v", got, want)
	}
	for _, forbidden := range []protoreflect.Name{"actor", "principal", "unsafe"} {
		if request.Fields().ByName(forbidden) != nil {
			t.Errorf("admin request self-asserts forbidden field %q", forbidden)
		}
	}
	if request.Fields().ByName("confirmation").Kind() != protoreflect.StringKind {
		t.Error("confirmation must be the exact danger phrase, not a boolean")
	}
}
