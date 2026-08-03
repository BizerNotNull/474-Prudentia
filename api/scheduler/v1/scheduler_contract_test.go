package schedulerv1

import (
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestSchedulerRequestPlaneContract(t *testing.T) {
	file := File_api_scheduler_v1_scheduler_proto
	service := file.Services().ByName("SchedulerService")
	if service == nil {
		t.Fatal("SchedulerService descriptor is missing")
	}
	if service.Methods().Len() != 6 {
		t.Fatalf("SchedulerService methods = %d, want 6", service.Methods().Len())
	}
	wantMethods := []protoreflect.Name{
		"Schedule",
		"PrepareDispatch",
		"AbandonBeforeDispatch",
		"GiveUpBeforeDispatch",
		"Finalize",
		"MarkAmbiguous",
	}
	for i, want := range wantMethods {
		if got := service.Methods().Get(i).Name(); got != want {
			t.Fatalf("method %d = %q, want %q", i, got, want)
		}
	}

	assertFieldNumbers(t, file.Messages().ByName("ScheduleRequest"), map[protoreflect.Name]protoreflect.FieldNumber{
		"request_id": 1, "attempt_id": 2, "tenant_scope": 3,
		"idempotency_lookup_candidates": 4, "lookup_write_version": 5,
		"digest_candidates": 6, "digest_write_version": 7, "model": 8,
		"slot_cost": 9, "execution_budget_ms": 10, "features": 11,
		"priority": 12, "schema_version": 13, "budget_schema_version": 14,
	})
	for _, name := range []protoreflect.Name{
		"ScheduleRequest",
		"PrepareDispatchRequest",
		"AbandonBeforeDispatchRequest",
		"GiveUpBeforeDispatchRequest",
		"FinalizeRequest",
		"MarkAmbiguousRequest",
	} {
		message := file.Messages().ByName(name)
		if message == nil || message.Fields().ByName("schema_version") == nil {
			t.Errorf("%s lacks schema_version", name)
		}
	}

	features := file.Messages().ByName("FeatureSet")
	assertFieldNumbers(t, features, map[protoreflect.Name]protoreflect.FieldNumber{
		"schema_version": 1,
		"bits":           2,
	})
	if got := features.Fields().ByName("bits").Kind(); got != protoreflect.Uint64Kind {
		t.Errorf("FeatureSet.bits kind = %s, want uint64", got)
	}

	terminal := file.Enums().ByName("TerminalProof")
	wantTerminal := map[protoreflect.Name]protoreflect.EnumNumber{
		"TERMINAL_PROOF_UNSPECIFIED":                        0,
		"TERMINAL_PROOF_PROVIDER_FINISH":                    1,
		"TERMINAL_PROOF_NOT_SENT":                           2,
		"TERMINAL_PROOF_COMPLETE_NON_STREAMING":             3,
		"TERMINAL_PROOF_AUTHENTICATED_PROVIDER_TERMINATION": 4,
	}
	for name, number := range wantTerminal {
		value := terminal.Values().ByName(name)
		if value == nil || value.Number() != number {
			t.Errorf("TerminalProof.%s = %v, want %d", name, value, number)
		}
	}

	assertFieldNumbers(t, file.Messages().ByName("ErrorDetail"), map[protoreflect.Name]protoreflect.FieldNumber{
		"schema_version": 1,
		"code":           2,
		"retry_after_ms": 3,
	})
}

func TestSchedulerWireSchemaExcludesSensitivePayloadFields(t *testing.T) {
	forbidden := []string{
		"idempotency_key",
		"prompt",
		"provider_body",
		"kubernetes_object",
	}
	messages := File_api_scheduler_v1_scheduler_proto.Messages()
	for i := range messages.Len() {
		fields := messages.Get(i).Fields()
		for j := range fields.Len() {
			name := string(fields.Get(j).Name())
			for _, fragment := range forbidden {
				if strings.Contains(name, fragment) {
					t.Errorf("sensitive field %s.%s is present", messages.Get(i).Name(), name)
				}
			}
		}
	}
}

func assertFieldNumbers(t *testing.T, message protoreflect.MessageDescriptor, want map[protoreflect.Name]protoreflect.FieldNumber) {
	t.Helper()
	if message == nil {
		t.Fatal("message descriptor is missing")
	}
	got := make(map[protoreflect.Name]protoreflect.FieldNumber, message.Fields().Len())
	for i := range message.Fields().Len() {
		field := message.Fields().Get(i)
		got[field.Name()] = field.Number()
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s fields = %v, want %v", message.Name(), got, want)
	}
}
