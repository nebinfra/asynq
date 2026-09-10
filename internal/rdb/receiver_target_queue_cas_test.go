package rdb

import (
	"context"
	"testing"

	"github.com/nebinfra/asynq/internal/errors"
)

func TestParseReceiverTargetTaskCASResult(t *testing.T) {
	op := errors.Op("rdb.test")
	tests := []struct {
		name     string
		value    interface{}
		want     receiverTargetTaskCASResult
		wantCode errors.Code
	}{
		{
			name:  "commit",
			value: []interface{}{"ok", "2", `{"targetRevision":"2"}`, "0"},
			want: receiverTargetTaskCASResult{
				targetRevision: "2",
				resultPayload:  `{"targetRevision":"2"}`,
			},
		},
		{
			name:  "replay",
			value: []interface{}{"ok", "2", `{"targetRevision":"2"}`, "1"},
			want: receiverTargetTaskCASResult{
				targetRevision: "2",
				resultPayload:  `{"targetRevision":"2"}`,
				replayed:       true,
			},
		},
		{
			name:     "refusal",
			value:    []interface{}{"refused", "source-conflict"},
			wantCode: errors.FailedPrecondition,
		},
		{
			name:     "unknown status",
			value:    []interface{}{"accepted", "2", "payload", "0"},
			wantCode: errors.Internal,
		},
		{
			name:     "invalid replay",
			value:    []interface{}{"ok", "2", "payload", "yes"},
			wantCode: errors.Internal,
		},
		{
			name:     "wrong shape",
			value:    "ok",
			wantCode: errors.Internal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseReceiverTargetTaskCASResult(op, tc.value)
			if tc.wantCode != errors.Unspecified {
				if code := errors.CanonicalCode(err); code != tc.wantCode {
					t.Fatalf("error code = %v, want %v, error = %v", code, tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse result: %v", err)
			}
			if got != tc.want {
				t.Fatalf("result = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestApplyReceiverTargetTaskCASRefusesInvalidFrameWithoutWrites(t *testing.T) {
	r := setup(t)
	defer r.Close()

	keys := [receiverTargetQueueKeyCount]string{}
	for i := range keys {
		keys[i] = "receiver-target-test:" + string(rune('a'+i))
	}
	operands := [receiverTargetQueueOperandCount]string{}
	_, err := r.applyReceiverTargetTaskCAS(context.Background(), "rdb.test", keys, operands)
	if code := errors.CanonicalCode(err); code != errors.FailedPrecondition {
		t.Fatalf("error code = %v, want %v, error = %v", code, errors.FailedPrecondition, err)
	}
	for _, key := range keys {
		if exists := r.client.Exists(context.Background(), key).Val(); exists != 0 {
			t.Fatalf("key %q exists after refusal", key)
		}
	}
}
