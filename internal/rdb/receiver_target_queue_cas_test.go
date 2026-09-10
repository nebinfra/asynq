package rdb

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nebinfra/asynq/internal/base"
	"github.com/nebinfra/asynq/internal/errors"
	"github.com/nebinfra/asynq/internal/timeutil"
	"github.com/redis/go-redis/v9"
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

func TestEnqueueReceiverTargetUsesGeneratedTransaction(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Unix(1725148800, 123)
	r.SetClock(timeutil.NewSimulatedClock(now))
	digest := strings.Repeat("d", 64)
	input := base.ReceiverTargetQueueInitial{
		RuntimeEpochRevision: "7", StateEpoch: "9", CatalogGeneration: digest,
		InstanceTenant: "nebcore", EffectID: "00000000-0000-4000-8000-000000000001:1:queue_wakeup:0",
		TaskDigest: strings.Repeat("b", 64), ProcessAt: now,
	}
	input.SourceIDDigest = receiverTargetSHA256(`{"effectId":"` + input.EffectID + `","handlerDigest":"` + input.TaskDigest + `","instanceTenant":"` + input.InstanceTenant + `","payloadDigest":"` + strings.Repeat("e", 64) + `","schemaVersion":"queue-wakeup.source-id.v1","sourceRevision":"1","stateEpoch":"` + input.StateEpoch + `"}`)
	msg := &base.TaskMessage{ID: "advance:task", Type: "nebpilot:advance", Payload: []byte(`{"effectId":"fixture"}`), Queue: base.DefaultQueueName, Retry: 3}
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	keys, _, err := r.receiverTargetInitialCommand(t.Context(), msg, encoded, input)
	if err != nil {
		t.Fatal(err)
	}
	seedReceiverTargetCommon(t, r, keys, input)
	if err := r.EnqueueReceiverTarget(t.Context(), msg, input); err != nil {
		t.Fatalf("marked enqueue: %v", err)
	}
	fields, err := r.client.HGetAll(t.Context(), keys[10]).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 7 || fields["state"] != "pending" || fields["sourceIdDigest"] != input.SourceIDDigest || fields["taskDigest"] != input.TaskDigest {
		t.Fatalf("marked task fields = %#v", fields)
	}
	if score, err := r.client.ZScore(t.Context(), keys[11], msg.ID).Result(); err != nil || score != 0 {
		t.Fatalf("pending score = %v, %v", score, err)
	}
	if count := r.client.HLen(t.Context(), keys[9]).Val(); count != 12 {
		t.Fatalf("source field count = %d, want 12", count)
	}
	if count := r.client.HLen(t.Context(), keys[5]).Val(); count != 0 {
		t.Fatalf("receipt field count = %d, want 0 after finalize", count)
	}
	if count := r.client.HLen(t.Context(), keys[6]).Val(); count != 0 {
		t.Fatalf("envelope field count = %d, want 0 after finalize", count)
	}
	if digest := r.client.HGet(t.Context(), keys[9], "ackReceiptDigest").Val(); digest == "" {
		t.Fatal("finalized source has no acknowledgement receipt")
	}
	if err := r.EnqueueReceiverTarget(t.Context(), msg, input); err != nil {
		t.Fatalf("marked enqueue replay: %v", err)
	}
}

func TestEnqueueReceiverTargetTaskHashBoundary(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Unix(1725148800, 123)
	r.SetClock(timeutil.NewSimulatedClock(now))
	input := receiverTargetInitialFixture(now)

	tooLarge := receiverTargetMessageWithTaskHashSize(t, input, now, receiverTargetMaximumTaskHashBytes+1)
	encoded, err := base.EncodeMessage(tooLarge)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.EnqueueReceiverTarget(t.Context(), tooLarge, input); errors.CanonicalCode(err) != errors.FailedPrecondition {
		t.Fatalf("maximum plus one error = %v, want FailedPrecondition", err)
	}
	if size := r.client.DBSize(t.Context()).Val(); size != 0 {
		t.Fatalf("database key count after maximum plus one refusal = %d, want 0", size)
	}

	maximum := receiverTargetMessageWithTaskHashSize(t, input, now, receiverTargetMaximumTaskHashBytes)
	encoded, err = base.EncodeMessage(maximum)
	if err != nil {
		t.Fatal(err)
	}
	keys, _, err := r.receiverTargetInitialCommand(t.Context(), maximum, encoded, input)
	if err != nil {
		t.Fatalf("maximum task hash: %v", err)
	}
	seedReceiverTargetCommon(t, r, keys, input)
	if err := r.EnqueueReceiverTarget(t.Context(), maximum, input); err != nil {
		t.Fatalf("enqueue maximum task hash: %v", err)
	}
	fields := r.client.HGetAll(t.Context(), keys[10]).Val()
	if got := receiverTargetMapHashBytes(fields); got != receiverTargetMaximumTaskHashBytes {
		t.Fatalf("stored task hash bytes = %d, want %d", got, receiverTargetMaximumTaskHashBytes)
	}
}

func TestArchiveRefusesMarkedTaskWithoutWrites(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := t.Context()
	now := time.Unix(1725148800, 123)
	r.SetClock(timeutil.NewSimulatedClock(now))
	msg := &base.TaskMessage{ID: "advance:task", Type: "nebpilot:advance", Payload: []byte(`{"effectId":"fixture"}`), Queue: base.DefaultQueueName, Retry: 3}
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	taskKey := base.TaskKey(msg.Queue, msg.ID)
	fields := map[string]any{
		"msg":               string(encoded),
		"state":             "active",
		"sourceIdDigest":    strings.Repeat("a", 64),
		"stateEpoch":        "9",
		"enqueueGeneration": "1",
		"taskDigest":        strings.Repeat("b", 64),
	}
	if err := r.client.HSet(ctx, taskKey, fields).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.ZAdd(ctx, base.ActiveKey(msg.Queue), redis.Z{Score: 0, Member: msg.ID}).Err(); err != nil {
		t.Fatal(err)
	}
	lease := now.Add(LeaseDuration).Unix()
	if err := r.client.ZAdd(ctx, base.LeaseKey(msg.Queue), redis.Z{Score: float64(lease), Member: msg.ID}).Err(); err != nil {
		t.Fatal(err)
	}

	if err := r.Archive(ctx, msg, "failed"); err == nil || !strings.Contains(err.Error(), "RECEIVER TARGET ARCHIVE UNSUPPORTED") {
		t.Fatalf("marked archive error = %v, want unsupported refusal", err)
	}
	if got := r.client.HGetAll(ctx, taskKey).Val(); !reflect.DeepEqual(got, mapStringAnyToString(fields)) {
		t.Fatalf("marked task changed after archive refusal: %#v", got)
	}
	if score := r.client.ZScore(ctx, base.ActiveKey(msg.Queue), msg.ID).Val(); score != 0 {
		t.Fatalf("active score = %v, want 0", score)
	}
	if score := r.client.ZScore(ctx, base.LeaseKey(msg.Queue), msg.ID).Val(); score != float64(lease) {
		t.Fatalf("lease score = %v, want %d", score, lease)
	}
	if count := r.client.ZCard(ctx, base.ArchivedKey(msg.Queue)).Val(); count != 0 {
		t.Fatalf("archived count = %d, want 0", count)
	}
}

func mapStringAnyToString(input map[string]any) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value.(string)
	}
	return output
}

func receiverTargetInitialFixture(now time.Time) base.ReceiverTargetQueueInitial {
	digest := strings.Repeat("d", 64)
	input := base.ReceiverTargetQueueInitial{
		RuntimeEpochRevision: "7", StateEpoch: "9", CatalogGeneration: digest,
		InstanceTenant: "nebcore", EffectID: "00000000-0000-4000-8000-000000000001:1:queue_wakeup:0",
		TaskDigest: strings.Repeat("b", 64), ProcessAt: now,
	}
	input.SourceIDDigest = receiverTargetSHA256(`{"effectId":"` + input.EffectID + `","handlerDigest":"` + input.TaskDigest + `","instanceTenant":"` + input.InstanceTenant + `","payloadDigest":"` + strings.Repeat("e", 64) + `","schemaVersion":"queue-wakeup.source-id.v1","sourceRevision":"1","stateEpoch":"` + input.StateEpoch + `"}`)
	return input
}

func receiverTargetMessageWithTaskHashSize(t *testing.T, input base.ReceiverTargetQueueInitial, now time.Time, want int64) *base.TaskMessage {
	t.Helper()
	for low, high := 0, int(want); low <= high; {
		payloadSize := low + (high-low)/2
		msg := &base.TaskMessage{ID: "advance:task", Type: "nebpilot:advance", Payload: make([]byte, payloadSize), Queue: base.DefaultQueueName, Retry: 3}
		encoded, err := base.EncodeMessage(msg)
		if err != nil {
			t.Fatal(err)
		}
		fields := []string{"msg", "state", "sourceIdDigest", "stateEpoch", "enqueueGeneration", "taskDigest", "pending_since"}
		values := []string{string(encoded), "pending", input.SourceIDDigest, input.StateEpoch, "1", input.TaskDigest, strconv.FormatInt(now.UnixNano(), 10)}
		size := receiverTargetHashBytes(fields, values)
		if size == want {
			return msg
		}
		if size < want {
			low = payloadSize + 1
		} else {
			high = payloadSize - 1
		}
	}
	t.Fatalf("could not construct task hash with %d bytes", want)
	return nil
}

func receiverTargetMapHashBytes(fields map[string]string) int64 {
	var total int64
	for field, value := range fields {
		total += int64(len(field) + len(value))
	}
	return total
}

func seedReceiverTargetCommon(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string, input base.ReceiverTargetQueueInitial) {
	t.Helper()
	if err := r.client.HSet(t.Context(), keys[0], map[string]any{"instanceTenant": input.InstanceTenant, "revision": input.RuntimeEpochRevision, "stateEpoch": input.StateEpoch, "catalogGeneration": input.CatalogGeneration, "admissionState": "open"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.HSet(t.Context(), keys[1], map[string]any{
		"agentStats":    "9f909ec563ed2c8673a69dd4a701d2f5c0875906dd9b62703f35cb2f9b498a12",
		"earnedHistory": "4c38b66f690987523f686503d638c4d7f61bd3f2dbccb9096326fe32a89e1727",
		"routinePause":  "1863d6677b240314065ea2b4d14655eaae5bf292ae069421d69ebeb16b72f571",
		"queueWakeup":   "84e40baf8682c07ef783bda93b9fdddf6dc2274db66e590b9bad0dbd5b499cc4",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.Set(t.Context(), keys[2], "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.HSet(t.Context(), keys[3], "descriptorDigest", strings.Repeat("c", 64)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.HSet(t.Context(), keys[4], receiverTargetCapacityFixture()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.HSet(t.Context(), keys[7], map[string]any{
		"schemaVersion": "queue-effect.v1", "stateEpoch": input.StateEpoch, "effectId": input.EffectID,
		"tenant": input.InstanceTenant, "runId": "00000000-0000-4000-8000-000000000001", "owner": "fixture",
		"kind": "queue_wakeup", "correlationId": "fixture", "payloadSchema": "fixture.v1", "payload": "{}",
		"payloadDigest": strings.Repeat("e", 64), "nextEligibleAtRedisMillis": "1", "attemptClass": "initial", "sourceRevision": "1",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.HSet(t.Context(), keys[8], map[string]any{"state": "reserved", "sourceRevision": "1", "kind": "queue_wakeup", "handlerDigest": input.TaskDigest, "payloadDigest": strings.Repeat("e", 64), "receiptRevision": "0"}).Err(); err != nil {
		t.Fatal(err)
	}
}

func receiverTargetCapacityFixture() map[string]any {
	return map[string]any{
		"total.rows": "0", "total.bytes": "0", "total.reservedBytes": "0", "admitted.rows": "400717", "admitted.bytes": "481734986", "admitted.reservedBytes": "0",
		"envelope.declaredCeilingBytes": "601882624", "envelope.predecessorMeasurementProofDigest": "sha256:3d0e3b7f42bd36a9b4254aa6979a5b2d4c9fb60629a588546264c0a3fe3eb9d6", "kernel.reservedBytes": "1085", "kernel.proofDigest": "sha256:eb7ac47fa2dc0fe0e78a4607d135fa799b1cdb702c4b7b7c77b853aee84047e2",
		"agentStats.rows": "0", "agentStats.bytes": "0", "agentStats.reservedBytes": "0", "agentStats.limitRows": "88320", "agentStats.limitBytes": "77008632", "agentStats.limitReservedBytes": "0",
		"earnedHistory.rows": "0", "earnedHistory.bytes": "0", "earnedHistory.reservedBytes": "0", "earnedHistory.limitRows": "188481", "earnedHistory.limitBytes": "24523320", "earnedHistory.limitReservedBytes": "0",
		"routinePause.rows": "0", "routinePause.bytes": "0", "routinePause.reservedBytes": "0", "routinePause.limitRows": "62476", "routinePause.limitBytes": "26763034", "routinePause.limitReservedBytes": "0",
		"queueWakeup.rows": "0", "queueWakeup.bytes": "0", "queueWakeup.reservedBytes": "0", "queueWakeup.limitRows": "61440", "queueWakeup.limitBytes": "353440000", "queueWakeup.limitReservedBytes": "0",
	}
}
