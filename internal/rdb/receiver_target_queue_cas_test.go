package rdb

import (
	"context"
	"reflect"
	"sort"
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
	msg := &base.TaskMessage{ID: "advance:task", Type: "nebpilot:advance", Payload: []byte(`{"effectId":"00000000-0000-4000-8000-000000000001:1:queue_wakeup:0","schemaVersion":"nebpilot.queue-effect-task.v1","sourceRevision":"1","taskPayload":{},"taskPayloadDigest":"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"}`), Queue: base.DefaultQueueName, Retry: 3}
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
	beforeReplay := receiverTargetKeySnapshot(t, r, keys)
	if err := r.EnqueueReceiverTarget(t.Context(), msg, input); err != nil {
		t.Fatalf("marked enqueue replay: %v", err)
	}
	if afterReplay := receiverTargetKeySnapshot(t, r, keys); !reflect.DeepEqual(afterReplay, beforeReplay) {
		t.Fatal("marked enqueue replay changed receiver transaction state")
	}
	dequeued, _, err := r.Dequeue(base.DefaultQueueName)
	if err != nil || dequeued.ID != msg.ID {
		t.Fatalf("marked dequeue = %v, %v", dequeued, err)
	}
	if _, err := r.ExtendLease(base.DefaultQueueName, dequeued.ID); err != nil {
		t.Fatalf("marked lease extension: %v", err)
	}
	if err := r.Requeue(t.Context(), dequeued); err != nil {
		t.Fatalf("marked requeue: %v", err)
	}
	dequeued, _, err = r.Dequeue(base.DefaultQueueName)
	if err != nil || dequeued.ID != msg.ID {
		t.Fatalf("marked dequeue after requeue = %v, %v", dequeued, err)
	}
	due := now.Add(time.Minute)
	if err := r.Retry(t.Context(), dequeued, due, "retry", true); err != nil {
		t.Fatalf("marked retry: %v", err)
	}
	r.SetClock(timeutil.NewSimulatedClock(due.Add(time.Second)))
	if err := r.ForwardIfReady(base.DefaultQueueName); err != nil {
		t.Fatalf("marked due forward: %v", err)
	}
	dequeued, _, err = r.Dequeue(base.DefaultQueueName)
	if err != nil || dequeued.ID != msg.ID {
		t.Fatalf("marked dequeue after retry = %v, %v", dequeued, err)
	}
	if err := r.Done(t.Context(), dequeued); err != nil {
		t.Fatalf("marked done: %v", err)
	}
	if exists := r.client.Exists(t.Context(), keys[10]).Val(); exists != 0 {
		t.Fatalf("marked task exists after done: %d", exists)
	}
	beforeRevision := r.client.HGet(t.Context(), keys[9], "targetRevision").Val()
	if err := r.EnqueueReceiverTarget(t.Context(), msg, input); err != nil {
		t.Fatalf("marked recovery enqueue: %v", err)
	}
	recovered := r.client.HGetAll(t.Context(), keys[10]).Val()
	if recovered["state"] != "pending" || recovered["enqueueGeneration"] != "2" {
		t.Fatalf("recovered task fields = %#v", recovered)
	}
	afterRevision := r.client.HGet(t.Context(), keys[9], "targetRevision").Val()
	wantRevision, err := receiverTargetIncrementUint(beforeRevision)
	if err != nil {
		t.Fatal(err)
	}
	if afterRevision != wantRevision {
		t.Fatalf("recovery target revision = %q, want %q", afterRevision, wantRevision)
	}
	if generation := r.client.HGet(t.Context(), keys[9], "enqueueGeneration").Val(); generation != "2" {
		t.Fatalf("recovery source generation = %q, want 2", generation)
	}
	if score, err := r.client.ZScore(t.Context(), keys[11], msg.ID).Result(); err != nil || score != 0 {
		t.Fatalf("recovered pending score = %v, %v", score, err)
	}
}

func TestReleaseReceiverTargetRequiresCommittedInboxAndAbsence(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Unix(1725148800, 123)
	r.SetClock(timeutil.NewSimulatedClock(now))
	input := receiverTargetInitialFixture(now)
	msg := receiverTargetQueueEffectMessage()
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
		t.Fatal(err)
	}
	beforeTask := r.client.HGetAll(t.Context(), keys[10]).Val()
	beforeSource := r.client.HGetAll(t.Context(), keys[9]).Val()
	release := base.ReceiverTargetQueueRelease{
		RuntimeEpochRevision: input.RuntimeEpochRevision, StateEpoch: input.StateEpoch,
		CatalogGeneration: input.CatalogGeneration, InstanceTenant: input.InstanceTenant,
		EffectID: input.EffectID, SourceIDDigest: input.SourceIDDigest, TaskDigest: input.TaskDigest,
	}
	if err := r.ReleaseReceiverTarget(t.Context(), msg.ID, release); errors.CanonicalCode(err) != errors.AlreadyExists {
		t.Fatalf("release with present task error = %v, want AlreadyExists", err)
	}
	if got := r.client.HGetAll(t.Context(), keys[10]).Val(); !reflect.DeepEqual(got, beforeTask) {
		t.Fatalf("task changed after refused release: got %#v, want %#v", got, beforeTask)
	}
	if got := r.client.HGetAll(t.Context(), keys[9]).Val(); !reflect.DeepEqual(got, beforeSource) {
		t.Fatalf("source changed after refused release: got %#v, want %#v", got, beforeSource)
	}

	dequeued, _, err := r.Dequeue(base.DefaultQueueName)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Done(t.Context(), dequeued); err != nil {
		t.Fatal(err)
	}
	if err := r.client.HSet(t.Context(), keys[8], "state", "committed", "receiptRevision", "1").Err(); err != nil {
		t.Fatal(err)
	}
	releaseKeys, _, err := r.receiverTargetReleaseCommand(t.Context(), msg.ID, release)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := releaseKeys[20], base.ProcessedKey(base.DefaultQueueName, now); got != want {
		t.Fatalf("release processed day key = %q, want %q", got, want)
	}
	if got, want := releaseKeys[22], base.FailedKey(base.DefaultQueueName, now); got != want {
		t.Fatalf("release failed day key = %q, want %q", got, want)
	}
	if err := r.ReleaseReceiverTarget(t.Context(), msg.ID, release); err != nil {
		t.Fatalf("release absent marked task: %v", err)
	}
	if fence := r.client.HGet(t.Context(), keys[9], "releaseFence").Val(); fence != "closed" {
		t.Fatalf("release fence = %q, want closed", fence)
	}
	if count := r.client.HLen(t.Context(), keys[9]).Val(); count != 14 {
		t.Fatalf("released source field count = %d, want 14", count)
	}
	beforeReplay := receiverTargetKeySnapshot(t, r, keys)
	if err := r.ReleaseReceiverTarget(t.Context(), msg.ID, release); err != nil {
		t.Fatalf("replay released source: %v", err)
	}
	if afterReplay := receiverTargetKeySnapshot(t, r, keys); !reflect.DeepEqual(afterReplay, beforeReplay) {
		t.Fatal("release replay changed receiver transaction state")
	}
	if fence := r.client.HGet(t.Context(), keys[9], "releaseFence").Val(); fence != "closed" {
		t.Fatalf("replayed release fence = %q, want closed", fence)
	}
	if exists := r.client.Exists(t.Context(), keys[10]).Val(); exists != 0 {
		t.Fatalf("released task exists: %d", exists)
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

func TestEnqueueReceiverTargetRefusesUnsupportedNativeFields(t *testing.T) {
	now := time.Unix(1725148800, 123)
	for _, mutate := range []func(*base.TaskMessage){
		func(msg *base.TaskMessage) { msg.UniqueKey = "unique" },
		func(msg *base.TaskMessage) { msg.GroupKey = "group" },
		func(msg *base.TaskMessage) { msg.Retention = 1 },
	} {
		r := setup(t)
		input := receiverTargetInitialFixture(now)
		msg := receiverTargetQueueEffectMessage()
		mutate(msg)
		if err := r.EnqueueReceiverTarget(t.Context(), msg, input); errors.CanonicalCode(err) != errors.FailedPrecondition {
			t.Fatalf("unsupported native fields error = %v, want FailedPrecondition", err)
		}
		if size := r.client.DBSize(t.Context()).Val(); size != 0 {
			t.Fatalf("database key count after refusal = %d, want 0", size)
		}
		r.Close()
	}
}

func TestDequeueReceiverTargetRefusesCorruptEnvelopeWithoutWrites(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Unix(1725148800, 123)
	r.SetClock(timeutil.NewSimulatedClock(now))
	input := receiverTargetInitialFixture(now)
	msg := receiverTargetQueueEffectMessage()
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
		t.Fatal(err)
	}
	msg.Payload = []byte(`{"effectId":"wrong","schemaVersion":"nebpilot.queue-effect-task.v1","sourceRevision":"1","taskPayload":{},"taskPayloadDigest":"` + strings.Repeat("0", 64) + `"}`)
	corrupt, err := base.EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.client.HSet(t.Context(), keys[10], "msg", corrupt).Err(); err != nil {
		t.Fatal(err)
	}
	if got, _, err := r.Dequeue(base.DefaultQueueName); errors.CanonicalCode(err) != errors.FailedPrecondition || got != nil {
		t.Fatalf("corrupt marked dequeue = %v, %v, want FailedPrecondition", got, err)
	}
	if state := r.client.HGet(t.Context(), keys[10], "state").Val(); state != "pending" {
		t.Fatalf("task state after refusal = %q, want pending", state)
	}
	if score, err := r.client.ZScore(t.Context(), keys[11], msg.ID).Result(); err != nil || score != 0 {
		t.Fatalf("pending membership after refusal = %v, %v", score, err)
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

func TestInspectorRefusesMarkedTaskWithoutWrites(t *testing.T) {
	tests := []struct {
		name string
		run  func(*RDB) error
	}{
		{name: "run task", run: func(r *RDB) error { return r.RunTask(base.DefaultQueueName, "advance:task") }},
		{name: "archive task", run: func(r *RDB) error { return r.ArchiveTask(base.DefaultQueueName, "advance:task") }},
		{name: "update payload", run: func(r *RDB) error { return r.UpdateTaskPayload(base.DefaultQueueName, "advance:task", []byte(`{}`)) }},
		{name: "delete task", run: func(r *RDB) error { return r.DeleteTask(base.DefaultQueueName, "advance:task") }},
		{name: "remove queue force", run: func(r *RDB) error { return r.RemoveQueue(base.DefaultQueueName, true) }},
		{name: "write result", run: func(r *RDB) error {
			_, err := r.WriteResult(base.DefaultQueueName, "advance:task", []byte("result"))
			return err
		}},
		{name: "run all", run: func(r *RDB) error { _, err := r.RunAllScheduledTasks(base.DefaultQueueName); return err }},
		{name: "archive all", run: func(r *RDB) error { _, err := r.ArchiveAllScheduledTasks(base.DefaultQueueName); return err }},
		{name: "delete all", run: func(r *RDB) error { _, err := r.DeleteAllScheduledTasks(base.DefaultQueueName); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := setup(t)
			defer r.Close()
			ctx := t.Context()
			msg := receiverTargetQueueEffectMessage()
			encoded, err := base.EncodeMessage(msg)
			if err != nil {
				t.Fatal(err)
			}
			taskKey := base.TaskKey(msg.Queue, msg.ID)
			fields := map[string]any{
				"msg": string(encoded), "state": "scheduled", "sourceIdDigest": strings.Repeat("a", 64),
				"stateEpoch": "9", "enqueueGeneration": "1", "taskDigest": strings.Repeat("b", 64),
			}
			if err := r.client.HSet(ctx, taskKey, fields).Err(); err != nil {
				t.Fatal(err)
			}
			if err := r.client.ZAdd(ctx, base.ScheduledKey(msg.Queue), redis.Z{Score: 123, Member: msg.ID}).Err(); err != nil {
				t.Fatal(err)
			}
			if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
				t.Fatal(err)
			}
			beforeFields := r.client.HGetAll(ctx, taskKey).Val()
			if err := test.run(r); err == nil || !strings.Contains(err.Error(), "RECEIVER TARGET") {
				t.Fatalf("marked Inspector mutation error = %v, want receiver target refusal", err)
			}
			if afterFields := r.client.HGetAll(ctx, taskKey).Val(); !reflect.DeepEqual(afterFields, beforeFields) {
				t.Fatalf("marked task changed after refusal: %#v", afterFields)
			}
			if score, err := r.client.ZScore(ctx, base.ScheduledKey(msg.Queue), msg.ID).Result(); err != nil || score != 123 {
				t.Fatalf("scheduled membership after refusal = %v, %v", score, err)
			}
		})
	}
}

func TestRemoveQueueForceRefusesMarkedTaskWithoutWrites(t *testing.T) {
	for _, state := range []string{"pending", "scheduled", "retry", "archived"} {
		t.Run(state, func(t *testing.T) {
			r := setup(t)
			defer r.Close()
			ctx := t.Context()
			const markedID = "advance:marked"
			const ordinaryID = "ordinary"
			markedKey := base.TaskKey(base.DefaultQueueName, markedID)
			ordinaryKey := base.TaskKey(base.DefaultQueueName, ordinaryID)
			if err := r.client.HSet(ctx, markedKey, "state", state, "sourceIdDigest", strings.Repeat("a", 64)).Err(); err != nil {
				t.Fatal(err)
			}
			if err := r.client.HSet(ctx, ordinaryKey, "state", state).Err(); err != nil {
				t.Fatal(err)
			}
			stateKey := map[string]string{
				"pending": base.PendingKey(base.DefaultQueueName), "scheduled": base.ScheduledKey(base.DefaultQueueName),
				"retry": base.RetryKey(base.DefaultQueueName), "archived": base.ArchivedKey(base.DefaultQueueName),
			}[state]
			if err := r.client.ZAdd(ctx, stateKey,
				redis.Z{Score: 1, Member: markedID}, redis.Z{Score: 2, Member: ordinaryID}).Err(); err != nil {
				t.Fatal(err)
			}
			if err := r.client.SAdd(ctx, base.AllQueues, base.DefaultQueueName).Err(); err != nil {
				t.Fatal(err)
			}
			keys := []string{
				markedKey, ordinaryKey, base.PendingKey(base.DefaultQueueName), base.ActiveKey(base.DefaultQueueName),
				base.ScheduledKey(base.DefaultQueueName), base.RetryKey(base.DefaultQueueName),
				base.ArchivedKey(base.DefaultQueueName), base.LeaseKey(base.DefaultQueueName), base.AllQueues,
			}
			before := make(map[string]string, len(keys))
			for _, key := range keys {
				before[key] = r.client.Dump(ctx, key).Val()
			}
			if err := r.RemoveQueue(base.DefaultQueueName, true); err == nil || !strings.Contains(err.Error(), "RECEIVER TARGET QUEUE REMOVAL UNSUPPORTED") {
				t.Fatalf("force removal error = %v, want receiver target refusal", err)
			}
			for _, key := range keys {
				if after := r.client.Dump(ctx, key).Val(); after != before[key] {
					t.Errorf("key %q changed after refusal", key)
				}
			}
		})
	}
}

func TestWriteResultRefusalPreservesRedisErrorClassification(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := t.Context()
	taskKey := base.TaskKey(base.DefaultQueueName, "advance:task")
	if err := r.client.HSet(ctx, taskKey, "sourceIdDigest", strings.Repeat("a", 64)).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := r.WriteResult(base.DefaultQueueName, "advance:task", []byte("result"))
	if !errors.IsRedisCommandError(err) {
		t.Fatalf("WriteResult error = %v, want RedisCommandError classification", err)
	}
}

func TestDeleteExpiredCompletedTasksRefusesMarkedTaskWithoutWrites(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := t.Context()
	now := time.Unix(1700000000, 0)
	r.SetClock(timeutil.NewSimulatedClock(now))
	const markedID = "advance:marked"
	const ordinaryID = "ordinary"
	markedKey := base.TaskKey(base.DefaultQueueName, markedID)
	ordinaryKey := base.TaskKey(base.DefaultQueueName, ordinaryID)
	if err := r.client.HSet(ctx, markedKey, "state", "completed", "sourceIdDigest", strings.Repeat("a", 64)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.HSet(ctx, ordinaryKey, "state", "completed").Err(); err != nil {
		t.Fatal(err)
	}
	completedKey := base.CompletedKey(base.DefaultQueueName)
	if err := r.client.ZAdd(ctx, completedKey,
		redis.Z{Score: float64(now.Unix() - 2), Member: markedID},
		redis.Z{Score: float64(now.Unix() - 1), Member: ordinaryID}).Err(); err != nil {
		t.Fatal(err)
	}
	keys := []string{markedKey, ordinaryKey, completedKey}
	before := make(map[string]string, len(keys))
	for _, key := range keys {
		before[key] = r.client.Dump(ctx, key).Val()
	}
	if err := r.DeleteExpiredCompletedTasks(base.DefaultQueueName, 100); err == nil || !strings.Contains(err.Error(), "RECEIVER TARGET COMPLETED CLEANUP UNSUPPORTED") {
		t.Fatalf("completed cleanup error = %v, want receiver target refusal", err)
	}
	for _, key := range keys {
		if after := r.client.Dump(ctx, key).Val(); after != before[key] {
			t.Errorf("key %q changed after refusal", key)
		}
	}
}

func TestMarkAsCompleteRefusesMarkedTaskWithoutWrites(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := t.Context()
	now := time.Unix(1700000000, 0)
	r.SetClock(timeutil.NewSimulatedClock(now))
	msg := receiverTargetQueueEffectMessage()
	msg.Retention = 3600
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	taskKey := base.TaskKey(msg.Queue, msg.ID)
	if err := r.client.HSet(ctx, taskKey, "msg", encoded, "state", "active", "sourceIdDigest", strings.Repeat("a", 64)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.ZAdd(ctx, base.ActiveKey(msg.Queue), redis.Z{Score: 1, Member: msg.ID}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.ZAdd(ctx, base.LeaseKey(msg.Queue), redis.Z{Score: float64(now.Unix() + 30), Member: msg.ID}).Err(); err != nil {
		t.Fatal(err)
	}
	keys := []string{
		taskKey, base.ActiveKey(msg.Queue), base.LeaseKey(msg.Queue), base.CompletedKey(msg.Queue),
		base.ProcessedKey(msg.Queue, now), base.ProcessedTotalKey(msg.Queue),
	}
	before := make(map[string]string, len(keys))
	for _, key := range keys {
		before[key] = r.client.Dump(ctx, key).Val()
	}
	if err := r.MarkAsComplete(ctx, msg); err == nil || !strings.Contains(err.Error(), "receiver target completion retention unsupported") {
		t.Fatalf("completion error = %v, want receiver target refusal", err)
	}
	for _, key := range keys {
		if after := r.client.Dump(ctx, key).Val(); after != before[key] {
			t.Errorf("key %q changed after refusal", key)
		}
	}
}

func TestInspectorBulkRefusesMarkedTaskWithoutWrites(t *testing.T) {
	const group = "group"
	tests := []struct {
		name  string
		state string
		run   func(*RDB) error
	}{
		{"archive pending", "pending", func(r *RDB) error { _, err := r.ArchiveAllPendingTasks(base.DefaultQueueName); return err }},
		{"delete pending", "pending", func(r *RDB) error { _, err := r.DeleteAllPendingTasks(base.DefaultQueueName); return err }},
		{"run aggregating", "aggregating", func(r *RDB) error { _, err := r.RunAllAggregatingTasks(base.DefaultQueueName, group); return err }},
		{"archive aggregating", "aggregating", func(r *RDB) error { _, err := r.ArchiveAllAggregatingTasks(base.DefaultQueueName, group); return err }},
		{"delete aggregating", "aggregating", func(r *RDB) error { _, err := r.DeleteAllAggregatingTasks(base.DefaultQueueName, group); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := setup(t)
			defer r.Close()
			ctx := t.Context()
			msg := receiverTargetQueueEffectMessage()
			encoded, err := base.EncodeMessage(msg)
			if err != nil {
				t.Fatal(err)
			}
			taskKey := base.TaskKey(msg.Queue, msg.ID)
			fields := []any{"msg", encoded, "state", test.state, "sourceIdDigest", strings.Repeat("a", 64)}
			if test.state == "aggregating" {
				fields = append(fields, "group", group)
			}
			if err := r.client.HSet(ctx, taskKey, fields...).Err(); err != nil {
				t.Fatal(err)
			}
			if test.state == "pending" {
				if err := r.client.ZAdd(ctx, base.PendingKey(msg.Queue), redis.Z{Score: 1, Member: msg.ID}).Err(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := r.client.ZAdd(ctx, base.GroupKey(msg.Queue, group), redis.Z{Score: 1, Member: msg.ID}).Err(); err != nil {
					t.Fatal(err)
				}
				if err := r.client.SAdd(ctx, base.AllGroups(msg.Queue), group).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
				t.Fatal(err)
			}
			keys := []string{taskKey, base.PendingKey(msg.Queue), base.ActiveKey(msg.Queue), base.ScheduledKey(msg.Queue), base.RetryKey(msg.Queue), base.ArchivedKey(msg.Queue), base.CompletedKey(msg.Queue), base.LeaseKey(msg.Queue), base.GroupKey(msg.Queue, group), base.AllGroups(msg.Queue), base.AllQueues}
			before := make(map[string]string, len(keys))
			for _, key := range keys {
				before[key] = r.client.Dump(ctx, key).Val()
			}
			if err := test.run(r); err == nil || !strings.Contains(err.Error(), "RECEIVER TARGET") {
				t.Fatalf("bulk Inspector error = %v, want receiver target refusal", err)
			}
			for _, key := range keys {
				if after := r.client.Dump(ctx, key).Val(); after != before[key] {
					t.Errorf("key %q changed after refusal", key)
				}
			}
		})
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

func receiverTargetQueueEffectMessage() *base.TaskMessage {
	return &base.TaskMessage{
		ID: "advance:task", Type: "nebpilot:advance", Queue: base.DefaultQueueName, Retry: 3,
		Payload: []byte(`{"effectId":"00000000-0000-4000-8000-000000000001:1:queue_wakeup:0","schemaVersion":"nebpilot.queue-effect-task.v1","sourceRevision":"1","taskPayload":{},"taskPayloadDigest":"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"}`),
	}
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
		"schemaVersion": "receiver-target.capacity.v2",
		"total.rows":    "0", "total.bytes": "0", "total.reservedBytes": "0", "admitted.rows": "400717", "admitted.bytes": "481734986", "admitted.reservedBytes": "0",
		"envelope.declaredCeilingBytes": "601882624", "envelope.predecessorMeasurementProofDigest": "sha256:3d0e3b7f42bd36a9b4254aa6979a5b2d4c9fb60629a588546264c0a3fe3eb9d6", "kernel.reservedBytes": "2639", "kernel.proofDigest": "sha256:d1518bea5f6314653a7ccbe314b0c51f0a5b07d451a8782eaa78f07ce28c44d3",
		"agentStats.rows": "0", "agentStats.bytes": "0", "agentStats.reservedBytes": "0", "agentStats.limitRows": "88320", "agentStats.limitBytes": "77008632", "agentStats.limitReservedBytes": "0",
		"earnedHistory.rows": "0", "earnedHistory.bytes": "0", "earnedHistory.reservedBytes": "0", "earnedHistory.limitRows": "188481", "earnedHistory.limitBytes": "24523320", "earnedHistory.limitReservedBytes": "0",
		"routinePause.rows": "0", "routinePause.bytes": "0", "routinePause.reservedBytes": "0", "routinePause.limitRows": "62476", "routinePause.limitBytes": "26763034", "routinePause.limitReservedBytes": "0",
		"queueWakeup.rows": "0", "queueWakeup.bytes": "0", "queueWakeup.reservedBytes": "0", "queueWakeup.limitRows": "61440", "queueWakeup.limitBytes": "353440000", "queueWakeup.limitReservedBytes": "0",
	}
}

type receiverTargetRedisKeyState struct {
	kind    string
	value   interface{}
	ttlKind int8
}

func receiverTargetKeySnapshot(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) [receiverTargetQueueKeyCount]receiverTargetRedisKeyState {
	t.Helper()
	var state [receiverTargetQueueKeyCount]receiverTargetRedisKeyState
	for i, key := range keys {
		kind, err := r.client.Type(t.Context(), key).Result()
		if err != nil {
			t.Fatal(err)
		}
		var value interface{}
		switch kind {
		case "none":
		case "hash":
			value = r.client.HGetAll(t.Context(), key).Val()
		case "string":
			value = r.client.Get(t.Context(), key).Val()
		case "set":
			members := r.client.SMembers(t.Context(), key).Val()
			sort.Strings(members)
			value = members
		case "zset":
			value = r.client.ZRangeWithScores(t.Context(), key, 0, -1).Val()
		default:
			t.Fatalf("unsupported Redis key type %q", kind)
		}
		ttl := r.client.PTTL(t.Context(), key).Val()
		ttlKind := int8(1)
		if ttl == -1 {
			ttlKind = -1
		} else if ttl == -2 {
			ttlKind = -2
		}
		state[i] = receiverTargetRedisKeyState{kind: kind, value: value, ttlKind: ttlKind}
	}
	return state
}

func TestReceiverTargetCapacityV2FamilyAndRefusalCoverage(t *testing.T) {
	engineeringDigest := "70e19aef76a6f73f1a253546df4c1e6d48db271989d56f7667d4ce051ca4e04f"
	blueprintDigest := "23ba36df6076e55bfedb7047d04d97f2a57b27f6be96cb40f22ead1a1ec2893b"
	addEngineering := func(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) {
		t.Helper()
		if err := r.client.HSet(t.Context(), keys[1], "engineeringOutcome", engineeringDigest).Err(); err != nil {
			t.Fatal(err)
		}
		if err := r.client.HSet(t.Context(), keys[4], map[string]any{
			"admitted.rows": "401485", "admitted.bytes": "482601716",
			"engineeringOutcome.rows": "0", "engineeringOutcome.bytes": "0", "engineeringOutcome.reservedBytes": "0",
			"engineeringOutcome.limitRows": "768", "engineeringOutcome.limitBytes": "866730", "engineeringOutcome.limitReservedBytes": "0",
		}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	addBlueprint := func(rows, bytes string) func(*testing.T, *RDB, [receiverTargetQueueKeyCount]string) {
		return func(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) {
			t.Helper()
			if err := r.client.HSet(t.Context(), keys[1], "blueprintStats", blueprintDigest).Err(); err != nil {
				t.Fatal(err)
			}
			if err := r.client.HSet(t.Context(), keys[4], map[string]any{
				"total.rows": rows, "total.bytes": bytes,
				"blueprintStats.rows": rows, "blueprintStats.bytes": bytes, "blueprintStats.reservedBytes": "0",
			}).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	tests := []struct {
		name   string
		valid  bool
		mutate func(*testing.T, *RDB, [receiverTargetQueueKeyCount]string)
	}{
		{name: "engineering outcome fixed family", valid: true, mutate: addEngineering},
		{name: "blueprint statistics shared family", valid: true, mutate: addBlueprint("1", "1")},
		{name: "all six families", valid: true, mutate: func(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) {
			addEngineering(t, r, keys)
			addBlueprint("1", "1")(t, r, keys)
		}},
		{name: "wrong schema", mutate: func(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) {
			receiverTargetHSet(t, r, keys[4], "schemaVersion", "receiver-target.capacity.v1")
		}},
		{name: "unknown active member", mutate: func(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) {
			receiverTargetHSet(t, r, keys[1], "unknown", strings.Repeat("a", 64))
		}},
		{name: "unknown capacity member", mutate: func(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) {
			receiverTargetHSet(t, r, keys[4], "unknown", "0")
		}},
		{name: "inactive family fields", mutate: func(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) {
			receiverTargetHSet(t, r, keys[4], "engineeringOutcome.rows", "0", "engineeringOutcome.bytes", "0", "engineeringOutcome.reservedBytes", "0", "engineeringOutcome.limitRows", "768", "engineeringOutcome.limitBytes", "866730", "engineeringOutcome.limitReservedBytes", "0")
		}},
		{name: "descriptor mismatch", mutate: func(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) {
			receiverTargetHSet(t, r, keys[1], "queueWakeup", strings.Repeat("a", 64))
		}},
		{name: "admitted totals mismatch", mutate: func(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) {
			receiverTargetHSet(t, r, keys[4], "admitted.rows", "400718")
		}},
		{name: "occupied totals mismatch", mutate: func(t *testing.T, r *RDB, keys [receiverTargetQueueKeyCount]string) {
			receiverTargetHSet(t, r, keys[4], "total.bytes", "1")
		}},
		{name: "shared row charge violation", mutate: addBlueprint("2", "1")},
		{name: "envelope breach", mutate: addBlueprint("1", "121000000")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := setup(t)
			defer r.Close()
			now := time.Unix(1725148800, 123)
			r.SetClock(timeutil.NewSimulatedClock(now))
			input := receiverTargetInitialFixture(now)
			msg := receiverTargetQueueEffectMessage()
			encoded, err := base.EncodeMessage(msg)
			if err != nil {
				t.Fatal(err)
			}
			keys, _, err := r.receiverTargetInitialCommand(t.Context(), msg, encoded, input)
			if err != nil {
				t.Fatal(err)
			}
			seedReceiverTargetCommon(t, r, keys, input)
			tc.mutate(t, r, keys)
			before := receiverTargetKeySnapshot(t, r, keys)
			err = r.EnqueueReceiverTarget(t.Context(), msg, input)
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
				capacity := r.client.HGetAll(t.Context(), keys[4]).Val()
				queueRows, rowsErr := strconv.ParseInt(capacity["queueWakeup.rows"], 10, 64)
				queueBytes, bytesErr := strconv.ParseInt(capacity["queueWakeup.bytes"], 10, 64)
				totalRows, totalRowsErr := strconv.ParseInt(capacity["total.rows"], 10, 64)
				totalBytes, totalBytesErr := strconv.ParseInt(capacity["total.bytes"], 10, 64)
				blueprintRows, _ := strconv.ParseInt(capacity["blueprintStats.rows"], 10, 64)
				blueprintBytes, _ := strconv.ParseInt(capacity["blueprintStats.bytes"], 10, 64)
				if rowsErr != nil || bytesErr != nil || totalRowsErr != nil || totalBytesErr != nil || queueRows <= 0 || queueBytes <= 0 || totalRows != queueRows+blueprintRows || totalBytes != queueBytes+blueprintBytes {
					t.Fatalf("invalid projected capacity: %#v", capacity)
				}
				return
			}
			if errors.CanonicalCode(err) != errors.FailedPrecondition {
				t.Fatalf("error = %v, want FailedPrecondition", err)
			}
			after := receiverTargetKeySnapshot(t, r, keys)
			for i := range before {
				if !reflect.DeepEqual(after[i], before[i]) {
					t.Fatalf("capacity refusal changed receiver transaction key %d", i+1)
				}
			}
		})
	}
}

func receiverTargetHSet(t *testing.T, r *RDB, key string, values ...interface{}) {
	t.Helper()
	if err := r.client.HSet(t.Context(), key, values...).Err(); err != nil {
		t.Fatal(err)
	}
}
