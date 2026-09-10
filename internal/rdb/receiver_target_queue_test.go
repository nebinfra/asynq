package rdb

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/nebinfra/asynq/internal/base"
	h "github.com/nebinfra/asynq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func TestReceiverTargetDefaultQueueOrdering(t *testing.T) {
	r := setup(t)
	defer r.Close()
	first := h.NewTaskMessage("first", nil)
	second := h.NewTaskMessage("second", nil)
	if err := r.Enqueue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	assertQueueScores(t, r, base.PendingKey(base.DefaultQueueName), []redis.Z{
		{Member: first.ID, Score: 0},
		{Member: second.ID, Score: 1},
	})

	got, _, err := r.Dequeue(base.DefaultQueueName)
	if err != nil || got.ID != first.ID {
		t.Fatalf("first dequeue = %v, %v", got, err)
	}
	got, _, err = r.Dequeue(base.DefaultQueueName)
	if err != nil || got.ID != second.ID {
		t.Fatalf("second dequeue = %v, %v", got, err)
	}
	assertQueueScores(t, r, base.ActiveKey(base.DefaultQueueName), []redis.Z{
		{Member: first.ID, Score: 0},
		{Member: second.ID, Score: 1},
	})

	if err := r.Requeue(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err := r.Requeue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	assertQueueScores(t, r, base.PendingKey(base.DefaultQueueName), []redis.Z{
		{Member: first.ID, Score: -1},
		{Member: second.ID, Score: 0},
	})
	got, _, err = r.Dequeue(base.DefaultQueueName)
	if err != nil || got.ID != first.ID {
		t.Fatalf("requeued dequeue = %v, %v", got, err)
	}
}

func TestReceiverTargetForceRemoveDefaultQueue(t *testing.T) {
	r := setup(t)
	defer r.Close()
	msg := h.NewTaskMessage("pending", nil)
	if err := r.Enqueue(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if err := r.RemoveQueue(base.DefaultQueueName, true); err != nil {
		t.Fatal(err)
	}
	if got := r.client.Exists(context.Background(), base.DefaultQueue, base.TaskKey(base.DefaultQueueName, msg.ID)).Val(); got != 0 {
		t.Fatalf("force remove left %d queue or task keys", got)
	}
}

func TestReceiverTargetUniqueEnqueueRefusesInvalidScoreWithoutWrites(t *testing.T) {
	r := setup(t)
	defer r.Close()
	r.queuesPublished.Store(base.DefaultQueueName, true)
	r.client.ZAdd(context.Background(), base.DefaultQueue,
		redis.Z{Member: "first", Score: 0}, redis.Z{Member: "second", Score: 0})
	msg := h.NewTaskMessage("unique", nil)
	msg.UniqueKey = base.UniqueKey(msg.Queue, msg.Type, msg.Payload)
	before := r.client.Dump(context.Background(), base.DefaultQueue).Val()
	if err := r.EnqueueUnique(context.Background(), msg, time.Minute); err == nil {
		t.Fatal("unique enqueue accepted duplicate endpoint")
	}
	if after := r.client.Dump(context.Background(), base.DefaultQueue).Val(); after != before {
		t.Fatal("unique enqueue changed pending queue")
	}
	if got := r.client.Exists(context.Background(), msg.UniqueKey, base.TaskKey(msg.Queue, msg.ID)).Val(); got != 0 {
		t.Fatalf("unique enqueue left %d lock or task keys", got)
	}
}

func TestReceiverTargetQueueAppendPathsRefuseDuplicateEndpointWithoutWrites(t *testing.T) {
	tests := []struct {
		name  string
		seed  func(*RDB, *base.TaskMessage) []string
		apply func(*RDB, *base.TaskMessage) error
	}{
		{
			name: "forward scheduled",
			seed: func(r *RDB, msg *base.TaskMessage) []string {
				seedTaskHash(t, r, msg)
				r.client.HSet(context.Background(), base.TaskKey(msg.Queue, msg.ID), "state", "scheduled")
				r.client.ZAdd(context.Background(), base.ScheduledKey(msg.Queue), redis.Z{Member: msg.ID, Score: 0})
				return []string{base.DefaultQueue, base.ScheduledKey(msg.Queue), base.TaskKey(msg.Queue, msg.ID)}
			},
			apply: func(r *RDB, _ *base.TaskMessage) error {
				return r.ForwardIfReady(base.DefaultQueueName)
			},
		},
		{
			name: "run archived task",
			seed: func(r *RDB, msg *base.TaskMessage) []string {
				seedTaskHash(t, r, msg)
				r.client.HSet(context.Background(), base.TaskKey(msg.Queue, msg.ID), "state", "archived")
				r.client.ZAdd(context.Background(), base.ArchivedKey(msg.Queue), redis.Z{Member: msg.ID, Score: 0})
				r.client.SAdd(context.Background(), base.AllQueues, msg.Queue)
				return []string{base.DefaultQueue, base.ArchivedKey(msg.Queue), base.TaskKey(msg.Queue, msg.ID)}
			},
			apply: func(r *RDB, msg *base.TaskMessage) error {
				return r.RunTask(msg.Queue, msg.ID)
			},
		},
		{
			name: "run all retry tasks",
			seed: func(r *RDB, msg *base.TaskMessage) []string {
				seedTaskHash(t, r, msg)
				r.client.HSet(context.Background(), base.TaskKey(msg.Queue, msg.ID), "state", "retry")
				r.client.ZAdd(context.Background(), base.RetryKey(msg.Queue), redis.Z{Member: msg.ID, Score: 0})
				r.client.SAdd(context.Background(), base.AllQueues, msg.Queue)
				return []string{base.DefaultQueue, base.RetryKey(msg.Queue), base.TaskKey(msg.Queue, msg.ID)}
			},
			apply: func(r *RDB, msg *base.TaskMessage) error {
				_, err := r.RunAllRetryTasks(msg.Queue)
				return err
			},
		},
		{
			name: "run all aggregating tasks",
			seed: func(r *RDB, msg *base.TaskMessage) []string {
				const group = "group"
				seedTaskHash(t, r, msg)
				r.client.HSet(context.Background(), base.TaskKey(msg.Queue, msg.ID), "state", "aggregating", "group", group)
				r.client.ZAdd(context.Background(), base.GroupKey(msg.Queue, group), redis.Z{Member: msg.ID, Score: 0})
				r.client.SAdd(context.Background(), base.AllQueues, msg.Queue)
				r.client.SAdd(context.Background(), base.AllGroups(msg.Queue), group)
				return []string{base.DefaultQueue, base.GroupKey(msg.Queue, group), base.AllGroups(msg.Queue), base.TaskKey(msg.Queue, msg.ID)}
			},
			apply: func(r *RDB, msg *base.TaskMessage) error {
				_, err := r.RunAllAggregatingTasks(msg.Queue, "group")
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := setup(t)
			defer r.Close()
			msg := h.NewTaskMessage("candidate", nil)
			r.client.ZAdd(context.Background(), base.DefaultQueue,
				redis.Z{Member: "first", Score: 0}, redis.Z{Member: "second", Score: 0})
			keys := tc.seed(r, msg)
			before := make(map[string]string, len(keys))
			for _, key := range keys {
				before[key] = r.client.Dump(context.Background(), key).Val()
			}
			if err := tc.apply(r, msg); err == nil {
				t.Fatal("append path accepted duplicate endpoint")
			}
			for _, key := range keys {
				if after := r.client.Dump(context.Background(), key).Val(); after != before[key] {
					t.Fatalf("key %q changed after refusal", key)
				}
			}
		})
	}
}

func TestReceiverTargetDefaultQueueRefusesInvalidScoresWithoutWrites(t *testing.T) {
	tests := []struct {
		name  string
		seed  func(*RDB, *base.TaskMessage)
		apply func(*RDB, *base.TaskMessage) error
	}{
		{
			name: "append endpoint",
			seed: func(r *RDB, _ *base.TaskMessage) {
				r.client.ZAdd(context.Background(), base.DefaultQueue, redis.Z{Member: "tail", Score: receiverTargetMaximumQueueScore})
			},
			apply: func(r *RDB, msg *base.TaskMessage) error { return r.Enqueue(context.Background(), msg) },
		},
		{
			name: "append duplicate endpoint",
			seed: func(r *RDB, _ *base.TaskMessage) {
				r.client.ZAdd(context.Background(), base.DefaultQueue,
					redis.Z{Member: "first", Score: 0}, redis.Z{Member: "second", Score: 0})
			},
			apply: func(r *RDB, msg *base.TaskMessage) error { return r.Enqueue(context.Background(), msg) },
		},
		{
			name: "append fractional endpoint",
			seed: func(r *RDB, _ *base.TaskMessage) {
				r.client.ZAdd(context.Background(), base.DefaultQueue, redis.Z{Member: "tail", Score: 0.5})
			},
			apply: func(r *RDB, msg *base.TaskMessage) error { return r.Enqueue(context.Background(), msg) },
		},
		{
			name: "append endpoint below safe range",
			seed: func(r *RDB, _ *base.TaskMessage) {
				r.client.ZAdd(context.Background(), base.DefaultQueue,
					redis.Z{Member: "tail", Score: -receiverTargetMaximumQueueScore - 1})
			},
			apply: func(r *RDB, msg *base.TaskMessage) error { return r.Enqueue(context.Background(), msg) },
		},
		{
			name: "duplicate pending score",
			seed: func(r *RDB, msg *base.TaskMessage) {
				seedTaskHash(t, r, msg)
				r.client.ZAdd(context.Background(), base.DefaultQueue,
					redis.Z{Member: msg.ID, Score: 0}, redis.Z{Member: "peer", Score: 0})
			},
			apply: func(r *RDB, _ *base.TaskMessage) error {
				_, _, err := r.Dequeue(base.DefaultQueueName)
				return err
			},
		},
		{
			name: "selected score above safe range",
			seed: func(r *RDB, msg *base.TaskMessage) {
				seedTaskHash(t, r, msg)
				r.client.ZAdd(context.Background(), base.DefaultQueue,
					redis.Z{Member: msg.ID, Score: receiverTargetMaximumQueueScore + 1})
			},
			apply: func(r *RDB, _ *base.TaskMessage) error {
				_, _, err := r.Dequeue(base.DefaultQueueName)
				return err
			},
		},
		{
			name: "active append endpoint",
			seed: func(r *RDB, msg *base.TaskMessage) {
				seedTaskHash(t, r, msg)
				r.client.ZAdd(context.Background(), base.DefaultQueue, redis.Z{Member: msg.ID, Score: 0})
				r.client.ZAdd(context.Background(), base.ActiveKey(base.DefaultQueueName), redis.Z{Member: "active-tail", Score: receiverTargetMaximumQueueScore})
			},
			apply: func(r *RDB, _ *base.TaskMessage) error {
				_, _, err := r.Dequeue(base.DefaultQueueName)
				return err
			},
		},
		{
			name: "active duplicate endpoint",
			seed: func(r *RDB, msg *base.TaskMessage) {
				seedTaskHash(t, r, msg)
				r.client.ZAdd(context.Background(), base.DefaultQueue, redis.Z{Member: msg.ID, Score: 0})
				r.client.ZAdd(context.Background(), base.ActiveKey(base.DefaultQueueName),
					redis.Z{Member: "first", Score: 0}, redis.Z{Member: "second", Score: 0})
			},
			apply: func(r *RDB, _ *base.TaskMessage) error {
				_, _, err := r.Dequeue(base.DefaultQueueName)
				return err
			},
		},
		{
			name: "active endpoint below safe range",
			seed: func(r *RDB, msg *base.TaskMessage) {
				seedTaskHash(t, r, msg)
				r.client.ZAdd(context.Background(), base.DefaultQueue, redis.Z{Member: msg.ID, Score: 0})
				r.client.ZAdd(context.Background(), base.ActiveKey(base.DefaultQueueName),
					redis.Z{Member: "tail", Score: -receiverTargetMaximumQueueScore - 1})
			},
			apply: func(r *RDB, _ *base.TaskMessage) error {
				_, _, err := r.Dequeue(base.DefaultQueueName)
				return err
			},
		},
		{
			name: "requeue prepend endpoint",
			seed: func(r *RDB, msg *base.TaskMessage) {
				seedTaskHash(t, r, msg)
				r.client.HSet(context.Background(), base.TaskKey(msg.Queue, msg.ID), "state", "active")
				r.client.ZAdd(context.Background(), base.DefaultQueue, redis.Z{Member: "pending-head", Score: -receiverTargetMaximumQueueScore})
				r.client.ZAdd(context.Background(), base.ActiveKey(base.DefaultQueueName), redis.Z{Member: msg.ID, Score: 0})
				r.client.ZAdd(context.Background(), base.LeaseKey(base.DefaultQueueName), redis.Z{Member: msg.ID, Score: 1})
			},
			apply: func(r *RDB, msg *base.TaskMessage) error { return r.Requeue(context.Background(), msg) },
		},
		{
			name: "requeue duplicate endpoint",
			seed: func(r *RDB, msg *base.TaskMessage) {
				seedTaskHash(t, r, msg)
				r.client.HSet(context.Background(), base.TaskKey(msg.Queue, msg.ID), "state", "active")
				r.client.ZAdd(context.Background(), base.DefaultQueue,
					redis.Z{Member: "first", Score: 0}, redis.Z{Member: "second", Score: 0})
				r.client.ZAdd(context.Background(), base.ActiveKey(base.DefaultQueueName), redis.Z{Member: msg.ID, Score: 0})
				r.client.ZAdd(context.Background(), base.LeaseKey(base.DefaultQueueName), redis.Z{Member: msg.ID, Score: 1})
			},
			apply: func(r *RDB, msg *base.TaskMessage) error { return r.Requeue(context.Background(), msg) },
		},
		{
			name: "requeue endpoint above safe range",
			seed: func(r *RDB, msg *base.TaskMessage) {
				seedTaskHash(t, r, msg)
				r.client.HSet(context.Background(), base.TaskKey(msg.Queue, msg.ID), "state", "active")
				r.client.ZAdd(context.Background(), base.DefaultQueue,
					redis.Z{Member: "head", Score: receiverTargetMaximumQueueScore + 1})
				r.client.ZAdd(context.Background(), base.ActiveKey(base.DefaultQueueName), redis.Z{Member: msg.ID, Score: 0})
				r.client.ZAdd(context.Background(), base.LeaseKey(base.DefaultQueueName), redis.Z{Member: msg.ID, Score: 1})
			},
			apply: func(r *RDB, msg *base.TaskMessage) error { return r.Requeue(context.Background(), msg) },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := setup(t)
			defer r.Close()
			msg := h.NewTaskMessage("candidate", nil)
			tc.seed(r, msg)
			beforePending := r.client.ZRangeWithScores(context.Background(), base.DefaultQueue, 0, -1).Val()
			beforeActive := r.client.ZRangeWithScores(context.Background(), base.ActiveKey(base.DefaultQueueName), 0, -1).Val()
			beforeLease := r.client.ZRangeWithScores(context.Background(), base.LeaseKey(base.DefaultQueueName), 0, -1).Val()
			beforeState := r.client.HGet(context.Background(), base.TaskKey(msg.Queue, msg.ID), "state").Val()
			if err := tc.apply(r, msg); err == nil {
				t.Fatal("invalid queue score accepted")
			}
			afterPending := r.client.ZRangeWithScores(context.Background(), base.DefaultQueue, 0, -1).Val()
			if !reflect.DeepEqual(beforePending, afterPending) {
				t.Fatalf("pending queue changed: before=%v after=%v", beforePending, afterPending)
			}
			if after := r.client.ZRangeWithScores(context.Background(), base.ActiveKey(base.DefaultQueueName), 0, -1).Val(); !reflect.DeepEqual(beforeActive, after) {
				t.Fatalf("active queue changed: before=%v after=%v", beforeActive, after)
			}
			if after := r.client.ZRangeWithScores(context.Background(), base.LeaseKey(base.DefaultQueueName), 0, -1).Val(); !reflect.DeepEqual(beforeLease, after) {
				t.Fatalf("lease changed: before=%v after=%v", beforeLease, after)
			}
			if after := r.client.HGet(context.Background(), base.TaskKey(msg.Queue, msg.ID), "state").Val(); after != beforeState {
				t.Fatalf("task state changed: before=%q after=%q", beforeState, after)
			}
			if tc.name == "append endpoint" && r.client.Exists(context.Background(), base.TaskKey(msg.Queue, msg.ID)).Val() != 0 {
				t.Fatal("refused enqueue wrote task hash")
			}
		})
	}
}

func seedTaskHash(t *testing.T, r *RDB, msg *base.TaskMessage) {
	t.Helper()
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.client.HSet(context.Background(), base.TaskKey(msg.Queue, msg.ID), "msg", encoded, "state", "pending").Err(); err != nil {
		t.Fatal(err)
	}
}

func assertQueueScores(t *testing.T, r *RDB, key string, want []redis.Z) {
	t.Helper()
	got := r.client.ZRangeWithScores(context.Background(), key, 0, -1).Val()
	if len(got) != len(want) {
		t.Fatalf("%s entries = %v, want %v", key, got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("%s entries = %v, want %v", key, got, want)
		}
	}
}
