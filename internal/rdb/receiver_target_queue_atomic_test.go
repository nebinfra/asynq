package rdb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nebinfra/asynq/internal/base"
	"github.com/nebinfra/asynq/internal/timeutil"
	"github.com/redis/go-redis/v9"
)

type receiverAcknowledgementHook struct {
	action string
	once   sync.Once
	after  func(redis.Cmder) error
}

func (h *receiverAcknowledgementHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *receiverAcknowledgementHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *receiverAcknowledgementHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err != nil || cmd.Name() != "evalsha" && cmd.Name() != "eval" {
			return err
		}
		args := cmd.Args()
		// EVAL and EVALSHA use the same fixed key/argument positions.
		const actionIndex = 3 + receiverTargetQueueKeyCount + 7
		if len(args) != 3+receiverTargetQueueKeyCount+receiverTargetQueueOperandCount || fmt.Sprint(args[actionIndex]) != h.action {
			return nil
		}
		result, ok := cmd.(*redis.Cmd)
		if !ok {
			return nil
		}
		values, ok := result.Val().([]interface{})
		if !ok || len(values) != 4 || values[0] != "ok" {
			return nil
		}
		h.once.Do(func() { err = h.after(cmd) })
		if err != nil {
			cmd.SetErr(err)
		}
		return err
	}
}

func TestReceiverTargetForwardAcknowledgementAtomic(t *testing.T) {
	r := setup(t)
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Unix(1725148800, 0)
	clock := timeutil.NewSimulatedClock(now)
	r.SetClock(clock)
	input := receiverTargetInitialFixture(now)
	input.ProcessAt = now.Add(time.Minute)
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
	if err = r.EnqueueReceiverTarget(t.Context(), msg, input); err != nil {
		t.Fatal(err)
	}
	clock.AdvanceTime(time.Minute)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	r.client.AddHook(&receiverAcknowledgementHook{action: "due-forward", after: func(redis.Cmder) error { close(entered); <-release; return nil }})
	done := make(chan error, 1)
	go func() { done <- r.ForwardIfReady(base.DefaultQueueName) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("forward write did not complete")
	}
	count, err := r.client.HLen(t.Context(), keys[9]).Result()
	if err != nil {
		t.Fatal(err)
	}
	if count != 14 {
		t.Fatalf("forward exposed unfinalized reservation: fields=%d", count)
	}
	if n, err := r.client.Exists(t.Context(), keys[5], keys[6]).Result(); err != nil || n != 0 {
		t.Fatalf("forward retained acknowledgement artifacts: %d %v", n, err)
	}
	task, _, err := r.Dequeue(base.DefaultQueueName)
	if err != nil || task == nil || task.ID != msg.ID {
		t.Fatalf("concurrent dequeue: %+v %v", task, err)
	}
	unblock()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReceiverTargetAtomicLostEnqueueResponse(t *testing.T) {
	r := setup(t)
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Unix(1725148800, 0)
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
	lost := errors.New("completed response unavailable")
	r.client.AddHook(&receiverAcknowledgementHook{action: "initial-enqueue", after: func(redis.Cmder) error { return lost }})
	if err = r.EnqueueReceiverTarget(t.Context(), msg, input); err == nil {
		t.Fatal("lost response reported success")
	}
	if n, err := r.client.HLen(t.Context(), keys[9]).Result(); err != nil || n != 12 {
		t.Fatalf("lost response source: %d %v", n, err)
	}
	if n, err := r.client.Exists(t.Context(), keys[5], keys[6]).Result(); err != nil || n != 0 {
		t.Fatalf("lost response artifacts: %d %v", n, err)
	}
	before := receiverTargetKeySnapshot(t, r, keys)
	if err = r.EnqueueReceiverTarget(t.Context(), msg, input); err != nil {
		t.Fatalf("retained replay: %v", err)
	}
	if after := receiverTargetKeySnapshot(t, r, keys); !reflect.DeepEqual(after, before) {
		t.Fatal("lost-response replay changed custody or capacity")
	}
}

func TestReceiverTargetAtomicAcknowledgementPreflight(t *testing.T) {
	for _, mode := range []string{"wrong_type", "malformed", "stale", "both_present", "settled_recovery", "applied_recovery"} {
		t.Run(mode, func(t *testing.T) {
			r := setup(t)
			t.Cleanup(func() {
				if err := r.Close(); err != nil {
					t.Error(err)
				}
			})
			now := time.Unix(1725148800, 0)
			r.SetClock(timeutil.NewSimulatedClock(now))
			input := receiverTargetInitialFixture(now)
			msg := receiverTargetQueueEffectMessage()
			encoded, err := base.EncodeMessage(msg)
			if err != nil {
				t.Fatal(err)
			}
			keys, args, err := r.receiverTargetInitialCommand(t.Context(), msg, encoded, input)
			if err != nil {
				t.Fatal(err)
			}
			seedReceiverTargetCommon(t, r, keys, input)
			raw := func(action string) {
				values := make([]interface{}, len(args))
				for i, v := range args {
					values[i] = v
				}
				values[7] = action
				out, e := r.client.Eval(t.Context(), receiverTargetQueueSource, keys[:], values...).Result()
				if e != nil {
					t.Fatal(e)
				}
				if _, e = parseReceiverTargetTaskCASResult("test.raw", out); e != nil {
					t.Fatal(e)
				}
			}
			switch mode {
			case "wrong_type":
				err = r.client.Set(t.Context(), keys[6], "wrong", 0).Err()
			case "malformed":
				err = r.client.HSet(t.Context(), keys[6], "extra", "invalid").Err()
			default:
				raw("initial-enqueue")
				receipt, e := r.client.HGetAll(t.Context(), keys[5]).Result()
				if e != nil {
					t.Fatal(e)
				}
				if mode != "applied_recovery" {
					raw("settle")
				}
				if mode == "stale" {
					err = r.client.HSet(t.Context(), keys[6], "receiptIdentityDigest", strings.Repeat("f", 64)).Err()
				}
				if mode == "both_present" {
					err = r.client.HSet(t.Context(), keys[5], receipt).Err()
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			before := receiverTargetKeySnapshot(t, r, keys)
			err = r.EnqueueReceiverTarget(t.Context(), msg, input)
			if mode == "settled_recovery" || mode == "applied_recovery" {
				if err != nil {
					t.Fatal(err)
				}
				if n, e := r.client.HLen(t.Context(), keys[9]).Result(); e != nil || n != 12 {
					t.Fatalf("recovered source: %d %v", n, e)
				}
				if n, e := r.client.Exists(t.Context(), keys[5], keys[6]).Result(); e != nil || n != 0 {
					t.Fatalf("recovered artifacts: %d %v", n, e)
				}
				if n, e := r.client.ZCard(t.Context(), keys[11]).Result(); e != nil || n != 1 {
					t.Fatalf("recovered pending task: %d %v", n, e)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid acknowledgement accepted")
			}
			if after := receiverTargetKeySnapshot(t, r, keys); !reflect.DeepEqual(after, before) {
				t.Fatal("refusal changed native state or capacity")
			}
		})
	}
}

func TestReceiverTargetAtomicKernelBinding(t *testing.T) {
	if strings.Count(receiverTargetQueueAtomicSource, receiverTargetQueueSource) != 1 {
		t.Fatal("atomic executor must contain the unchanged kernel exactly once")
	}
}
