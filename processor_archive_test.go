package asynq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nebinfra/asynq/internal/base"
)

type archiveGateBroker struct {
	base.Broker
	archive func(context.Context, *base.TaskMessage, string) error
}

func (b archiveGateBroker) Archive(ctx context.Context, task *base.TaskMessage, cause string) error {
	return b.archive(ctx, task, cause)
}

func TestArchiveFinalizationSync(t *testing.T) {
	for _, failure := range []string{"finalizer", "broker"} {
		t.Run(failure, func(t *testing.T) {
			requests := make(chan *syncRequest, 1)
			message := &base.TaskMessage{ID: "task-1", Type: "example", Payload: []byte(`{"value":1}`), Headers: map[string]string{"origin": "original"}, Queue: "default", Retry: 3, Retried: 3}
			cause := errors.New("handler failed")
			calls, writes := 0, 0
			var contexts []context.Context
			p := &processor{logger: testLogger, baseCtxFn: context.Background, syncRequestCh: requests}
			p.beforeArchive = func(ctx context.Context, task *Task, got error) error {
				calls++
				contexts = append(contexts, ctx)
				id, ok := GetTaskID(ctx)
				if ctx.Err() != nil || !ok || id != message.ID || got != cause || task.Type() != message.Type || string(task.Payload()) != string(message.Payload) || task.Headers()["origin"] != "original" {
					t.Fatal("archive finalizer lost original task or live context")
				}
				if failure == "finalizer" && calls == 1 {
					return errors.New("write unavailable")
				}
				return nil
			}
			p.broker = archiveGateBroker{archive: func(ctx context.Context, task *base.TaskMessage, got string) error {
				writes++
				if ctx.Err() != nil || task != message || got != cause.Error() {
					t.Fatal("archive changed original task or used a cancelled context")
				}
				if failure == "broker" && writes == 1 {
					return errors.New("broker unavailable")
				}
				return nil
			}}
			p.archive(base.NewLease(time.Now().Add(time.Minute)), message, cause)
			if failure == "finalizer" && writes != 0 {
				t.Fatal("finalizer refusal reached archive broker")
			}
			select {
			case request := <-requests:
				if err := request.fn(); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("failed archive lost synchronization ownership")
			}
			if calls != 2 || contexts[0] == contexts[1] || contexts[0].Err() == nil || contexts[1].Err() == nil {
				t.Fatal("sync must repeat finalization in its own scoped context")
			}
		})
	}
}

func TestArchiveFinalizationLeaseExpires(t *testing.T) {
	requests := make(chan *syncRequest, 1)
	lease := base.NewLease(time.Now().Add(time.Minute))
	writes := 0
	p := &processor{logger: testLogger, baseCtxFn: context.Background, syncRequestCh: requests}
	p.beforeArchive = func(context.Context, *Task, error) error {
		lease.Reset(time.Now().Add(-time.Second))
		return nil
	}
	p.broker = archiveGateBroker{archive: func(context.Context, *base.TaskMessage, string) error { writes++; return nil }}
	p.archive(lease, &base.TaskMessage{ID: "task-1", Queue: "default"}, errors.New("failed"))
	request := <-requests
	if err := request.fn(); !errors.Is(err, ErrLeaseExpired) || writes != 0 {
		t.Fatalf("expired lease archived: writes=%d err=%v", writes, err)
	}
}
