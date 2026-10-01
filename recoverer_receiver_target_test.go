package asynq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nebinfra/asynq/internal/base"
	"github.com/nebinfra/asynq/internal/log"
)

// receiverTargetBroker refuses Archive exactly as the marked-task Lua does and
// records what the recoverer did instead.
type receiverTargetBroker struct {
	base.Broker
	expired  []*base.TaskMessage
	archived int
	retried  []*base.TaskMessage
}

func (b *receiverTargetBroker) ListLeaseExpired(time.Time, ...string) ([]*base.TaskMessage, error) {
	return b.expired, nil
}

func (b *receiverTargetBroker) Archive(context.Context, *base.TaskMessage, string) error {
	b.archived++
	return errors.New("ERR RECEIVER TARGET ARCHIVE UNSUPPORTED script: 0123")
}

func (b *receiverTargetBroker) Retry(_ context.Context, msg *base.TaskMessage, _ time.Time, _ string, _ bool) error {
	b.retried = append(b.retried, msg)
	return nil
}

func TestRecovererRedeliversExhaustedReceiverTargetTask(t *testing.T) {
	msg := &base.TaskMessage{ID: "timer:await:run:ref", Type: "nebpilot:timer", Queue: "default", Retry: 3, Retried: 3}
	broker := &receiverTargetBroker{expired: []*base.TaskMessage{msg}}
	r := newRecoverer(recovererParams{
		logger:         log.NewLogger(nil),
		broker:         broker,
		queues:         []string{"default"},
		interval:       time.Minute,
		retryDelayFunc: DefaultRetryDelayFunc,
		isFailureFunc:  defaultIsFailureFunc,
	})
	r.recoverLeaseExpiredTasks()
	if broker.archived != 1 || len(broker.retried) != 1 || broker.retried[0] != msg {
		t.Fatalf("archive attempts=%d retried=%d, want one refused archive then one redelivery", broker.archived, len(broker.retried))
	}
}

func TestReceiverTargetArchiveRefused(t *testing.T) {
	if !receiverTargetArchiveRefused(errors.New("ERR RECEIVER TARGET ARCHIVE UNSUPPORTED")) || receiverTargetArchiveRefused(errors.New("NOT FOUND")) || receiverTargetArchiveRefused(nil) {
		t.Fatal("refusal classification is wrong")
	}
}
