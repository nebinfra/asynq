package asynq

import (
	"testing"

	"github.com/nebinfra/asynq/internal/rdb"
)

func TestReceiverTargetQueueManifestDigest(t *testing.T) {
	if got, want := ReceiverTargetQueueManifestDigest(), rdb.ReceiverTargetQueueManifestDigest(); got != want {
		t.Fatalf("manifest digest = %q, want %q", got, want)
	}
}
