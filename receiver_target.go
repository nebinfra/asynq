package asynq

import "github.com/nebinfra/asynq/internal/rdb"

// ReceiverTargetQueueManifestDigest returns the immutable generated queue
// transaction identity compiled into this module.
func ReceiverTargetQueueManifestDigest() string {
	return rdb.ReceiverTargetQueueManifestDigest()
}
