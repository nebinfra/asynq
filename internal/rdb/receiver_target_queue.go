package rdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nebinfra/asynq/internal/base"
	"github.com/nebinfra/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

const (
	receiverTargetMaximumQueueScore    = 9007199254740991
	receiverTargetMaximumTaskHashBytes = 271559
)

const (
	receiverTargetQueueKeyCount     = 24
	receiverTargetQueueOperandCount = 20
)

var receiverTargetQueueCmd = redis.NewScript(receiverTargetQueueSource)

type receiverTargetTaskCASResult struct {
	targetRevision string
	resultPayload  string
	replayed       bool
}

type receiverTargetQueueEnvelope struct {
	SchemaVersion     string          `json:"schemaVersion"`
	EffectID          string          `json:"effectId"`
	SourceRevision    string          `json:"sourceRevision"`
	TaskPayload       json.RawMessage `json:"taskPayload"`
	TaskPayloadDigest string          `json:"taskPayloadDigest"`
}

type receiverTargetNativeSource struct {
	fields            []string
	values            []string
	sourceIDDigest    string
	stateEpoch        string
	enqueueGeneration string
	taskDigest        string
	releaseFence      string
	targetRevision    string
	nativeExpiry      string
	ackReceipt        string
	ackOperation      string
}

type receiverTargetNativeTask struct {
	fields []string
	values []string
	state  string
	msg    string
}

type receiverTargetNativeCommand struct {
	action        string
	expectedState string
	expectedLease string
	expectedDue   string
	nextMessage   string
	nextValue     string
	statisticsDay string
	nativeExpiry  string
	isFailure     bool
}

func (r *RDB) EnqueueReceiverTarget(ctx context.Context, msg *base.TaskMessage, input base.ReceiverTargetQueueInitial) error {
	const op errors.Op = "rdb.EnqueueReceiverTarget"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	keys, operands, err := r.receiverTargetInitialCommand(ctx, msg, encoded, input)
	if err != nil {
		return errors.E(op, errors.FailedPrecondition, err)
	}
	keys, operands, err = r.receiverTargetRecoveryCommand(ctx, msg, encoded, input, keys, operands)
	if err != nil {
		return errors.E(op, errors.FailedPrecondition, err)
	}
	return r.executeReceiverTargetTaskCAS(ctx, op, keys, operands)
}

func (r *RDB) executeReceiverTargetTaskCAS(ctx context.Context, op errors.Op, keys [receiverTargetQueueKeyCount]string, operands [receiverTargetQueueOperandCount]string) error {
	result, err := r.applyReceiverTargetTaskCAS(ctx, op, keys, operands)
	if err != nil {
		return err
	}
	if result.replayed {
		operands[7] = "finalize"
		if _, finalizeErr := r.applyReceiverTargetTaskCAS(ctx, op, keys, operands); finalizeErr == nil {
			return nil
		}
	}
	operands[7] = "settle"
	if _, err = r.applyReceiverTargetTaskCAS(ctx, op, keys, operands); err != nil {
		return err
	}
	operands[7] = "finalize"
	_, err = r.applyReceiverTargetTaskCAS(ctx, op, keys, operands)
	return err
}

func (r *RDB) ReleaseReceiverTarget(ctx context.Context, taskID string, input base.ReceiverTargetQueueRelease) error {
	const op errors.Op = "rdb.ReleaseReceiverTarget"
	keys, operands, err := r.receiverTargetReleaseCommand(ctx, taskID, input)
	if err != nil {
		return errors.E(op, errors.FailedPrecondition, err)
	}
	return r.executeReceiverTargetTaskCAS(ctx, op, keys, operands)
}

func (r *RDB) receiverTargetReleaseCommand(ctx context.Context, taskID string, input base.ReceiverTargetQueueRelease) ([receiverTargetQueueKeyCount]string, [receiverTargetQueueOperandCount]string, error) {
	var keys [receiverTargetQueueKeyCount]string
	var operands [receiverTargetQueueOperandCount]string
	if taskID == "" || len(taskID) > 256 || !receiverTargetPositiveUint(input.RuntimeEpochRevision) || !receiverTargetPositiveUint(input.StateEpoch) || !receiverTargetDigest(input.CatalogGeneration) || input.InstanceTenant == "" || len(input.InstanceTenant) > 63 || input.EffectID == "" || len(input.EffectID) > 128 || !receiverTargetDigest(input.SourceIDDigest) || !receiverTargetDigest(input.TaskDigest) {
		return keys, operands, fmt.Errorf("invalid release receiver input")
	}
	sourceKey := "nebpilot:e:" + input.StateEpoch + ":receiver-target:queue-reservation:" + input.SourceIDDigest + ":" + taskID
	source, err := r.receiverTargetFinalizedSource(ctx, sourceKey)
	if err != nil {
		return keys, operands, err
	}
	if source.sourceIDDigest != input.SourceIDDigest || source.stateEpoch != input.StateEpoch || source.taskDigest != input.TaskDigest || source.releaseFence != "open" {
		return keys, operands, fmt.Errorf("release source mismatch")
	}
	frame := receiverTargetSuccessorSourceID(source, input.EffectID, taskID)
	evidence := receiverTargetSuccessorEvidenceDigest(frame, source)
	receipt := receiverTargetOperationReceiptIdentity(source.stateEpoch, input.InstanceTenant, frame, "Release", "release_acknowledged_absent", "existing_source_revision")
	epochID := source.stateEpoch
	keys = [receiverTargetQueueKeyCount]string{
		"nebpilot:runtime:epoch", "nebpilot:e:" + epochID + ":receiver-target:active", "nebpilot:e:" + epochID + ":receiver-target:active-revision", "nebpilot:e:" + epochID + ":receiver-target:advancement:queueWakeup", "nebpilot:e:" + epochID + ":receiver-target:capacity",
		"nebpilot:e:" + epochID + ":receiver-target:receipt:" + receipt, "nebpilot:e:" + epochID + ":receiver-target:queue-ack:" + receipt, "nebpilot:e:" + epochID + ":effects:body:" + input.EffectID, "nebpilot:e:" + epochID + ":effects:receiver-inbox:body:" + input.EffectID, sourceKey,
		base.TaskKey(base.DefaultQueueName, taskID), base.PendingKey(base.DefaultQueueName), base.ActiveKey(base.DefaultQueueName), base.ScheduledKey(base.DefaultQueueName), base.RetryKey(base.DefaultQueueName), base.ArchivedKey(base.DefaultQueueName), base.CompletedKey(base.DefaultQueueName), base.LeaseKey(base.DefaultQueueName), base.AllQueues, base.PausedKey(base.DefaultQueueName), "", base.ProcessedTotalKey(base.DefaultQueueName), "", base.FailedTotalKey(base.DefaultQueueName),
	}
	afterFields := []string{"sourceIdDigest", "stateEpoch", "enqueueGeneration", "reservedBytes", "taskDigest", "releaseFence", "targetRevision", "sourceEnqueueGeneration", "nativeExpiry", "sourceNativeExpiry", "predecessorAckReceiptDigest", "predecessorAckOperationDigest", "receiptIdentityDigest"}
	afterValues := []string{source.sourceIDDigest, source.stateEpoch, source.enqueueGeneration, "4096", source.taskDigest, "closed", source.targetRevision, source.enqueueGeneration, "", source.nativeExpiry, source.ackReceipt, source.ackOperation, receipt}
	operands = [receiverTargetQueueOperandCount]string{
		frame, evidence, input.RuntimeEpochRevision, input.CatalogGeneration, input.InstanceTenant, source.targetRevision, receipt,
		"release", "", source.taskDigest, "", "", "", "",
		strconv.FormatInt(int64(len(afterFields)-len(source.fields)), 10),
		strconv.FormatInt(receiverTargetHashBytes(afterFields, afterValues)-receiverTargetHashBytes(source.fields, source.values), 10),
		"0", "", "", "0",
	}
	return keys, operands, nil
}

func (r *RDB) receiverTargetRecoveryCommand(
	ctx context.Context,
	msg *base.TaskMessage,
	encoded []byte,
	input base.ReceiverTargetQueueInitial,
	keys [receiverTargetQueueKeyCount]string,
	operands [receiverTargetQueueOperandCount]string,
) ([receiverTargetQueueKeyCount]string, [receiverTargetQueueOperandCount]string, error) {
	taskExists, err := r.client.Exists(ctx, keys[10]).Result()
	if err != nil || taskExists != 0 {
		return keys, operands, err
	}
	sourceCount, err := r.client.HLen(ctx, keys[9]).Result()
	if err != nil || sourceCount == 0 {
		return keys, operands, err
	}
	source, err := r.receiverTargetFinalizedSource(ctx, keys[9])
	if err != nil {
		return keys, operands, err
	}
	if source.sourceIDDigest != input.SourceIDDigest || source.stateEpoch != input.StateEpoch || source.taskDigest != input.TaskDigest || source.releaseFence != "open" {
		return keys, operands, fmt.Errorf("recovery source mismatch")
	}
	nextGeneration, err := receiverTargetIncrementUint(source.enqueueGeneration)
	if err != nil {
		return keys, operands, err
	}
	nextRevision, err := receiverTargetIncrementUint(source.targetRevision)
	if err != nil {
		return keys, operands, err
	}
	frame := receiverTargetSuccessorSourceID(source, input.EffectID, msg.ID)
	evidence := receiverTargetSuccessorEvidenceDigest(frame, source)
	receipt := receiverTargetOperationReceiptIdentity(source.stateEpoch, input.InstanceTenant, frame, "Apply", "enqueue_fenced", "existing_source_revision")
	keys[5] = "nebpilot:e:" + source.stateEpoch + ":receiver-target:receipt:" + receipt
	keys[6] = "nebpilot:e:" + source.stateEpoch + ":receiver-target:queue-ack:" + receipt

	taskFields := []string{"msg", "state", "sourceIdDigest", "stateEpoch", "enqueueGeneration", "taskDigest"}
	taskValues := []string{string(encoded), operands[8], source.sourceIDDigest, source.stateEpoch, nextGeneration, source.taskDigest}
	if operands[8] == "pending" {
		taskFields = append(taskFields, "pending_since")
		taskValues = append(taskValues, operands[13])
	}
	if receiverTargetHashBytes(taskFields, taskValues) > receiverTargetMaximumTaskHashBytes {
		return keys, operands, fmt.Errorf("marked task hash exceeds maximum")
	}
	afterSourceFields := []string{"sourceIdDigest", "stateEpoch", "enqueueGeneration", "reservedBytes", "taskDigest", "releaseFence", "targetRevision", "sourceEnqueueGeneration", "nativeExpiry", "sourceNativeExpiry", "predecessorAckReceiptDigest", "predecessorAckOperationDigest", "receiptIdentityDigest"}
	afterSourceValues := []string{source.sourceIDDigest, source.stateEpoch, nextGeneration, "4096", source.taskDigest, "open", nextRevision, source.enqueueGeneration, "", source.nativeExpiry, source.ackReceipt, source.ackOperation, receipt}
	score := operands[13]
	if operands[8] == "pending" {
		score, err = r.receiverTargetEndpointScore(ctx, keys[11], true)
		if err != nil {
			return keys, operands, err
		}
	}
	afterRows := int64(len(afterSourceFields) + len(taskFields) + 1)
	afterBytes := receiverTargetHashBytes(afterSourceFields, afterSourceValues) + receiverTargetHashBytes(taskFields, taskValues) + int64(len(msg.ID)+len(score))
	operands[0], operands[1], operands[5], operands[6], operands[7] = frame, evidence, source.targetRevision, receipt, "recovery-enqueue"
	operands[14] = strconv.FormatInt(afterRows-int64(len(source.fields)), 10)
	operands[15] = strconv.FormatInt(afterBytes-receiverTargetHashBytes(source.fields, source.values), 10)
	return keys, operands, nil
}

func (r *RDB) receiverTargetInitialCommand(ctx context.Context, msg *base.TaskMessage, encoded []byte, input base.ReceiverTargetQueueInitial) ([receiverTargetQueueKeyCount]string, [receiverTargetQueueOperandCount]string, error) {
	var keys [receiverTargetQueueKeyCount]string
	var operands [receiverTargetQueueOperandCount]string
	if msg == nil || msg.Queue != base.DefaultQueueName || msg.ID == "" || len(msg.ID) > 256 || len(encoded) == 0 || msg.UniqueKey != "" || msg.GroupKey != "" || msg.Retention != 0 ||
		!receiverTargetPositiveUint(input.RuntimeEpochRevision) || !receiverTargetPositiveUint(input.StateEpoch) || len(input.CatalogGeneration) != 64 || len(input.InstanceTenant) == 0 || len(input.InstanceTenant) > 63 ||
		len(input.EffectID) == 0 || len(input.EffectID) > 128 || !receiverTargetDigest(input.SourceIDDigest) || !receiverTargetDigest(input.TaskDigest) {
		return keys, operands, fmt.Errorf("invalid initial receiver input")
	}
	sourceID := receiverTargetInitialSourceID(input, msg.ID)
	sourceDigest := receiverTargetInitialEvidenceDigest(sourceID, input.TaskDigest)
	receiptIdentity := receiverTargetReceiptIdentity(input.StateEpoch, input.InstanceTenant, sourceID)
	state, next, score, err := r.receiverTargetInitialPlacement(ctx, input.ProcessAt)
	if err != nil {
		return keys, operands, err
	}
	sourceFields := []string{"sourceIdDigest", "stateEpoch", "enqueueGeneration", "reservedBytes", "taskDigest", "releaseFence", "targetRevision", "sourceEnqueueGeneration", "nativeExpiry", "sourceNativeExpiry", "receiptIdentityDigest"}
	sourceValues := []string{input.SourceIDDigest, input.StateEpoch, "1", "4096", input.TaskDigest, "open", "1", "1", "", "", receiptIdentity}
	taskFields := []string{"msg", "state", "sourceIdDigest", "stateEpoch", "enqueueGeneration", "taskDigest"}
	taskValues := []string{string(encoded), state, input.SourceIDDigest, input.StateEpoch, "1", input.TaskDigest}
	if state == "pending" {
		taskFields = append(taskFields, "pending_since")
		taskValues = append(taskValues, next)
	}
	taskBytes := receiverTargetHashBytes(taskFields, taskValues)
	if taskBytes > receiverTargetMaximumTaskHashBytes {
		return keys, operands, fmt.Errorf("marked task hash exceeds maximum")
	}
	rows := int64(len(sourceFields) + len(taskFields) + 1)
	bytes := receiverTargetHashBytes(sourceFields, sourceValues) + taskBytes + int64(len(msg.ID)+len(score))
	epoch := input.StateEpoch
	keys = [receiverTargetQueueKeyCount]string{
		"nebpilot:runtime:epoch",
		"nebpilot:e:" + epoch + ":receiver-target:active",
		"nebpilot:e:" + epoch + ":receiver-target:active-revision",
		"nebpilot:e:" + epoch + ":receiver-target:advancement:queueWakeup",
		"nebpilot:e:" + epoch + ":receiver-target:capacity",
		"nebpilot:e:" + epoch + ":receiver-target:receipt:" + receiptIdentity,
		"nebpilot:e:" + epoch + ":receiver-target:queue-ack:" + receiptIdentity,
		"nebpilot:e:" + epoch + ":effects:body:" + input.EffectID,
		"nebpilot:e:" + epoch + ":effects:receiver-inbox:body:" + input.EffectID,
		"nebpilot:e:" + epoch + ":receiver-target:queue-reservation:" + input.SourceIDDigest + ":" + msg.ID,
		base.TaskKey(base.DefaultQueueName, msg.ID), base.PendingKey(base.DefaultQueueName), base.ActiveKey(base.DefaultQueueName), base.ScheduledKey(base.DefaultQueueName), base.RetryKey(base.DefaultQueueName), base.ArchivedKey(base.DefaultQueueName), base.CompletedKey(base.DefaultQueueName), base.LeaseKey(base.DefaultQueueName), base.AllQueues, base.PausedKey(base.DefaultQueueName), base.ProcessedKey(base.DefaultQueueName, input.ProcessAt), base.ProcessedTotalKey(base.DefaultQueueName), base.FailedKey(base.DefaultQueueName, input.ProcessAt), base.FailedTotalKey(base.DefaultQueueName),
	}
	operands = [receiverTargetQueueOperandCount]string{sourceID, sourceDigest, input.RuntimeEpochRevision, input.CatalogGeneration, input.InstanceTenant, "0", receiptIdentity, "initial-enqueue", state, input.TaskDigest, "", "", string(encoded), next, strconv.FormatInt(rows, 10), strconv.FormatInt(bytes, 10), "0", "", "", "0"}
	return keys, operands, nil
}

func (r *RDB) receiverTargetInitialPlacement(ctx context.Context, processAt time.Time) (state, next, score string, err error) {
	if processAt.After(r.clock.Now()) {
		score = strconv.FormatInt(processAt.Unix(), 10)
		return "scheduled", score, score, nil
	}
	tail, err := r.client.ZRevRangeWithScores(ctx, base.PendingKey(base.DefaultQueueName), 0, 0).Result()
	if err != nil {
		return "", "", "", err
	}
	value := int64(0)
	if len(tail) != 0 {
		if len(tail) != 1 || tail[0].Score != math.Trunc(tail[0].Score) || tail[0].Score < -receiverTargetMaximumQueueScore || tail[0].Score >= receiverTargetMaximumQueueScore {
			return "", "", "", fmt.Errorf("queue score exhausted")
		}
		raw := strconv.FormatInt(int64(tail[0].Score), 10)
		count, countErr := r.client.ZCount(ctx, base.PendingKey(base.DefaultQueueName), raw, raw).Result()
		if countErr != nil || count != 1 {
			return "", "", "", fmt.Errorf("queue score is not unique")
		}
		value = int64(tail[0].Score) + 1
	}
	return "pending", strconv.FormatInt(r.clock.Now().UnixNano(), 10), strconv.FormatInt(value, 10), nil
}

func receiverTargetInitialSourceID(input base.ReceiverTargetQueueInitial, taskID string) string {
	return `{"enqueueGeneration":"1","effectId":` + strconv.Quote(input.EffectID) + `,"kind":"initial","schemaVersion":"queue-wakeup.source.v1","sourceIdDigest":"` + input.SourceIDDigest + `","stateEpoch":"` + input.StateEpoch + `","targetRevision":"0","taskId":` + strconv.Quote(taskID) + `}`
}

func receiverTargetInitialEvidenceDigest(sourceID, taskDigest string) string {
	value := `{"ackOperationDigest":"","ackReceiptDigest":"","releaseFence":"open","reservedBytes":"4096","schemaVersion":"queue-wakeup.initial-evidence.v1","sourceIdentity":` + strconv.Quote(sourceID) + `,"taskDigest":"` + taskDigest + `"}`
	return receiverTargetSHA256(value)
}

func receiverTargetReceiptIdentity(stateEpoch, tenant, sourceID string) string {
	return receiverTargetOperationReceiptIdentity(stateEpoch, tenant, sourceID, "Apply", "enqueue_fenced", "effect_inbox")
}

func receiverTargetOperationReceiptIdentity(stateEpoch, tenant, sourceID, command, variant, sourceKind string) string {
	value := `{"command":` + strconv.Quote(command) + `,"family":"queueWakeup","instanceTenant":` + strconv.Quote(tenant) + `,"schemaVersion":"receiver-target.receipt-identity.v1","sourceId":` + strconv.Quote(sourceID) + `,"sourceKind":` + strconv.Quote(sourceKind) + `,"stateEpoch":` + stateEpoch + `,"variant":` + strconv.Quote(variant) + `}`
	return receiverTargetSHA256(value)
}

func receiverTargetSHA256(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func receiverTargetDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func receiverTargetPositiveUint(value string) bool {
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && parsed != 0 && strconv.FormatUint(parsed, 10) == value
}

func receiverTargetHashBytes(fields, values []string) int64 {
	var total int64
	for i := range fields {
		total += int64(len(fields[i]) + len(values[i]))
	}
	return total
}

func (r *RDB) receiverTargetTaskMarked(ctx context.Context, msg *base.TaskMessage) (bool, error) {
	if msg == nil || msg.Queue != base.DefaultQueueName || msg.ID == "" {
		return false, nil
	}
	marked, err := r.client.HExists(ctx, base.TaskKey(msg.Queue, msg.ID), "sourceIdDigest").Result()
	if err != nil {
		return false, err
	}
	return marked, nil
}

func (r *RDB) applyReceiverTargetNative(ctx context.Context, op errors.Op, msg *base.TaskMessage, command receiverTargetNativeCommand) error {
	task, source, epoch, envelope, keys, err := r.receiverTargetNativeSnapshot(ctx, msg)
	if err != nil {
		return errors.E(op, errors.FailedPrecondition, err)
	}
	if command.nextMessage == "" {
		command.nextMessage = task.msg
	}
	if command.action == "retry" && command.nextMessage == "" {
		return errors.E(op, errors.FailedPrecondition, "missing retry message")
	}
	frame := receiverTargetSuccessorSourceID(source, envelope.EffectID, msg.ID)
	evidence := receiverTargetSuccessorEvidenceDigest(frame, source)
	receipt := receiverTargetOperationReceiptIdentity(source.stateEpoch, epoch[4], frame, "Apply", "transition_task_state", "existing_source_revision")
	keys[5] = "nebpilot:e:" + source.stateEpoch + ":receiver-target:receipt:" + receipt
	keys[6] = "nebpilot:e:" + source.stateEpoch + ":receiver-target:queue-ack:" + receipt
	statisticsDay := command.statisticsDay
	if statisticsDay == "" {
		statisticsDay = r.clock.Now().UTC().Format("2006-01-02")
	}
	keys[20] = base.QueueKeyPrefix(msg.Queue) + "processed:" + statisticsDay
	keys[22] = base.QueueKeyPrefix(msg.Queue) + "failed:" + statisticsDay
	operands := [receiverTargetQueueOperandCount]string{
		frame, evidence, epoch[0], epoch[2], epoch[4], source.targetRevision, receipt,
		command.action, command.expectedState, source.taskDigest, command.expectedLease,
		command.expectedDue, command.nextMessage, command.nextValue, "", "", "0",
		command.statisticsDay, command.nativeExpiry, "0",
	}
	if command.isFailure {
		operands[19] = "1"
	} else {
		operands[19] = "0"
	}
	delta, err := r.receiverTargetNativeDelta(ctx, msg.ID, task, source, receipt, command, keys)
	if err != nil {
		return errors.E(op, errors.FailedPrecondition, err)
	}
	operands[14], operands[15], operands[16] = delta[0], delta[1], delta[2]
	return r.executeReceiverTargetTaskCAS(ctx, op, keys, operands)
}

func (r *RDB) receiverTargetNativeSnapshot(ctx context.Context, msg *base.TaskMessage) (receiverTargetNativeTask, receiverTargetNativeSource, [5]string, receiverTargetQueueEnvelope, [receiverTargetQueueKeyCount]string, error) {
	var task receiverTargetNativeTask
	var source receiverTargetNativeSource
	var epoch [5]string
	var envelope receiverTargetQueueEnvelope
	var keys [receiverTargetQueueKeyCount]string
	if msg == nil || msg.Queue != base.DefaultQueueName || msg.ID == "" || len(msg.ID) > 256 {
		return task, source, epoch, envelope, keys, fmt.Errorf("invalid marked task coordinate")
	}
	taskKey := base.TaskKey(msg.Queue, msg.ID)
	taskFields := []string{"msg", "state", "sourceIdDigest", "stateEpoch", "enqueueGeneration", "taskDigest"}
	count, err := r.client.HLen(ctx, taskKey).Result()
	if err != nil || count != 6 && count != 7 {
		return task, source, epoch, envelope, keys, fmt.Errorf("invalid marked task shape")
	}
	if count == 7 {
		taskFields = append(taskFields, "pending_since")
	}
	taskValues, err := receiverTargetHMGetStrings(ctx, r.client, taskKey, taskFields)
	if err != nil {
		return task, source, epoch, envelope, keys, fmt.Errorf("read marked task: %w", err)
	}
	task = receiverTargetNativeTask{fields: taskFields, values: taskValues, msg: taskValues[0], state: taskValues[1]}
	if !receiverTargetDigest(taskValues[2]) || !receiverTargetPositiveUint(taskValues[3]) || !receiverTargetPositiveUint(taskValues[4]) || !receiverTargetDigest(taskValues[5]) || receiverTargetHashBytes(taskFields, taskValues) > receiverTargetMaximumTaskHashBytes {
		return task, source, epoch, envelope, keys, fmt.Errorf("invalid marked task values")
	}
	decoded, err := base.DecodeMessage([]byte(task.msg))
	if err != nil || decoded.ID != msg.ID || decoded.Queue != msg.Queue {
		return task, source, epoch, envelope, keys, fmt.Errorf("invalid marked task message")
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil || envelope.SchemaVersion != "nebpilot.queue-effect-task.v1" || envelope.EffectID == "" || len(envelope.EffectID) > 128 || !receiverTargetPositiveUint(envelope.SourceRevision) || len(envelope.TaskPayload) == 0 || len(envelope.TaskPayload) > 512 || !receiverTargetDigest(envelope.TaskPayloadDigest) || receiverTargetSHA256(string(envelope.TaskPayload)) != envelope.TaskPayloadDigest {
		return task, source, epoch, envelope, keys, fmt.Errorf("invalid marked task envelope")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return task, source, epoch, envelope, keys, fmt.Errorf("invalid marked task envelope")
	}
	epochFields := []string{"revision", "stateEpoch", "catalogGeneration", "admissionState", "instanceTenant"}
	if n, readErr := r.client.HLen(ctx, "nebpilot:runtime:epoch").Result(); readErr != nil || n != int64(len(epochFields)) {
		return task, source, epoch, envelope, keys, fmt.Errorf("invalid runtime epoch shape")
	}
	epochValues, err := receiverTargetHMGetStrings(ctx, r.client, "nebpilot:runtime:epoch", epochFields)
	if err != nil {
		return task, source, epoch, envelope, keys, fmt.Errorf("read runtime epoch: %w", err)
	}
	copy(epoch[:], epochValues)
	if !receiverTargetPositiveUint(epoch[0]) || epoch[1] != taskValues[3] || !receiverTargetDigest(epoch[2]) || epoch[3] != "open" || epoch[4] == "" || len(epoch[4]) > 63 {
		return task, source, epoch, envelope, keys, fmt.Errorf("runtime epoch mismatch")
	}
	sourceKey := "nebpilot:e:" + taskValues[3] + ":receiver-target:queue-reservation:" + taskValues[2] + ":" + msg.ID
	source, err = r.receiverTargetFinalizedSource(ctx, sourceKey)
	if err != nil {
		return task, source, epoch, envelope, keys, err
	}
	if source.sourceIDDigest != taskValues[2] || source.stateEpoch != taskValues[3] || source.enqueueGeneration != taskValues[4] || source.taskDigest != taskValues[5] {
		return task, source, epoch, envelope, keys, fmt.Errorf("finalized source mismatch")
	}
	epochID := source.stateEpoch
	keys = [receiverTargetQueueKeyCount]string{
		"nebpilot:runtime:epoch", "nebpilot:e:" + epochID + ":receiver-target:active", "nebpilot:e:" + epochID + ":receiver-target:active-revision", "nebpilot:e:" + epochID + ":receiver-target:advancement:queueWakeup", "nebpilot:e:" + epochID + ":receiver-target:capacity",
		"", "", "nebpilot:e:" + epochID + ":effects:body:" + envelope.EffectID, "nebpilot:e:" + epochID + ":effects:receiver-inbox:body:" + envelope.EffectID, sourceKey,
		taskKey, base.PendingKey(msg.Queue), base.ActiveKey(msg.Queue), base.ScheduledKey(msg.Queue), base.RetryKey(msg.Queue), base.ArchivedKey(msg.Queue), base.CompletedKey(msg.Queue), base.LeaseKey(msg.Queue), base.AllQueues, base.PausedKey(msg.Queue), "", base.ProcessedTotalKey(msg.Queue), "", base.FailedTotalKey(msg.Queue),
	}
	return task, source, epoch, envelope, keys, nil
}

func (r *RDB) receiverTargetFinalizedSource(ctx context.Context, sourceKey string) (receiverTargetNativeSource, error) {
	var source receiverTargetNativeSource
	sourceCount, err := r.client.HLen(ctx, sourceKey).Result()
	if err != nil || sourceCount != 12 && sourceCount != 14 {
		return source, fmt.Errorf("invalid finalized source shape")
	}
	sourceFields := []string{"sourceIdDigest", "stateEpoch", "enqueueGeneration", "reservedBytes", "taskDigest", "releaseFence", "targetRevision", "sourceEnqueueGeneration", "nativeExpiry", "sourceNativeExpiry"}
	if sourceCount == 14 {
		sourceFields = append(sourceFields, "predecessorAckReceiptDigest", "predecessorAckOperationDigest")
	}
	sourceFields = append(sourceFields, "ackReceiptDigest", "ackOperationDigest")
	sourceValues, err := receiverTargetHMGetStrings(ctx, r.client, sourceKey, sourceFields)
	if err != nil {
		return source, fmt.Errorf("read finalized source: %w", err)
	}
	source = receiverTargetNativeSource{
		fields: sourceFields, values: sourceValues, sourceIDDigest: sourceValues[0], stateEpoch: sourceValues[1], enqueueGeneration: sourceValues[2],
		taskDigest: sourceValues[4], releaseFence: sourceValues[5], targetRevision: sourceValues[6], nativeExpiry: sourceValues[8],
		ackReceipt: sourceValues[len(sourceValues)-2], ackOperation: sourceValues[len(sourceValues)-1],
	}
	if !receiverTargetDigest(source.sourceIDDigest) || !receiverTargetPositiveUint(source.stateEpoch) || !receiverTargetPositiveUint(source.enqueueGeneration) || source.values[3] != "4096" || !receiverTargetDigest(source.taskDigest) || source.releaseFence != "open" || !receiverTargetPositiveUint(source.targetRevision) || !receiverTargetPositiveUint(source.values[7]) || !receiverTargetOptionalPositiveUint(source.nativeExpiry) || !receiverTargetOptionalPositiveUint(source.values[9]) || !receiverTargetDigest(source.ackReceipt) || !receiverTargetDigest(source.ackOperation) {
		return source, fmt.Errorf("invalid finalized source values")
	}
	if sourceCount == 14 && (!receiverTargetDigest(sourceValues[10]) || !receiverTargetDigest(sourceValues[11])) {
		return source, fmt.Errorf("invalid predecessor acknowledgement")
	}
	return source, nil
}

func receiverTargetHMGetStrings(ctx context.Context, client redis.UniversalClient, key string, fields []string) ([]string, error) {
	raw, err := client.HMGet(ctx, key, fields...).Result()
	if err != nil {
		return nil, err
	}
	values := make([]string, len(raw))
	for i, value := range raw {
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("missing field %s", fields[i])
		}
		values[i] = text
	}
	return values, nil
}

func receiverTargetSuccessorSourceID(source receiverTargetNativeSource, effectID, taskID string) string {
	return `{"enqueueGeneration":` + strconv.Quote(source.enqueueGeneration) + `,"effectId":` + strconv.Quote(effectID) + `,"kind":"successor","schemaVersion":"queue-wakeup.source.v1","sourceIdDigest":"` + source.sourceIDDigest + `","stateEpoch":` + strconv.Quote(source.stateEpoch) + `,"targetRevision":` + strconv.Quote(source.targetRevision) + `,"taskId":` + strconv.Quote(taskID) + `}`
}

func receiverTargetSuccessorEvidenceDigest(sourceID string, source receiverTargetNativeSource) string {
	value := `{"ackOperationDigest":"` + source.ackOperation + `","ackReceiptDigest":"` + source.ackReceipt + `","releaseFence":"open","reservedBytes":"4096","schemaVersion":"queue-wakeup.successor-evidence.v1","sourceIdentity":` + strconv.Quote(sourceID) + `,"taskDigest":"` + source.taskDigest + `"}`
	return receiverTargetSHA256(value)
}

func (r *RDB) receiverTargetNativeDelta(ctx context.Context, taskID string, task receiverTargetNativeTask, source receiverTargetNativeSource, receipt string, command receiverTargetNativeCommand, keys [receiverTargetQueueKeyCount]string) ([3]string, error) {
	var delta [3]string
	beforeRows := int64(len(task.fields) + len(source.fields))
	beforeBytes := receiverTargetHashBytes(task.fields, task.values) + receiverTargetHashBytes(source.fields, source.values)
	members := make(map[int]string, 7)
	for slot := 11; slot <= 17; slot++ {
		score, err := r.client.ZScore(ctx, keys[slot], taskID).Result()
		if err == nil {
			if score != math.Trunc(score) || score < -receiverTargetMaximumQueueScore || score > receiverTargetMaximumQueueScore {
				return delta, fmt.Errorf("invalid marked membership score")
			}
			members[slot] = strconv.FormatInt(int64(score), 10)
			beforeRows++
			beforeBytes += int64(len(taskID) + len(members[slot]))
		} else if err != redis.Nil {
			return delta, err
		}
	}
	afterFields := append([]string(nil), task.fields...)
	afterValues := append([]string(nil), task.values...)
	setTask := func(name, value string) {
		for i := range afterFields {
			if afterFields[i] == name {
				afterValues[i] = value
				return
			}
		}
		afterFields = append(afterFields, name)
		afterValues = append(afterValues, value)
	}
	removeTask := func(name string) {
		for i := range afterFields {
			if afterFields[i] == name {
				afterFields = append(afterFields[:i], afterFields[i+1:]...)
				afterValues = append(afterValues[:i], afterValues[i+1:]...)
				return
			}
		}
	}
	switch command.action {
	case "dequeue":
		if members[11] != command.expectedDue {
			return delta, fmt.Errorf("pending membership mismatch")
		}
		next, err := r.receiverTargetEndpointScore(ctx, keys[12], true)
		if err != nil {
			return delta, err
		}
		delete(members, 11)
		members[12], members[17] = next, command.nextValue
		setTask("state", "active")
		removeTask("pending_since")
	case "requeue":
		if members[12] == "" || members[17] != command.expectedLease {
			return delta, fmt.Errorf("active membership mismatch")
		}
		next, err := r.receiverTargetEndpointScore(ctx, keys[11], false)
		if err != nil {
			return delta, err
		}
		delete(members, 12)
		delete(members, 17)
		members[11] = next
		setTask("state", "pending")
		setTask("pending_since", command.nextValue)
	case "lease-extension":
		if members[12] == "" || members[17] != command.expectedLease {
			return delta, fmt.Errorf("lease membership mismatch")
		}
		members[17] = command.nextValue
	case "due-forward":
		slot := 13
		if command.expectedState == "retry" {
			slot = 14
		}
		if members[slot] != command.expectedDue {
			return delta, fmt.Errorf("due membership mismatch")
		}
		next, err := r.receiverTargetEndpointScore(ctx, keys[11], true)
		if err != nil {
			return delta, err
		}
		delete(members, slot)
		members[11] = next
		setTask("state", "pending")
		setTask("pending_since", command.nextValue)
	case "retry":
		if members[12] == "" || members[17] != command.expectedLease {
			return delta, fmt.Errorf("retry membership mismatch")
		}
		delete(members, 12)
		delete(members, 17)
		members[14] = command.nextValue
		setTask("state", "retry")
		setTask("msg", command.nextMessage)
		removeTask("pending_since")
	case "done":
		if members[12] == "" || members[17] != command.expectedLease {
			return delta, fmt.Errorf("done membership mismatch")
		}
		afterFields, afterValues = nil, nil
		delete(members, 12)
		delete(members, 17)
	default:
		return delta, fmt.Errorf("unsupported marked action")
	}
	nextRevision, err := receiverTargetIncrementUint(source.targetRevision)
	if err != nil {
		return delta, err
	}
	afterSourceFields := []string{"sourceIdDigest", "stateEpoch", "enqueueGeneration", "reservedBytes", "taskDigest", "releaseFence", "targetRevision", "sourceEnqueueGeneration", "nativeExpiry", "sourceNativeExpiry", "predecessorAckReceiptDigest", "predecessorAckOperationDigest", "receiptIdentityDigest"}
	afterSourceValues := []string{source.sourceIDDigest, source.stateEpoch, source.enqueueGeneration, "4096", source.taskDigest, "open", nextRevision, source.enqueueGeneration, command.nativeExpiry, source.nativeExpiry, source.ackReceipt, source.ackOperation, receipt}
	afterRows := int64(len(afterFields) + len(afterSourceFields) + len(members))
	afterBytes := receiverTargetHashBytes(afterFields, afterValues) + receiverTargetHashBytes(afterSourceFields, afterSourceValues)
	for _, score := range members {
		afterBytes += int64(len(taskID) + len(score))
	}
	delta[0] = strconv.FormatInt(afterRows-beforeRows, 10)
	delta[1] = strconv.FormatInt(afterBytes-beforeBytes, 10)
	delta[2] = "0"
	return delta, nil
}

func (r *RDB) receiverTargetEndpointScore(ctx context.Context, key string, appendScore bool) (string, error) {
	var values []redis.Z
	var err error
	if appendScore {
		values, err = r.client.ZRevRangeWithScores(ctx, key, 0, 0).Result()
	} else {
		values, err = r.client.ZRangeWithScores(ctx, key, 0, 0).Result()
	}
	if err != nil {
		return "", err
	}
	if len(values) == 0 {
		return "0", nil
	}
	score := values[0].Score
	if score != math.Trunc(score) || score < -receiverTargetMaximumQueueScore || score > receiverTargetMaximumQueueScore {
		return "", fmt.Errorf("queue score exhausted")
	}
	raw := strconv.FormatInt(int64(score), 10)
	count, err := r.client.ZCount(ctx, key, raw, raw).Result()
	if err != nil || count != 1 {
		return "", fmt.Errorf("queue score is not unique")
	}
	if appendScore {
		if score == receiverTargetMaximumQueueScore {
			return "", fmt.Errorf("queue score exhausted")
		}
		score++
	} else {
		if score == -receiverTargetMaximumQueueScore {
			return "", fmt.Errorf("queue score exhausted")
		}
		score--
	}
	return strconv.FormatInt(int64(score), 10), nil
}

func (r *RDB) receiverTargetMembershipScore(ctx context.Context, key, taskID string) (string, error) {
	score, err := r.client.ZScore(ctx, key, taskID).Result()
	if err != nil {
		return "", fmt.Errorf("read marked membership: %w", err)
	}
	if score != math.Trunc(score) || score < -receiverTargetMaximumQueueScore || score > receiverTargetMaximumQueueScore {
		return "", fmt.Errorf("invalid marked membership score")
	}
	return strconv.FormatInt(int64(score), 10), nil
}

func receiverTargetIncrementUint(value string) (string, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == math.MaxUint64 {
		return "", fmt.Errorf("target revision exhausted")
	}
	return strconv.FormatUint(parsed+1, 10), nil
}

func receiverTargetOptionalPositiveUint(value string) bool {
	return value == "" || receiverTargetPositiveUint(value)
}

// applyReceiverTargetTaskCAS is the sole invocation boundary for the generated
// receiver transaction. Fixed arrays keep the generated key and operand grammar
// exact at every caller.
func (r *RDB) applyReceiverTargetTaskCAS(
	ctx context.Context,
	op errors.Op,
	keys [receiverTargetQueueKeyCount]string,
	operands [receiverTargetQueueOperandCount]string,
) (receiverTargetTaskCASResult, error) {
	args := make([]interface{}, len(operands))
	for i := range operands {
		args[i] = operands[i]
	}
	value, err := receiverTargetQueueCmd.Run(ctx, r.client, keys[:], args...).Result()
	if err != nil {
		return receiverTargetTaskCASResult{}, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}
	return parseReceiverTargetTaskCASResult(op, value)
}

func parseReceiverTargetTaskCASResult(op errors.Op, value interface{}) (receiverTargetTaskCASResult, error) {
	items, ok := value.([]interface{})
	if !ok || len(items) < 1 {
		return receiverTargetTaskCASResult{}, errors.E(op, errors.Internal, fmt.Sprintf("unexpected receiver transaction result: %v", value))
	}
	status, ok := items[0].(string)
	if !ok {
		return receiverTargetTaskCASResult{}, errors.E(op, errors.Internal, fmt.Sprintf("unexpected receiver transaction result: %v", value))
	}
	if status == "refused" {
		if len(items) != 2 {
			return receiverTargetTaskCASResult{}, errors.E(op, errors.Internal, fmt.Sprintf("unexpected receiver transaction refusal: %v", value))
		}
		reason, ok := items[1].(string)
		if !ok || reason == "" {
			return receiverTargetTaskCASResult{}, errors.E(op, errors.Internal, fmt.Sprintf("unexpected receiver transaction refusal: %v", value))
		}
		if reason == "target-revision" {
			return receiverTargetTaskCASResult{}, errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
		}
		return receiverTargetTaskCASResult{}, errors.E(op, errors.FailedPrecondition, reason)
	}
	if status != "ok" || len(items) != 4 {
		return receiverTargetTaskCASResult{}, errors.E(op, errors.Internal, fmt.Sprintf("unexpected receiver transaction result: %v", value))
	}
	targetRevision, revisionOK := items[1].(string)
	resultPayload, payloadOK := items[2].(string)
	replay, replayOK := items[3].(string)
	if !revisionOK || targetRevision == "" || !payloadOK || resultPayload == "" || !replayOK || replay != "0" && replay != "1" {
		return receiverTargetTaskCASResult{}, errors.E(op, errors.Internal, fmt.Sprintf("unexpected receiver transaction result: %v", value))
	}
	return receiverTargetTaskCASResult{
		targetRevision: targetRevision,
		resultPayload:  resultPayload,
		replayed:       replay == "1",
	}, nil
}

var receiverTargetEnqueueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
local tail = redis.call("ZREVRANGE", KEYS[2], 0, 0, "WITHSCORES")
local score = 0
if #tail ~= 0 then
	score = tonumber(tail[2])
	if score == nil or score % 1 ~= 0 or
	   redis.call("ZCOUNT", KEYS[2], score, score) ~= 1 or
	   score < -tonumber(ARGV[4]) or score >= tonumber(ARGV[4]) then
		return redis.error_reply("QUEUE SCORE EXHAUSTED")
	end
	score = score + 1
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "pending",
           "pending_since", ARGV[3])
redis.call("ZADD", KEYS[2], score, ARGV[2])
return 1
`)

var receiverTargetEnqueueUniqueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
	return 0
end
local tail = redis.call("ZREVRANGE", KEYS[3], 0, 0, "WITHSCORES")
local score = 0
if #tail ~= 0 then
	score = tonumber(tail[2])
	if score == nil or score % 1 ~= 0 or
	   redis.call("ZCOUNT", KEYS[3], score, score) ~= 1 or
	   score < -tonumber(ARGV[5]) or score >= tonumber(ARGV[5]) then
		return redis.error_reply("QUEUE SCORE EXHAUSTED")
	end
	score = score + 1
end
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
redis.call("HSET", KEYS[2],
           "msg", ARGV[3],
           "state", "pending",
           "pending_since", ARGV[4],
           "unique_key", KEYS[1])
redis.call("ZADD", KEYS[3], score, ARGV[1])
return 1
`)

var receiverTargetDequeueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) ~= 0 then
	return nil
end
local head = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
if #head == 0 then
	return nil
end
local id = head[1]
local pendingScore = tonumber(head[2])
if pendingScore == nil or pendingScore % 1 ~= 0 or
   pendingScore < -tonumber(ARGV[3]) or pendingScore > tonumber(ARGV[3]) or
   redis.call("ZCOUNT", KEYS[1], pendingScore, pendingScore) ~= 1 then
	return redis.error_reply("QUEUE SCORE INVALID")
end
local tail = redis.call("ZREVRANGE", KEYS[3], 0, 0, "WITHSCORES")
local activeScore = 0
if #tail ~= 0 then
	activeScore = tonumber(tail[2])
	if activeScore == nil or activeScore % 1 ~= 0 or
	   redis.call("ZCOUNT", KEYS[3], activeScore, activeScore) ~= 1 or
	   activeScore < -tonumber(ARGV[3]) or activeScore >= tonumber(ARGV[3]) then
		return redis.error_reply("QUEUE SCORE EXHAUSTED")
	end
	activeScore = activeScore + 1
end
local key = ARGV[2] .. id
local message = redis.call("HGET", key, "msg")
if not message then
	return redis.error_reply("TASK MESSAGE NOT FOUND")
end
if redis.call("HEXISTS", key, "sourceIdDigest") == 1 then
	return {"receiver-target", id, tostring(pendingScore), message}
end
redis.call("ZREM", KEYS[1], id)
redis.call("ZADD", KEYS[3], activeScore, id)
redis.call("HSET", key, "state", "active")
redis.call("HDEL", key, "pending_since")
redis.call("ZADD", KEYS[4], ARGV[1], id)
return message
`)

var receiverTargetRequeueCmd = redis.NewScript(`
if redis.call("ZSCORE", KEYS[1], ARGV[1]) == false then
	return redis.error_reply("NOT FOUND")
end
if redis.call("ZSCORE", KEYS[2], ARGV[1]) == false then
	return redis.error_reply("NOT FOUND")
end
local head = redis.call("ZRANGE", KEYS[3], 0, 0, "WITHSCORES")
local score = 0
if #head ~= 0 then
	score = tonumber(head[2])
	if score == nil or score % 1 ~= 0 or
	   redis.call("ZCOUNT", KEYS[3], score, score) ~= 1 or
	   score <= -tonumber(ARGV[2]) or score > tonumber(ARGV[2]) then
		return redis.error_reply("QUEUE SCORE EXHAUSTED")
	end
	score = score - 1
end
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("ZADD", KEYS[3], score, ARGV[1])
redis.call("HSET", KEYS[4], "state", "pending")
return redis.status_reply("OK")
`)

var receiverTargetForwardCmd = redis.NewScript(`
local selected = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "WITHSCORES", "LIMIT", 0, 1)
if #selected == 0 then return 0 end
local id = selected[1]
local sourceScore = tonumber(selected[2])
if sourceScore == nil or sourceScore % 1 ~= 0 or sourceScore < -tonumber(ARGV[5]) or sourceScore > tonumber(ARGV[5]) then
	return redis.error_reply("QUEUE SCORE INVALID")
end
local taskKey = ARGV[2] .. id
local group = redis.call("HGET", taskKey, "group") or ""
if redis.call("HEXISTS", taskKey, "sourceIdDigest") == 1 then
	if group ~= "" then return redis.error_reply("RECEIVER TARGET GROUP UNSUPPORTED") end
	local message = redis.call("HGET", taskKey, "msg")
	if not message then return redis.error_reply("TASK MESSAGE NOT FOUND") end
	return {"receiver-target", id, tostring(sourceScore), message}
end
local tail = redis.call("ZREVRANGE", KEYS[2], 0, 0, "WITHSCORES")
local score = -1
if #tail ~= 0 then
	score = tonumber(tail[2])
	if score == nil or score % 1 ~= 0 or
	   redis.call("ZCOUNT", KEYS[2], score, score) ~= 1 or
	   score < -tonumber(ARGV[5]) or score > tonumber(ARGV[5]) then
		return redis.error_reply("QUEUE SCORE INVALID")
	end
end
if group ~= "" then
	redis.call("ZADD", ARGV[4] .. group, ARGV[1], id)
	redis.call("ZREM", KEYS[1], id)
	redis.call("HSET", taskKey, "state", "aggregating")
	return 1
end
if score >= tonumber(ARGV[5]) then return redis.error_reply("QUEUE SCORE EXHAUSTED") end
score = score + 1
redis.call("ZADD", KEYS[2], score, id)
redis.call("ZREM", KEYS[1], id)
redis.call("HSET", taskKey, "state", "pending", "pending_since", ARGV[3])
return 1
`)

var receiverTargetRunTaskCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then return 0 end
if redis.call("HEXISTS", KEYS[1], "sourceIdDigest") == 1 then
	return redis.error_reply("RECEIVER TARGET INSPECTOR MUTATION UNSUPPORTED")
end
local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
if state == "active" then return -1 end
if state == "pending" then return -2 end
local tail = redis.call("ZREVRANGE", KEYS[2], 0, 0, "WITHSCORES")
local score = 0
if #tail ~= 0 then
	score = tonumber(tail[2])
	if score == nil or score % 1 ~= 0 or
	   redis.call("ZCOUNT", KEYS[2], score, score) ~= 1 or
	   score < -tonumber(ARGV[4]) or score >= tonumber(ARGV[4]) then
		return redis.error_reply("QUEUE SCORE EXHAUSTED")
	end
	score = score + 1
end
if state == "aggregating" then
	local groupKey = ARGV[3] .. group
	if redis.call("ZREM", groupKey, ARGV[1]) == 0 then
		return redis.error_reply("TASK MEMBERSHIP NOT FOUND")
	end
	if redis.call("ZCARD", groupKey) == 0 then redis.call("SREM", KEYS[3], group) end
elseif redis.call("ZREM", ARGV[2] .. state, ARGV[1]) == 0 then
	return redis.error_reply("TASK MEMBERSHIP NOT FOUND")
end
redis.call("ZADD", KEYS[2], score, ARGV[1])
redis.call("HSET", KEYS[1], "state", "pending")
return 1
`)

var receiverTargetRunAllCmd = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	if redis.call("HEXISTS", ARGV[1] .. id, "sourceIdDigest") == 1 then
		return redis.error_reply("RECEIVER TARGET INSPECTOR MUTATION UNSUPPORTED")
	end
end
local tail = redis.call("ZREVRANGE", KEYS[2], 0, 0, "WITHSCORES")
local score = -1
if #tail ~= 0 then
	score = tonumber(tail[2])
	if score == nil or score % 1 ~= 0 or
	   redis.call("ZCOUNT", KEYS[2], score, score) ~= 1 or
	   score < -tonumber(ARGV[2]) or score > tonumber(ARGV[2]) then
		return redis.error_reply("QUEUE SCORE INVALID")
	end
end
if #ids > 0 and score > tonumber(ARGV[2]) - #ids then
	return redis.error_reply("QUEUE SCORE EXHAUSTED")
end
for _, id in ipairs(ids) do
	score = score + 1
	redis.call("ZADD", KEYS[2], score, id)
	redis.call("HSET", ARGV[1] .. id, "state", "pending")
end
redis.call("DEL", KEYS[1])
return table.getn(ids)
`)

var receiverTargetRunAllAggregatingCmd = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	if redis.call("HEXISTS", ARGV[1] .. id, "sourceIdDigest") == 1 then
		return redis.error_reply("RECEIVER TARGET INSPECTOR MUTATION UNSUPPORTED")
	end
end
local tail = redis.call("ZREVRANGE", KEYS[2], 0, 0, "WITHSCORES")
local score = -1
if #tail ~= 0 then
	score = tonumber(tail[2])
	if score == nil or score % 1 ~= 0 or
	   redis.call("ZCOUNT", KEYS[2], score, score) ~= 1 or
	   score < -tonumber(ARGV[3]) or score > tonumber(ARGV[3]) then
		return redis.error_reply("QUEUE SCORE INVALID")
	end
end
if #ids > 0 and score > tonumber(ARGV[3]) - #ids then
	return redis.error_reply("QUEUE SCORE EXHAUSTED")
end
for _, id in ipairs(ids) do
	score = score + 1
	redis.call("ZADD", KEYS[2], score, id)
	redis.call("HSET", ARGV[1] .. id, "state", "pending")
end
redis.call("DEL", KEYS[1])
redis.call("SREM", KEYS[3], ARGV[2])
return table.getn(ids)
`)
