package rdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nebinfra/asynq/internal/base"
	"github.com/nebinfra/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

const receiverTargetMaximumQueueScore = 9007199254740991

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

func (r *RDB) receiverTargetInitialCommand(ctx context.Context, msg *base.TaskMessage, encoded []byte, input base.ReceiverTargetQueueInitial) ([receiverTargetQueueKeyCount]string, [receiverTargetQueueOperandCount]string, error) {
	var keys [receiverTargetQueueKeyCount]string
	var operands [receiverTargetQueueOperandCount]string
	if msg == nil || msg.Queue != base.DefaultQueueName || msg.ID == "" || len(msg.ID) > 256 || len(encoded) == 0 || len(encoded) > 271559 ||
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
	rows := int64(len(sourceFields) + len(taskFields) + 1)
	bytes := receiverTargetHashBytes(sourceFields, sourceValues) + receiverTargetHashBytes(taskFields, taskValues) + int64(len(msg.ID)+len(score))
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
	value := `{"command":"Apply","family":"queueWakeup","instanceTenant":` + strconv.Quote(tenant) + `,"schemaVersion":"receiver-target.receipt-identity.v1","sourceId":` + strconv.Quote(sourceID) + `,"sourceKind":"effect_inbox","stateEpoch":` + stateEpoch + `,"variant":"enqueue_fenced"}`
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
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
local groups = {}
local pendingCount = 0
for index, id in ipairs(ids) do
	local group = redis.call("HGET", ARGV[2] .. id, "group")
	groups[index] = group or ""
	if not group or group == "" then
		pendingCount = pendingCount + 1
	end
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
if pendingCount > 0 and score > tonumber(ARGV[5]) - pendingCount then
	return redis.error_reply("QUEUE SCORE EXHAUSTED")
end
for index, id in ipairs(ids) do
	local taskKey = ARGV[2] .. id
	local group = groups[index]
	if group ~= "" then
		redis.call("ZADD", ARGV[4] .. group, ARGV[1], id)
		redis.call("ZREM", KEYS[1], id)
		redis.call("HSET", taskKey, "state", "aggregating")
	else
		score = score + 1
		redis.call("ZADD", KEYS[2], score, id)
		redis.call("ZREM", KEYS[1], id)
		redis.call("HSET", taskKey,
		           "state", "pending",
		           "pending_since", ARGV[3])
	end
end
return table.getn(ids)
`)

var receiverTargetRunTaskCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then return 0 end
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
