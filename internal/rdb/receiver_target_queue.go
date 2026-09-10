package rdb

import (
	"context"
	"fmt"

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
