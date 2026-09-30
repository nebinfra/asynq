package rdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestReceiverTargetMutationMatrix(t *testing.T) {
	files := parseReceiverTargetPackage(t)
	var callers []string
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				identifier, identifierOK := selectorXIdent(selector)
				if ok && identifierOK && selector.Sel.Name == "Run" && identifier == "receiverTargetQueueCmd" {
					callers = append(callers, function.Name.Name)
				}
				return true
			})
		}
	}
	if got, want := strings.Join(callers, ","), "applyReceiverTargetTaskCAS"; got != want {
		t.Fatalf("generated transaction callers = %q, want %q", got, want)
	}
}

func TestReceiverTargetUnsupportedMutationMatrix(t *testing.T) {
	files := parseReceiverTargetPackage(t)
	scripts := receiverTargetScriptSources(t, files)
	classes := map[string][]string{
		"generated-cas":                 {"receiverTargetQueueCmd"},
		"marked-selection-before-write": {"receiverTargetDequeueCmd", "receiverTargetForwardCmd"},
		"marked-refusal-before-write":   {"archiveCmd", "archiveAllAggregatingCmd", "archiveAllPendingCmd", "archiveTaskCmd", "archiveAllCmd", "updateTaskPayloadCmd", "deleteTaskCmd", "deleteAllCmd", "deleteAllAggregatingCmd", "deleteAllPendingCmd", "removeQueueForceCmd", "writeResultCmd", "deleteExpiredCompletedTasksCmd", "receiverTargetRunTaskCmd", "receiverTargetRunAllCmd", "receiverTargetRunAllAggregatingCmd"},
		"go-marker-guarded":             {"doneCmd", "doneUniqueCmd", "markAsCompleteCmd", "markAsCompleteUniqueCmd", "requeueCmd", "retryCmd", "receiverTargetRequeueCmd"},
		"cannot-create-marker":          {"enqueueCmd", "enqueueUniqueCmd", "addToGroupCmd", "addToGroupUniqueCmd", "scheduleCmd", "scheduleUniqueCmd", "receiverTargetEnqueueCmd", "receiverTargetEnqueueUniqueCmd"},
		"ordinary-route-only":           {"dequeueCmd", "forwardCmd", "runAllAggregatingCmd", "runTaskCmd", "runAllCmd"},
		"empty-only":                    {"removeQueueCmd"},
		"unrelated-metadata":            {"aggregationCheckCmd", "deleteAggregationSetCmd", "reclaimStateAggregationSetsCmd", "writeServerStateCmd", "clearServerStateCmd", "writeSchedulerEntriesCmd", "recordSchedulerEnqueueEventCmd", "listServerKeysCmd", "listWorkersCmd", "listSchedulerKeysCmd"},
		"read-only":                     {"currentStatsCmd", "memoryUsageCmd", "historicalStatsCmd", "getTaskInfoCmd", "groupStatsCmd", "listMessagesCmd", "listOrderedMessagesCmd", "listZSetEntriesCmd", "readAggregationSetCmd", "listLeaseExpiredCmd"},
	}
	classified := make(map[string]string)
	for class, names := range classes {
		for _, name := range names {
			if prior := classified[name]; prior != "" {
				t.Fatalf("script %s appears in %s and %s", name, prior, class)
			}
			classified[name] = class
		}
	}
	if !reflect.DeepEqual(sortedKeys(scripts), sortedKeys(classified)) {
		t.Fatalf("script inventory mismatch\ndiscovered: %v\nclassified: %v", sortedKeys(scripts), sortedKeys(classified))
	}
	wantScriptOwners := map[string]string{
		"addToGroupCmd": "AddToGroup", "addToGroupUniqueCmd": "AddToGroupUnique", "aggregationCheckCmd": "AggregationCheck",
		"archiveCmd": "Archive", "archiveAllAggregatingCmd": "ArchiveAllAggregatingTasks", "archiveAllPendingCmd": "ArchiveAllPendingTasks",
		"archiveTaskCmd": "ArchiveTask", "archiveAllCmd": "archiveAll", "clearServerStateCmd": "ClearServerState",
		"currentStatsCmd": "CurrentStats", "deleteAggregationSetCmd": "DeleteAggregationSet", "deleteAllAggregatingCmd": "DeleteAllAggregatingTasks",
		"deleteAllPendingCmd": "DeleteAllPendingTasks", "deleteTaskCmd": "DeleteTask", "deleteAllCmd": "deleteAll",
		"deleteExpiredCompletedTasksCmd": "deleteExpiredCompletedTasks", "dequeueCmd": "Dequeue", "receiverTargetDequeueCmd": "Dequeue",
		"doneCmd": "Done", "doneUniqueCmd": "Done", "enqueueCmd": "Enqueue", "receiverTargetEnqueueCmd": "Enqueue",
		"enqueueUniqueCmd": "EnqueueUnique", "receiverTargetEnqueueUniqueCmd": "EnqueueUnique", "getTaskInfoCmd": "GetTaskInfo",
		"groupStatsCmd": "GroupStats", "historicalStatsCmd": "HistoricalStats", "listLeaseExpiredCmd": "ListLeaseExpired",
		"listSchedulerKeysCmd": "ListSchedulerEntries", "listServerKeysCmd": "ListServers", "listWorkersCmd": "ListWorkers",
		"markAsCompleteCmd": "MarkAsComplete", "markAsCompleteUniqueCmd": "MarkAsComplete", "readAggregationSetCmd": "ReadAggregationSet",
		"reclaimStateAggregationSetsCmd": "ReclaimStaleAggregationSets", "recordSchedulerEnqueueEventCmd": "RecordSchedulerEnqueueEvent",
		"removeQueueCmd": "RemoveQueue", "removeQueueForceCmd": "RemoveQueue", "receiverTargetRequeueCmd": "Requeue", "requeueCmd": "Requeue",
		"retryCmd": "Retry", "receiverTargetRunAllAggregatingCmd": "RunAllAggregatingTasks", "runAllAggregatingCmd": "RunAllAggregatingTasks",
		"receiverTargetRunTaskCmd": "RunTask", "runTaskCmd": "RunTask", "scheduleCmd": "Schedule", "scheduleUniqueCmd": "ScheduleUnique",
		"updateTaskPayloadCmd": "UpdateTaskPayload", "writeResultCmd": "WriteResult", "writeSchedulerEntriesCmd": "WriteSchedulerEntries",
		"writeServerStateCmd": "WriteServerState", "receiverTargetQueueCmd": "applyReceiverTargetTaskCAS", "forwardCmd": "forward",
		"receiverTargetForwardCmd": "forward", "listMessagesCmd": "listMessages", "listOrderedMessagesCmd": "listMessages",
		"listZSetEntriesCmd": "listZSetEntries", "memoryUsageCmd": "memoryUsage", "receiverTargetRunAllCmd": "runAll", "runAllCmd": "runAll",
	}
	if got := receiverTargetScriptOwners(files, scripts); !reflect.DeepEqual(got, wantScriptOwners) {
		t.Fatalf("script owner inventory mismatch\ndiscovered: %v\nclassified: %v", got, wantScriptOwners)
	}
	if got, want := receiverTargetOwnerFunctionManifestDigest(t, files, wantScriptOwners), "a13bf88228a8e739570a7dac1f2340af9ea082764b0c3d11521296b5413ff7d9"; got != want {
		t.Fatalf("script owner function manifest digest = %s, want %s", got, want)
	}
	redisCall := regexp.MustCompile(`redis\.(?:call|pcall)\(\s*["']([A-Za-z]+)["']`)
	readOnlyCommands := map[string]bool{"EXISTS": true, "GET": true, "HGET": true, "HGETALL": true, "HEXISTS": true, "HLEN": true, "HMGET": true, "LLEN": true, "LRANGE": true, "MEMORY": true, "SCARD": true, "SISMEMBER": true, "SMEMBERS": true, "TYPE": true, "ZCARD": true, "ZCOUNT": true, "ZRANGE": true, "ZRANGEBYSCORE": true, "ZREVRANGE": true, "ZREVRANGEBYSCORE": true, "ZSCORE": true}
	for name, class := range classified {
		firstWrite := firstRedisMutation(scripts[name], redisCall, readOnlyCommands)
		if class == "read-only" && firstWrite != nil {
			t.Errorf("read-only script %s contains a write", name)
		}
		if class != "read-only" && firstWrite == nil {
			t.Errorf("write-capable script %s has no recognized write", name)
		}
	}
	for _, class := range []string{"marked-selection-before-write", "marked-refusal-before-write"} {
		for _, name := range classes[class] {
			source := scripts[name]
			marker := regexp.MustCompile(`redis\.call\(\s*["']HEXISTS["']\s*,[^\n]+,\s*["']sourceIdDigest["']\s*\)\s*==\s*1\s*then`).FindStringIndex(source)
			firstWrite := firstRedisMutation(source, redisCall, readOnlyCommands)
			if marker == nil {
				t.Errorf("%s lacks the closed sourceIdDigest marker condition", name)
			} else if firstWrite == nil || marker[0] >= firstWrite[0] {
				t.Errorf("%s does not inspect the marker before its first write", name)
			} else {
				guardBody := source[marker[1]:firstWrite[0]]
				if class == "marked-refusal-before-write" && !regexp.MustCompile(`return\s+redis\.error_reply\(\s*["']RECEIVER TARGET`).MatchString(guardBody) {
					t.Errorf("%s marker branch does not return a receiver target refusal before mutation", name)
				}
				if class == "marked-selection-before-write" && !regexp.MustCompile(`return\s+\{\s*["']receiver-target["']`).MatchString(guardBody) {
					t.Errorf("%s marker branch does not return receiver target selection before mutation", name)
				}
			}
		}
	}
	wantGuardedSources := map[string]string{
		"receiverTargetDequeueCmd":           "94a352b2e1d69558292f07e51f0abdcfa75124df6e4e683afa6fd8ecb72656d6",
		"receiverTargetForwardCmd":           "01afdccdb5c32f9fd09b94cc97be8b9704fbd1746166b939cf21a2d367f3b58c",
		"archiveCmd":                         "12c103b79fd96012ddd5c4f4e9b9a3909f6f1b6cfec7e9ef57594f7242c9efa4",
		"archiveAllAggregatingCmd":           "98f31b06c3e748fe4fb529a310cd7222ef622fe3403c3d1e6797022469459ebd",
		"archiveAllPendingCmd":               "7d621e4a9ceb7d15be5bd8b652bf15f3bf254fc9c2409fedc4bbbaf25be0fc6e",
		"archiveTaskCmd":                     "508227ecfd742ac0224e198da4eb4d24ac347ed1725db4c1df8f81698b6c1e17",
		"archiveAllCmd":                      "6ee505c8b7fbee5f200cdd56cca547d27cb1cddebe4761a48d922761639ed912",
		"updateTaskPayloadCmd":               "233c0f1ef8f39ee046acc1fc64592f2dcfc363a09f1c0af4b1764b262a539cd6",
		"deleteTaskCmd":                      "67d0b4ee2638163a5313d6869ab53b0cf3131360556b2c96470a82c4cf931289",
		"deleteAllCmd":                       "8a60181cd0911bae2c83037e72db0fc50bd8db34e5f6dda7b2cbb59d7d0a00d0",
		"deleteAllAggregatingCmd":            "9c9e4c6a146e2cc4e88836575d61ac1dece5ff54fa14d4a0c4bb11d3d1cc9e7f",
		"deleteAllPendingCmd":                "d11157ea817c2510d6d36b6e0c099327159366f31e1aceab11d76fece6a2b603",
		"removeQueueForceCmd":                "643a0d7cebbe72b6488cc1b0d01044671826d065993c60e1ffe6d5a96d061789",
		"writeResultCmd":                     "27465056998993346985faef87cd32defcd7dacf01bf0b9c4fc908274f600316",
		"deleteExpiredCompletedTasksCmd":     "68c09491f1c619649bd4489e2a3c84ae1d3c49dc3b1ea4e1b5933b6fc69f40ac",
		"receiverTargetRunTaskCmd":           "de519c6aa72782bff3f778b571fe90235f1dd6880e21b72b00a1c1b340d055b9",
		"receiverTargetRunAllCmd":            "a6b8f91fb298432132f38082587611970acafa7b95f39ac04c00eeb037aa76a0",
		"receiverTargetRunAllAggregatingCmd": "36adf19f45c1f392d7c9ef3396a5041f58db1b4be71edd404a75c2fc60f3f1f8",
	}
	for name, want := range wantGuardedSources {
		got := sha256.Sum256([]byte(scripts[name]))
		if encoded := hex.EncodeToString(got[:]); encoded != want {
			t.Errorf("guarded script %s changed: got %s, want %s", name, encoded, want)
		}
	}
	assertGoMarkerReturnBeforeScripts(t, files["rdb.go"], "Done", []string{"doneCmd", "doneUniqueCmd"})
	assertGoMarkerReturnBeforeScripts(t, files["rdb.go"], "MarkAsComplete", []string{"markAsCompleteCmd", "markAsCompleteUniqueCmd"})
	assertGoMarkerReturnBeforeScripts(t, files["rdb.go"], "Requeue", []string{"requeueCmd", "receiverTargetRequeueCmd"})
	assertGoMarkerReturnBeforeScripts(t, files["rdb.go"], "Retry", []string{"retryCmd"})
	generated := scripts["receiverTargetQueueCmd"]
	if !strings.Contains(generated, "ARGV[8]=='archive'") || !strings.Contains(generated, "ARGV[8]=='Archive'") {
		t.Error("generated receiver transaction does not refuse Archive")
	}
	wantDirect := map[string]int{
		"Enqueue.SAdd": 1, "EnqueueUnique.SAdd": 1, "AddToGroup.SAdd": 1, "AddToGroupUnique.SAdd": 1,
		"Schedule.SAdd": 1, "ScheduleUnique.SAdd": 1, "RemoveQueue.SRem": 1, "ExtendLease.ZAddXX": 1,
		"WriteServerState.ZAdd": 2, "ClearServerState.ZRem": 2, "WriteSchedulerEntries.ZAdd": 1,
		"ClearSchedulerEntries.ZRem": 1, "ClearSchedulerEntries.Del": 1, "ClearSchedulerHistory.Del": 1,
		"Pause.SetNX": 1, "Unpause.Del": 1, "PublishCancelation.Publish": 1,
	}
	if got := receiverTargetDirectMutations(files); !reflect.DeepEqual(got, wantDirect) {
		t.Fatalf("direct mutation inventory mismatch\ndiscovered: %v\nclassified: %v", sortedKeys(got), sortedKeys(wantDirect))
	}
	wantDirectFunctions := map[string]string{
		"Enqueue": "82b9f7fe5ecadb916724aafb9e26f4ba26b19fc5c4af5b5a67282547adf2b795", "EnqueueUnique": "e2ee4bb7ed34c3baad409972366ac9b105049ae5d36de3c7bb57da147eea10a0",
		"AddToGroup": "19ff8a095f830922dff2dade78b965af5c6444a958111de4d66172bd9ab25305", "AddToGroupUnique": "af56f75d46b7a90a70af16448eef1076beed8215b765240b4f1ed5eef1f9b25a",
		"Schedule": "2b841eaf5873299193e96c182ad8986df34afe9efa30209ad0d07192cf8b57ee", "ScheduleUnique": "8f4b11af738ccdfa583da6013a9fea1861af356d777aa9e17061f35579afaf5d",
		"ExtendLease": "6c1ef2e9ff4d082f42e598cd721dcd4c2e77b73b6fdbf42a6adbf4863c128ccd", "WriteServerState": "45fc94f40cc3c2db42e3e3175bf80047bcd853e3c2f4c4f855e7021a959b5394",
		"ClearServerState": "ec88a5f151052c80e3551759476443642ecce9c87c78af6b7438be9afa86a378", "WriteSchedulerEntries": "e3dce825a0cffb2a0ccd87135b92ec23cd513307ddd8aad5f9ad4d7d66ba94d7",
		"ClearSchedulerEntries": "cff2613ee3c94e15431571bcaea1882ef381f3560b0dedbebd26c9903c16e974", "PublishCancelation": "3b5e01b346358393302925da9b5b74b7de8bc471c6feb9e29da5be6699fe7238",
		"ClearSchedulerHistory": "39c0e2eb4b577decfd6ea97c461229f68cf1ee34943100ed28f862839a3b455f", "RemoveQueue": "37752768911870501456875c8f98a72468420c982b0bf8746feb88481c50f750",
		"Pause": "5e56df20b0a0c3a464b6871571457e01878fe25f1c7a6e3f7b697622a5401e2d", "Unpause": "c7c7ebd8bf051a39d8107c8433687e9c905c1d5887e048ff5e6ba0e5771640ca",
	}
	if got := receiverTargetFunctionDigests(t, files, wantDirectFunctions); !reflect.DeepEqual(got, wantDirectFunctions) {
		t.Fatalf("direct mutator function digest mismatch\ndiscovered: %v\nexpected: %v", got, wantDirectFunctions)
	}
}

func parseReceiverTargetPackage(t *testing.T) map[string]*ast.File {
	t.Helper()
	packages, err := parser.ParseDir(token.NewFileSet(), ".", func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	pkg := packages["rdb"]
	if pkg == nil {
		t.Fatal("rdb package not found")
	}
	return pkg.Files
}

func receiverTargetScriptSources(t *testing.T, files map[string]*ast.File) map[string]string {
	t.Helper()
	newScriptCalls := 0
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if ok && isRedisNewScript(call.Fun) {
				newScriptCalls++
			}
			return true
		})
	}
	literals := make(map[string]string)
	for _, file := range files {
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, specification := range general.Specs {
				value, ok := specification.(*ast.ValueSpec)
				if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
					continue
				}
				literal, ok := value.Values[0].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				decoded, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				literals[value.Names[0].Name] = decoded
			}
		}
	}
	scripts := make(map[string]string)
	for _, file := range files {
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, specification := range general.Specs {
				value, ok := specification.(*ast.ValueSpec)
				if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
					continue
				}
				call, ok := value.Values[0].(*ast.CallExpr)
				if !ok || len(call.Args) != 1 || !isRedisNewScript(call.Fun) {
					continue
				}
				switch argument := call.Args[0].(type) {
				case *ast.BasicLit:
					scripts[value.Names[0].Name], _ = strconv.Unquote(argument.Value)
				case *ast.Ident:
					source, exists := literals[argument.Name]
					if !exists {
						t.Fatalf("script %s source is not a literal", value.Names[0].Name)
					}
					scripts[value.Names[0].Name] = source
				default:
					t.Fatalf("script %s source is not statically closed", value.Names[0].Name)
				}
			}
		}
	}
	if newScriptCalls != len(scripts) {
		t.Fatalf("found %d redis.NewScript calls but classified %d supported declarations", newScriptCalls, len(scripts))
	}
	return scripts
}

func firstRedisMutation(source string, calls *regexp.Regexp, readOnly map[string]bool) []int {
	for _, match := range calls.FindAllStringSubmatchIndex(source, -1) {
		command := source[match[2]:match[3]]
		if !readOnly[strings.ToUpper(command)] {
			return match[:2]
		}
	}
	return nil
}

func assertGoMarkerReturnBeforeScripts(t *testing.T, file *ast.File, functionName string, scripts []string) {
	t.Helper()
	var function *ast.FuncDecl
	for _, declaration := range file.Decls {
		candidate, ok := declaration.(*ast.FuncDecl)
		if ok && candidate.Name.Name == functionName {
			function = candidate
			break
		}
	}
	if function == nil || function.Body == nil {
		t.Fatalf("function %s not found", functionName)
	}
	markerStatement, guardStatement, scriptStatement := -1, -1, -1
	for index, statement := range function.Body.List {
		if nodeContainsIdentifier(statement, "receiverTargetTaskMarked") {
			markerStatement = index
		}
		if branch, ok := statement.(*ast.IfStmt); ok {
			condition, conditionOK := branch.Cond.(*ast.Ident)
			if conditionOK && condition.Name == "marked" && len(branch.Body.List) > 0 {
				if _, returns := branch.Body.List[len(branch.Body.List)-1].(*ast.ReturnStmt); returns {
					guardStatement = index
				}
			}
		}
		for _, script := range scripts {
			if nodeContainsIdentifier(statement, script) && scriptStatement < 0 {
				scriptStatement = index
			}
		}
	}
	if markerStatement < 0 || guardStatement <= markerStatement || scriptStatement <= guardStatement {
		t.Errorf("%s does not return from its marked branch before stock script use", functionName)
	}
}

func nodeContainsIdentifier(node ast.Node, name string) bool {
	found := false
	ast.Inspect(node, func(candidate ast.Node) bool {
		identifier, ok := candidate.(*ast.Ident)
		if ok && identifier.Name == name {
			found = true
			return false
		}
		return !found
	})
	return found
}

func receiverTargetDirectMutations(files map[string]*ast.File) map[string]int {
	readOnly := map[string]bool{
		"Close": true, "Ping": true, "Exists": true, "HLen": true, "ZRevRangeWithScores": true,
		"ZCount": true, "HExists": true, "ZScore": true, "ZRangeWithScores": true, "SMembers": true,
		"Info": true, "ClusterInfo": true, "SIsMember": true, "Get": true, "HVals": true, "LRange": true,
		"ClusterKeySlot": true, "ClusterSlots": true, "Subscribe": true, "HMGet": true,
	}
	result := make(map[string]int)
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if assignment, ok := node.(*ast.AssignStmt); ok {
					for _, expression := range assignment.Rhs {
						if isRClientSelector(expression) {
							result[function.Name.Name+".client-alias"]++
						}
					}
				}
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				method, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				client, clientOK := method.X.(*ast.SelectorExpr)
				receiver, receiverOK := selectorXIdent(client)
				if ok && clientOK && receiverOK && receiver == "r" && client.Sel.Name == "client" && !readOnly[method.Sel.Name] {
					result[function.Name.Name+"."+method.Sel.Name]++
				}
				return true
			})
		}
	}
	return result
}

func receiverTargetScriptOwners(files map[string]*ast.File, scripts map[string]string) map[string]string {
	owners := make(map[string]string, len(scripts))
	for _, file := range files {
		for _, declaration := range file.Decls {
			if general, ok := declaration.(*ast.GenDecl); ok {
				for _, specification := range general.Specs {
					value, ok := specification.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, expression := range value.Values {
						ast.Inspect(expression, func(node ast.Node) bool {
							identifier, ok := node.(*ast.Ident)
							if ok {
								if _, isScript := scripts[identifier.Name]; isScript {
									owners[identifier.Name] = "package-alias"
								}
							}
							return true
						})
					}
				}
			}
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				identifier, ok := node.(*ast.Ident)
				if !ok || scripts[identifier.Name] == "" {
					return true
				}
				if prior := owners[identifier.Name]; prior != "" {
					owners[identifier.Name] = prior + "," + function.Name.Name
				} else {
					owners[identifier.Name] = function.Name.Name
				}
				return true
			})
		}
	}
	return owners
}

func receiverTargetOwnerFunctionManifestDigest(t *testing.T, files map[string]*ast.File, scriptOwners map[string]string) string {
	t.Helper()
	functions := make(map[string]struct{})
	for _, owner := range scriptOwners {
		functions[owner] = struct{}{}
	}
	digests := receiverTargetFunctionDigests(t, files, functions)
	var manifest strings.Builder
	for _, name := range sortedKeys(digests) {
		manifest.WriteString(name)
		manifest.WriteByte('=')
		manifest.WriteString(digests[name])
		manifest.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(manifest.String()))
	return hex.EncodeToString(sum[:])
}

func receiverTargetFunctionDigests[T any](t *testing.T, files map[string]*ast.File, expected map[string]T) map[string]string {
	t.Helper()
	result := make(map[string]string, len(expected))
	fileSet := token.NewFileSet()
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if _, exists := expected[function.Name.Name]; !exists {
				continue
			}
			var formatted bytes.Buffer
			if err := format.Node(&formatted, fileSet, function); err != nil {
				t.Fatalf("format %s: %v", function.Name.Name, err)
			}
			sum := sha256.Sum256(formatted.Bytes())
			result[function.Name.Name] = hex.EncodeToString(sum[:])
		}
	}
	return result
}

func isRClientSelector(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	receiver, receiverOK := selectorXIdent(selector)
	return ok && receiverOK && receiver == "r" && selector.Sel.Name == "client"
}

func isRedisNewScript(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	identifier, identifierOK := selectorXIdent(selector)
	return ok && identifierOK && identifier == "redis" && selector.Sel.Name == "NewScript"
}

func selectorXIdent(selector *ast.SelectorExpr) (string, bool) {
	if selector == nil {
		return "", false
	}
	identifier, ok := selector.X.(*ast.Ident)
	if !ok {
		return "", false
	}
	return identifier.Name, true
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
