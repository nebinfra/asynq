// Copyright 2026 NebInfra. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package proto

import (
	"testing"

	"google.golang.org/protobuf/reflect/protodesc"
)

func TestGoPackageMatchesForkModule(t *testing.T) {
	got := protodesc.ToFileDescriptorProto(File_asynq_proto).GetOptions().GetGoPackage()
	const want = "github.com/nebinfra/asynq/internal/proto"
	if got != want {
		t.Fatalf("asynq.proto go_package = %q, want %q", got, want)
	}
}
