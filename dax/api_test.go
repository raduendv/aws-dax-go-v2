/*
  Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  A copy of the License is located at

      http://www.apache.org/licenses/LICENSE-2.0

  or in the "license" file accompanying this file. This file is distributed
  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  express or implied. See the License for the specific language governing
  permissions and limitations under the License.
*/

package dax

import (
	"context"
	"testing"

	"github.com/aws/aws-dax-go-v2/dax/internal/client"
)

func TestUnimplementedBehavior(t *testing.T) {
	dax := createClient(t)

	// CreateBackup is not implemented by DAX
	o, err := dax.CreateBackup(context.Background(), nil)

	if o != nil {
		t.Errorf("expect nil from unimplemented method, got %v", o)
	}
	if err == nil || err.Error() != client.ErrCodeNotImplemented {
		t.Errorf("expect not implemented error, got %v", err)
	}
}

func createClient(t *testing.T) *Dax {
	cfg := DefaultConfig()
	cfg.HostPorts = []string{"127.0.0.1:8111"}
	cfg.Region = "us-west-2"
	dax, err := New(cfg)
	if err != nil {
		t.Errorf("expect no error, got %v", err)
	}
	return dax
}
