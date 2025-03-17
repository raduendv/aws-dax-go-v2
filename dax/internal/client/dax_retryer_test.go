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

package client

import (
	"fmt"
	"testing"

	"github.com/aws/smithy-go"
)

func TestDaxRetryer_IsErrorRetryable(t *testing.T) {
	retryer := &DaxRetryer{
		BaseThrottleDelay: DefaultBaseRetryDelay,
		MaxBackoffDelay:   DefaultMaxBackoffDelay,
	}

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "regular error",
			err:      fmt.Errorf("fmt error"),
			expected: false,
		},
		{
			name:     "code 1 error",
			err:      newDaxRequestFailure([]int{1}, "", "", "", 500, smithy.FaultServer),
			expected: true,
		},
		{
			name:     "code 2 error",
			err:      newDaxRequestFailure([]int{2}, "", "", "", 500, smithy.FaultServer),
			expected: true,
		},
		{
			name:     "authentication required error",
			err:      newDaxRequestFailure([]int{4, 23, 31, 33}, "", "", "", 500, smithy.FaultServer),
			expected: true,
		},
		{
			name:     "non-retryable error",
			err:      newDaxRequestFailure([]int{0}, "", "", "", 500, smithy.FaultServer),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := retryer.IsErrorRetryable(tt.err)
			if result != tt.expected {
				t.Errorf("IsErrorRetryable() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDaxRetryer_RetryDelay(t *testing.T) {
	retryer := &DaxRetryer{
		BaseThrottleDelay: DefaultBaseRetryDelay,
		MaxBackoffDelay:   DefaultMaxBackoffDelay,
	}

	// Test with throttle error
	throttleErr := newDaxRequestFailure([]int{}, "ThrottlingException", "", "", 400, smithy.FaultClient)
	delay := retryer.RetryDelay(1, throttleErr)
	if delay == 0 {
		t.Error("Expected non-zero delay for throttle error")
	}

	// Test with non-throttle error
	normalErr := fmt.Errorf("normal error")
	delay = retryer.RetryDelay(1, normalErr)
	if delay != 0 {
		t.Error("Expected zero delay for non-throttle error")
	}
}

// Test MaxAttempts
func TestDaxRetryer_MaxAttempts(t *testing.T) {
	retryer := &DaxRetryer{}
	if retryer.MaxAttempts() != 0 {
		t.Errorf("Expected MaxAttempts to return 0, got %d", retryer.MaxAttempts())
	}
}
