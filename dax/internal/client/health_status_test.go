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
	"context"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/mock"
)

// Generate all unit tests for onReadRequest method in healthStatus file

type mockRouteListener struct {
	mock.Mock
}

func (mrl *mockRouteListener) addRoute(endpoint string, route DaxAPI) {
	mrl.Called()
}

func (mrl *mockRouteListener) removeRoute(endpoint string, route DaxAPI) {
	mrl.Called()
}

func (mrl *mockRouteListener) isRouteManagerEnabled() bool {
	return true
}

func Test_nilRouteListener(t *testing.T) {
	hs := newHealthStatus("dummy", nil)
	_, ok := hs.(*disabledHealthStatus)
	if !ok {
		t.Errorf("disabledHealthStatus not initialized with empty routeListener")
	}
}

func Test_onErrorInReadRequest_differentError(t *testing.T) {
	mrl := &mockRouteListener{}
	hs := newHealthStatus("dummy", mrl)
	ehs, ok := hs.(*enabledHealthStatus)
	if !ok {
		t.Errorf("enabledHealthStatus not initialized with empty routeListener")
	}

	if ehs.curReadTimeoutCount != 0 {
		t.Errorf("curReadTimeoutCount should be initially 0")
	}
	prevReadTimeoutCount := ehs.curReadTimeoutCount
	hs.onErrorInReadRequest(context.DeadlineExceeded, nil)
	if ehs.curReadTimeoutCount != prevReadTimeoutCount+1 {
		t.Errorf("onErrorInReadRequest failed to increment curReadTimeoutCount on timeout error")
	}

	prevReadTimeoutCount = ehs.curReadTimeoutCount
	err := &smithy.GenericAPIError{
		Code:    "c1",
		Message: "msg",
	}
	hs.onErrorInReadRequest(err, nil)
	if ehs.curReadTimeoutCount != prevReadTimeoutCount {
		t.Errorf("onErrorInReadRequest incremented curReadTimeoutCount on non timeout error")
	}
}

func Test_onErrorInReadRequest_removeRouteCall(t *testing.T) {
	mrl := &mockRouteListener{}
	mrl.On("removeRoute").Return(nil).Times(1)
	hs := newHealthStatus("dummy", mrl)
	ehs, _ := hs.(*enabledHealthStatus)
	for i := 1; i <= timeoutErrorThreshold; i++ {
		hs.onErrorInReadRequest(context.DeadlineExceeded, nil)
		if i < timeoutErrorThreshold {
			mrl.AssertNotCalled(t, "removeRoute")
			if !ehs.isHealthy {
				t.Errorf("isHealthy should be true")
			}
		} else {
			mrl.AssertCalled(t, "removeRoute")
			if ehs.isHealthy {
				t.Errorf("isHealthy should be false")
			}
		}

	}
}

func Test_onSuccessInReadRequest(t *testing.T) {
	mrl := &mockRouteListener{}
	hs := newHealthStatus("dummy", mrl)
	ehs, _ := hs.(*enabledHealthStatus)
	ehs.curReadTimeoutCount = 5
	hs.onSuccessInReadRequest()
	if ehs.curReadTimeoutCount != 0 {
		t.Errorf("onSuccessInReadRequest failed to set curReadTimeoutCount to 0")
	}

	ehs.isHealthy = false
	ehs.curReadTimeoutCount = 5
	hs.onSuccessInReadRequest()
	if ehs.curReadTimeoutCount != 5 {
		t.Errorf("onSuccessInReadRequest reset the curReadTimeoutCount on unhealthy client")
	}
}

func Test_onHealthCheckSuccess(t *testing.T) {
	mrl := &mockRouteListener{}
	mrl.On("addRoute").Return(nil).Times(1)
	hs := newHealthStatus("dummy", mrl)
	ehs, _ := hs.(*enabledHealthStatus)
	ehs.isHealthy = false
	ehs.curReadTimeoutCount = 5

	ehs.onHealthCheckSuccess(nil)
	if !ehs.isHealthy {
		t.Errorf("onHealthCheckSuccess failed to set isHealthy to true")
	}
	if ehs.curReadTimeoutCount != 0 {
		t.Errorf("onHealthCheckSuccess failed to set curReadTimeoutCount to 0")
	}
	mrl.AssertCalled(t, "addRoute")
}
