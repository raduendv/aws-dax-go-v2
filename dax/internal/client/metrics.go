/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

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
	"time"

	"github.com/aws/smithy-go/metrics"
)

const (
	daxMeterScope = "github.com/aws/aws-dax-go-v2"

	daxOpNameSuccess              = "dax.op.%s.success"
	daxOpNameFailure              = "dax.op.%s.failure"
	daxOpNameLatencyUs            = "dax.op.%s.latency_us"
	daxConnectionsIdle            = "dax.connections.idle"
	daxConnectionsPendingAcquire  = "dax.connections.pending_acquire"
	daxConnectionsCreated         = "dax.connections.created"
	daxConnectionsClosedError     = "dax.connections.closed.error"
	daxConnectionsClosedIdle      = "dax.connections.closed.idle"
	daxConnectionsClosedSession   = "dax.connections.closed.session"
	daxHealthCheckSuccess         = "dax.health_check.success"
	daxHealthCheckFailure         = "dax.health_check.failure"
	daxRouteManagerRoutesAdded    = "dax.route_manager.routes.added"
	daxRouteManagerRoutesRemoved  = "dax.route_manager.routes.removed"
	daxRouteManagerFailOpenEvents = "dax.route_manager.fail_open.events"
	daxRouteManagerDisabledState  = "dax.route_manager.disabled_state"
)

type metricFunction[T any] func() (T, error)

func countMetricInt64(ctx context.Context, mp metrics.MeterProvider, name string, v int64) error {
	m := mp.Meter(daxMeterScope)

	c, err := m.Int64Counter(name)
	if err != nil {
		return err
	}

	c.Add(ctx, v)

	return nil
}

func gaugeInt64(ctx context.Context, mp metrics.MeterProvider, name string, v int64) error {
	m := mp.Meter(daxMeterScope)

	g, err := m.Int64Gauge(name)
	if err != nil {
		return err
	}

	g.Sample(ctx, v)

	return nil
}

func histogramMicrosecondsInt64(ctx context.Context, mp metrics.MeterProvider, name string, t time.Time) error {
	m := mp.Meter(daxMeterScope)

	h, err := m.Int64Histogram(name)
	if err != nil {
		return err
	}

	h.Record(ctx, time.Since(t).Microseconds())

	return nil
}

func withMicrosecondHistogramInt64[T any](ctx context.Context, provider metrics.MeterProvider, name string, fn metricFunction[T]) (T, error) {
	startTime := time.Now()

	out, err := fn()

	_ = histogramMicrosecondsInt64(ctx, provider, name, startTime)

	return out, err
}
