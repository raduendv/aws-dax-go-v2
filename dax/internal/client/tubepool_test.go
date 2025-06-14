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
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-dax-go-v2/dax/internal/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var connConfigData = connConfig{isEncrypted: false}

type mockTube struct {
	mock.Mock
}

func (m *mockTube) Init() error {
	args := m.Called()
	return args.Error(0)
}
func (m *mockTube) AuthExpiryUnix() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}
func (m *mockTube) SetAuthExpiryUnix(exp int64) {
	m.Called(exp)
}
func (m *mockTube) CompareAndSwapAuthID(auth string) bool {
	args := m.Called(auth)
	return args.Bool(0)
}
func (m *mockTube) SetDeadline(time time.Time) error {
	args := m.Called(time)
	return args.Error(0)
}
func (m *mockTube) Session() session {
	args := m.Called()
	return args.Get(0).(session)
}
func (m *mockTube) Next() tube {
	args := m.Called()
	return args.Get(0).(tube)
}
func (m *mockTube) SetNext(next tube) {
	m.Called(next)
}
func (m *mockTube) CborReader() *cbor.Reader {
	args := m.Called()
	return args.Get(0).(*cbor.Reader)
}
func (m *mockTube) CborWriter() *cbor.Writer {
	args := m.Called()
	return args.Get(0).(*cbor.Writer)
}
func (m *mockTube) Close() error {
	args := m.Called()
	return args.Error(0)
}

const localConnTimeoutMillis = 10

func TestTubePoolConnectionCache(t *testing.T) {
	endpoint := ":8181"
	var actualConnections, expectedConnections int
	startConnNotifier := make(chan net.Conn, 25)
	endConnNotifier := make(chan net.Conn, 25)
	listener, err := startServer(endpoint, startConnNotifier, endConnNotifier, drainAndCloseConn)
	if err != nil {
		t.Fatalf("cannot start server")
	}
	defer listener.Close()

	tmp := &testMeterProvider{}
	sdkMetrics, _ := buildDaxSdkMetrics(tmp)
	pool := newTubePoolWithOptions(endpoint, tubePoolOptions{10, time.Second * 1, defaultDialer.DialContext}, connConfigData, sdkMetrics)

	// verify tube is re-used
	expectedConnections = 1
	attempts := 3
	for i := 0; i < attempts; i++ {
		tube, err := pool.get()
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
		select {
		case <-startConnNotifier:
			actualConnections++
		case <-endConnNotifier:
			t.Errorf("unexpected connection term")
		case <-time.After(time.Millisecond * localConnTimeoutMillis):
		}

		if actualConnections != expectedConnections {
			t.Errorf("expected %v, actual %v", expectedConnections, actualConnections)
		}

		pool.put(tube)
	}

	// verify new connections are established if cached tube not available
	expectedConnections = 0
	attempts = 3
	tubes := make([]tube, attempts)
	for i := 0; i < attempts; i++ {
		tube, err := pool.get()
		tubes[i] = tube
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
		expectedConnections++
		select {
		case <-startConnNotifier:
			actualConnections++
		case <-endConnNotifier:
			t.Errorf("unexpected connection term")
		case <-time.After(time.Millisecond * localConnTimeoutMillis):
		}

		if actualConnections != expectedConnections {
			t.Errorf("expected %v, actual %v", expectedConnections, actualConnections)
		}
	}

	// verify connections are kept alive when returned
	for i := 0; i < attempts; i++ {
		pool.put(tubes[i])
		select {
		case <-startConnNotifier:
			t.Errorf("unexpected connection init")
		case <-endConnNotifier:
			t.Errorf("unexpected connection term")
		case <-time.After(time.Millisecond * localConnTimeoutMillis):
		}
	}

	// verify tubes cache is lifo
	for i := 0; i < len(tubes); i++ {
		tube, err := pool.get()
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
		select {
		case <-startConnNotifier:
			t.Errorf("unexpected connection init")
		case <-endConnNotifier:
			t.Errorf("unexpected connection term")
		case <-time.After(time.Millisecond * localConnTimeoutMillis):
		}

		if tube != tubes[len(tubes)-i-1] {
			t.Errorf("expected most recent tube")
		}
	}

	expectCounters(t, sdkMetrics, map[string]int{
		daxConnectionsCreated: 3,
	})
	expectGauges(t, sdkMetrics, map[string]int{
		daxConcurrentConnectionAttempts: 0,
		daxConnectionsIdle:              0,
	})
}

func TestTubePool_reapIdleTubes(t *testing.T) {
	endpoint := ":8182"
	startConnNotifier := make(chan net.Conn, 25)
	endConnNotifier := make(chan net.Conn, 25)
	listener, err := startServer(endpoint, startConnNotifier, endConnNotifier, drainAndCloseConn)
	if err != nil {
		t.Fatalf("cannot start server")
	}
	defer listener.Close()

	tmp := &testMeterProvider{}
	sdkMetrics, _ := buildDaxSdkMetrics(tmp)
	pool := newTubePool(endpoint, connConfigData, sdkMetrics)

	tubeCount := 10
	tubes := make([]tube, tubeCount)
	for i := 0; i < tubeCount; i++ {
		tubes[i], err = pool.get()
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
	}

	for i := 0; i < tubeCount; i++ {
		pool.put(tubes[i])
	}

	pool.reapIdleConnections()
	if countTubes(pool) != tubeCount {
		t.Errorf("expected cached tube count %v, actual %v", tubeCount, countTubes(pool))
	}

	active := make([]tube, 0, tubeCount)
	activeCount := 5
	for i := 0; i < activeCount; i++ {
		tb, err := pool.get()
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
		active = append([]tube{tb}, active...)
	}

	pool.reapIdleConnections()
	if countTubes(pool) != 1 {
		t.Errorf("expected cached tube count %v, actual %v", 1, countTubes(pool))
	}

	putCount := (activeCount + 1) / 2
	for i := 0; i < putCount; i++ {
		activeCount--
		a := active[activeCount]
		active = active[0:activeCount]
		pool.put(a)
	}

	pool.reapIdleConnections()
	if countTubes(pool) != putCount+1 {
		t.Errorf("expected cached tube count %v, actual %v", putCount+1, countTubes(pool))
	}

	count := len(active)
	for _, a := range active {
		pool.put(a)
	}

	pool.reapIdleConnections()
	if countTubes(pool) != count+1 {
		t.Errorf("expected cached tube count %v, actual %v", count+1, countTubes(pool))
	}

	expectCounters(t, sdkMetrics, map[string]int{
		daxConnectionsCreated:    10,
		daxConnectionsClosedIdle: -1,
	})
}

func TestTubePool_Close(t *testing.T) {
	endpoint := ":8183"
	startConnNotifier := make(chan net.Conn, 25)
	endConnNotifier := make(chan net.Conn, 25)
	listener, err := startServer(endpoint, startConnNotifier, endConnNotifier, drainAndCloseConn)
	if err != nil {
		t.Fatalf("could not start server")
	}
	defer listener.Close()

	tmp := &testMeterProvider{}
	sdkMetrics, _ := buildDaxSdkMetrics(tmp)
	pool := newTubePoolWithOptions(endpoint, tubePoolOptions{1, time.Second * 1, defaultDialer.DialContext}, connConfigData, sdkMetrics)
	tubes := make([]tube, 2)
	for i := 0; i < 2; i++ {
		tubes[i], err = pool.get()
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		select {
		case <-startConnNotifier:
		case <-time.After(time.Second * 1):
			t.Fatalf("could not establish connection")
		}
	}

	pool.put(tubes[0])
	pool.Close()

	select {
	case <-endConnNotifier:
	case <-time.After(time.Millisecond * localConnTimeoutMillis):
		t.Fatalf("cached connection was not terminated")
	}

	if tubeCount := countTubes(pool); tubeCount != 0 {
		t.Fatalf("Closed pool is not empty. Pool size: %d", tubeCount)
	}

	// We should be able to Close multiple times
	if err := pool.Close(); err != nil {
		t.Errorf("Must return nil if pool is already closed")
	}

	pool.put(tubes[1])
	select {
	case <-endConnNotifier:
	case <-time.After(time.Millisecond * localConnTimeoutMillis):
		t.Fatalf("tube returned to a closed pool was not terminated")
	}

	if tubeCount := countTubes(pool); tubeCount != 0 {
		t.Fatalf("Tube returned to a closed pool changed its size. Pool size: %d", tubeCount)
	}

	assert.Len(t, tmp.meters, 1)
	s, ok := tmp.meters[daxMeterScope]
	assert.True(t, ok, fmt.Sprintf(`expected key "%s" to exist in meters map`, daxMeterScope))
	assert.NotNil(t, s)
	if !ok || s == nil {
		return
	}

	expectCounters(t, sdkMetrics, map[string]int{
		daxConnectionsCreated:       2,
		daxConnectionsClosedSession: 1,
	})
	expectGauges(t, sdkMetrics, map[string]int{
		daxConcurrentConnectionAttempts: 0,
		daxConnectionsIdle:              0,
	})
}

func TestTubePoolError(t *testing.T) {
	endpoint := ":8184"

	tmp := &testMeterProvider{}
	sdkMetrics, _ := buildDaxSdkMetrics(tmp)

	pool := newTubePoolWithOptions(endpoint, tubePoolOptions{10, time.Second * 1, defaultDialer.DialContext}, connConfigData, sdkMetrics)

	go func() {
		time.After(time.Millisecond * 20)
		expectGauges(t, sdkMetrics, map[string]int{
			daxConcurrentConnectionAttempts: 1,
		})
	}()

	_, err := pool.get()
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("expected 'dial tcp :8184: connection refused', actual '%v'\n", err)
	}
}

func TestTubePoolErrorWithCustomDialContext(t *testing.T) {
	endpoint := ":8185"
	var numDials int64

	tmp := &testMeterProvider{}
	sdkMetrics, _ := buildDaxSdkMetrics(tmp)

	pool := newTubePoolWithOptions(endpoint, tubePoolOptions{10, time.Second * 1, func(ctx context.Context, network, address string) (net.Conn, error) {
		atomic.AddInt64(&numDials, 1)
		var d net.Dialer
		return d.DialContext(ctx, network, address)
	}}, connConfigData, sdkMetrics)
	_, err := pool.get()
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("expected 'dial tcp :8184: connection refused', actual '%v'\n", err)
	}

	nDials := atomic.LoadInt64(&numDials)
	if nDials == 0 {
		t.Error("expected custom dialer to be called, got 0 calls")
	}
}

func TestConnectionPriority(t *testing.T) {
	endpoint := ":8186"
	listener, err := startServer(endpoint, nil, nil, drainAndCloseConn)
	if err != nil {
		t.Fatalf("cannot start server")
	}
	defer listener.Close()

	maxAttempts := 5
	var delay sync.WaitGroup
	delay.Add(maxAttempts + 1)
	connectFn := func(ctx context.Context, network, address string) (net.Conn, error) {
		delay.Done()
		delay.Wait()
		var d net.Dialer
		return d.DialContext(ctx, network, address)
	}

	tmp := &testMeterProvider{}
	sdkMetrics, _ := buildDaxSdkMetrics(tmp)
	pool := newTubePoolWithOptions(endpoint, tubePoolOptions{maxAttempts, 1 * time.Second, defaultDialer.DialContext}, connConfigData, sdkMetrics)
	pool.dialContext = connectFn
	defer pool.Close()

	var wg sync.WaitGroup
	wg.Add(maxAttempts)
	for i := 0; i < maxAttempts; i++ {
		go func() {
			defer wg.Done()
			tb, err := pool.getWithContext(context.Background(), false, RequestOptions{})
			if err != nil {
				t.Errorf("unexpected error %v", err)
			} else {
				tb.Close()
			}
		}()
	}

	ctx, cfn := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cfn()
	_, err = pool.getWithContext(ctx, false, RequestOptions{})
	if err == nil {
		t.Errorf("expected error, got none")
	}
	if err != ctx.Err() {
		t.Errorf("expected %v, got %v", ctx.Err(), err)
	}

	tb, err := pool.getWithContext(context.Background(), true, RequestOptions{})
	if err != nil {
		t.Errorf("unexpected error %v", err)
	} else {
		tb.Close()
	}

	wg.Wait()

	expectCounters(t, sdkMetrics, map[string]int{
		daxConnectionsCreated: 6,
	})
}

func TestGetWithClosedErrorChannel(t *testing.T) {
	endpoint := ":8185"
	listener, err := startServer(endpoint, nil, nil, drainAndCloseConn)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer listener.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	tmp := &testMeterProvider{}
	sdkMetrics, _ := buildDaxSdkMetrics(tmp)
	pool := newTubePoolWithOptions(endpoint, tubePoolOptions{1, 10 * time.Second, defaultDialer.DialContext}, connConfigData, sdkMetrics)
	pool.dialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		wg.Done()
		// Block indefinetely to mimic a long connection
		for {

		}
	}

	go func() {
		wg.Wait()

		expectGauges(t, sdkMetrics, map[string]int{
			daxConcurrentConnectionAttempts: 1,
		})

		pool.Close()
	}()

	tube, err := pool.getWithContext(context.Background(), false, RequestOptions{})
	if tube != nil {
		t.Fatalf("Expected nil tube")
	}

	if err != os.ErrClosed {
		t.Fatalf("Expected os.ErrClosed error but got %v", err)
	}
}

func TestGate(t *testing.T) {
	size := 3
	g := make(gate, size)
	// fill all slots
	for i := 0; i < size; i++ {
		if !g.tryEnter() {
			t.Errorf("expected gate access allowed")
		}
	}

	// verify further access is not allowed
	for i := 0; i < (size * 3); i++ {
		if g.tryEnter() {
			t.Errorf("expected gate access denied")
		}
	}

	// free up one slot, verify access is allowed
	g.exit()
	if !g.tryEnter() {
		t.Errorf("expected gate access allowed")
	}

	// free up all slots and more
	for i := 0; i < (size * 3); i++ {
		g.exit()
	}

	// fill all slots
	for i := 0; i < size; i++ {
		if !g.tryEnter() {
			t.Errorf("expected gate access allowed")
		}
	}

	// verify further access is not allowed
	for i := 0; i < (size * 3); i++ {
		if g.tryEnter() {
			t.Errorf("expected gate access denied")
		}
	}
}

func startServer(endpoint string, startConnNotifier chan net.Conn, endConnNotifier chan net.Conn,
	connectionHandler func(conn net.Conn, endConnNotifier chan net.Conn)) (net.Listener, error) {
	listener, err := net.Listen(network, endpoint)
	if err != nil {
		return nil, fmt.Errorf("cannot create server due to %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				//log.Printf("cannot accept connection %v\n", err)
				return
			}
			startConnNotifier <- conn
			go connectionHandler(conn, endConnNotifier)
		}
	}()

	return listener, nil
}

func drainAndCloseConn(conn net.Conn, endConnNotifier chan net.Conn) {
	b := make([]byte, 1024)
	for {
		_, err := conn.Read(b)
		if err == io.EOF {
			endConnNotifier <- conn
			if err = conn.Close(); err != nil {
				log.Printf("unexpected error %v", err)
			}
			return
		} else if err != nil {
			log.Printf("unexpected error %v", err)
			return
		}
	}
}

func countTubes(pool *tubePool) int {
	head := pool.top
	count := 0
	for head != nil {
		count++
		head = head.Next()
	}
	return count
}

func TestTubePool_close(t *testing.T) {
	tmp := &testMeterProvider{}
	sdkMetrics, _ := buildDaxSdkMetrics(tmp)
	p := newTubePoolWithOptions(":1234", tubePoolOptions{1, 5 * time.Second, defaultDialer.DialContext}, connConfigData, sdkMetrics)
	origSession := p.session
	p.closeTubeImmediately = true

	tt := &mockTube{}
	tt.On("Close").Return(nil).Once()
	p.closeTube(tt)
	require.Equal(t, origSession, p.session)
	tt.AssertCalled(t, "Close")

	expectCounters(t, sdkMetrics, map[string]int{
		daxConnectionsClosedError: 1,
	})
}

func TestTubePool_PutClosesTubesIfPoolIsClosed(t *testing.T) {
	tmp := &testMeterProvider{}
	sdkMetrics, _ := buildDaxSdkMetrics(tmp)
	p := newTubePoolWithOptions(":1234", tubePoolOptions{1, 5 * time.Second, defaultDialer.DialContext}, connConfigData, sdkMetrics)
	p.closed = true

	tt := &mockTube{}
	tt.On("Session").Return(p.session).Maybe()
	tt.On("Close").Return(nil).Once()

	p.put(tt)

	tt.AssertExpectations(t)

	expectCounters(t, sdkMetrics, map[string]int{
		daxConnectionsClosedSession: 1,
	})
}

func TestTubePool_PutClosesTubesFromDifferentSession(t *testing.T) {
	tmp := &testMeterProvider{}
	sdkMetrics, _ := buildDaxSdkMetrics(tmp)
	p := newTubePoolWithOptions(":1234", tubePoolOptions{1, 5 * time.Second, defaultDialer.DialContext}, connConfigData, sdkMetrics)

	tt := &mockTube{}
	tt.On("Session").Return(p.session + 100)
	tt.On("Close").Return(nil).Once()

	p.put(tt)

	tt.AssertExpectations(t)

	expectCounters(t, sdkMetrics, map[string]int{
		daxConnectionsClosedSession: 1,
	})
}
