package failsafe_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/failsafe-go/failsafe-go/hedgepolicy"
	"github.com/failsafe-go/failsafe-go/ratelimiter"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/failsafe-go/failsafe-go/timeout"
)

// Verifies that snapshots are disabled by default and that SnapshotOf reports false.
func TestSnapshotDisabledByDefault(t *testing.T) {
	// Given
	var info failsafe.ExecutionInfo
	executor := failsafe.With[any](retrypolicy.NewBuilder[any]().WithMaxRetries(1).Build()).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			info = event.ExecutionInfo
		})

	// When
	_, err := executor.Get(func() (any, error) { return nil, nil })

	// Then
	require.NoError(t, err)
	require.NotNil(t, info)
	snapshot, ok := failsafe.SnapshotOf(info)
	assert.False(t, ok)
	assert.Equal(t, failsafe.ExecutionSnapshot{}, snapshot)
}

// Verifies that a retry and timeout composition records each attempt, the planned retry delay, the last error, and
// that a timeout is distinguishable from a context cancellation.
func TestSnapshotRetryWithTimeout(t *testing.T) {
	// Given
	retryPolicy := retrypolicy.NewBuilder[any]().
		WithDelay(50 * time.Millisecond).
		WithMaxRetries(2).
		Build()
	to := timeout.New[any](50 * time.Millisecond)
	var snapshot failsafe.ExecutionSnapshot
	executor := failsafe.With[any](retryPolicy, to).
		WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			var ok bool
			snapshot, ok = failsafe.SnapshotOf(event.ExecutionInfo)
			require.True(t, ok)
		})

	// When
	_, err := executor.GetWithExecution(func(exec failsafe.Execution[any]) (any, error) {
		<-exec.Context().Done()
		return nil, exec.Context().Err()
	})

	// Then
	require.Error(t, err)
	assert.Equal(t, 3, snapshot.Attempts)
	assert.Equal(t, 2, snapshot.Retries)
	assert.Equal(t, 50*time.Millisecond, snapshot.PlannedDelay)
	assert.NotNil(t, snapshot.LastError)
	assert.Equal(t, failsafe.CancelTimeout, snapshot.CancelCause)
	require.Len(t, snapshot.History, 3)
	orders := map[int]bool{}
	for _, attempt := range snapshot.History {
		assert.NotZero(t, attempt.ID)
		assert.Equal(t, snapshot.ID, attempt.ParentID)
		assert.False(t, attempt.Hedge)
		assert.True(t, attempt.Completed)
		assert.True(t, attempt.Canceled)
		assert.Equal(t, failsafe.CancelTimeout, attempt.CancelCause)
		assert.ErrorIs(t, attempt.Err, timeout.ErrExceeded)
		assert.Greater(t, attempt.Duration, time.Duration(0))
		orders[attempt.CompletionOrder] = true
	}
	assert.Len(t, orders, 3, "completion orders should be unique")

	// Policy states should be present for both policies
	states := policyStates(snapshot)
	assert.Contains(t, states, "RetryPolicy")
	assert.Contains(t, states, "Timeout")
	assert.False(t, states["RetryPolicy"].Shared)
}

// Verifies that a hedge execution records an independent ID and common parent for each concurrent attempt, that the
// completion order reflects what actually happened, and that the losing attempt is recorded as canceled by the hedge.
func TestSnapshotHedgeWinnerAndLoserCancellation(t *testing.T) {
	// Given
	hedge := hedgepolicy.NewBuilderWithDelay[any](20 * time.Millisecond).
		WithMaxHedges(1).
		Build()
	var info failsafe.ExecutionInfo
	executor := failsafe.With[any](hedge).
		WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			info = event.ExecutionInfo
		})
	var calls atomic.Int32
	loserDone := make(chan struct{})

	// When
	result, err := executor.GetWithExecution(func(exec failsafe.Execution[any]) (any, error) {
		if calls.Add(1) == 1 {
			// The initial attempt is slow and loses
			<-exec.Context().Done()
			defer close(loserDone)
			return nil, exec.Context().Err()
		}
		// The hedged attempt wins
		return "winner", nil
	})

	// Then
	require.NoError(t, err)
	assert.Equal(t, "winner", result)
	require.NotNil(t, info)

	// Wait for the loser's completion to actually be recorded
	require.Eventually(t, func() bool {
		snapshot, ok := failsafe.SnapshotOf(info)
		if !ok || len(snapshot.History) != 2 {
			return false
		}
		return snapshot.History[0].Completed && snapshot.History[1].Completed
	}, 5*time.Second, time.Millisecond)

	snapshot, ok := failsafe.SnapshotOf(info)
	require.True(t, ok)
	assert.Equal(t, 2, snapshot.Attempts)
	assert.Equal(t, 1, snapshot.Hedges)
	require.Len(t, snapshot.History, 2)

	loser, winner := snapshot.History[0], snapshot.History[1]
	assert.False(t, loser.Hedge)
	assert.True(t, winner.Hedge)
	assert.NotEqual(t, loser.ID, winner.ID)
	assert.Equal(t, snapshot.ID, loser.ParentID)
	assert.Equal(t, snapshot.ID, winner.ParentID)

	// The winner completed first and was not canceled
	assert.Equal(t, 1, winner.CompletionOrder)
	assert.False(t, winner.Canceled)
	assert.Nil(t, winner.Err)

	// The loser completed second and was canceled by the hedge
	assert.Equal(t, 2, loser.CompletionOrder)
	assert.True(t, loser.Canceled)
	assert.Equal(t, failsafe.CancelHedge, loser.CancelCause)
}

// Verifies that snapshots can be read concurrently from listeners and other goroutines while executions are in
// progress, without data races.
func TestSnapshotConcurrentReads(t *testing.T) {
	// Given
	retryPolicy := retrypolicy.NewBuilder[any]().
		WithDelay(time.Millisecond).
		WithMaxRetries(3).
		Build()
	readSnapshot := func(info failsafe.ExecutionInfo) {
		snapshot, ok := failsafe.SnapshotOf(info)
		require.True(t, ok)
		_ = snapshot.Attempts
		_ = snapshot.History
		_ = snapshot.Policies
	}
	executor := failsafe.With[any](retryPolicy).
		WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			readSnapshot(event.ExecutionInfo)
		})

	// When running executions and snapshot reads concurrently
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			executor.Get(func() (any, error) { return nil, errors.New("test") })
		}()
	}

	// Then reads from an unrelated goroutine should not race with the executions
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = failsafe.SnapshotOf(nil)
			}
		}
	}()
	wg.Wait()
	close(stop)
}

// Verifies that reading a snapshot from within a listener and triggering another execution from the same callback
// does not deadlock.
func TestSnapshotCallbackReentrancy(t *testing.T) {
	// Given
	innerExecutor := failsafe.With[any](retrypolicy.NewBuilder[any]().WithMaxRetries(1).Build()).
		WithSnapshots(true)
	var outer failsafe.Executor[any]
	outer = failsafe.With[any](retrypolicy.NewBuilder[any]().WithMaxRetries(1).Build()).
		WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			// Reading a snapshot within a callback
			snapshot, ok := failsafe.SnapshotOf(event.ExecutionInfo)
			require.True(t, ok)
			assert.Equal(t, 1, snapshot.Attempts)

			// Triggering another execution from within a callback
			_, err := innerExecutor.Get(func() (any, error) { return nil, nil })
			require.NoError(t, err)

			// Reading again, reentrantly
			_, ok = failsafe.SnapshotOf(event.ExecutionInfo)
			require.True(t, ok)
		})

	// When / Then should complete without deadlocking
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := outer.Get(func() (any, error) { return nil, nil })
		require.NoError(t, err)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: reentrant snapshot read or execution did not complete")
	}
}

// Verifies that circuit breaker state transitions are reflected in snapshots as shared, snapshot-time state.
func TestSnapshotCircuitBreakerStateTransitions(t *testing.T) {
	// Given
	breaker := circuitbreaker.NewBuilder[any]().
		WithFailureThreshold(1).
		WithDelay(50 * time.Millisecond).
		Build()
	var mu sync.Mutex
	var states []string
	executor := failsafe.With[any](breaker).
		WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			snapshot, ok := failsafe.SnapshotOf(event.ExecutionInfo)
			require.True(t, ok)
			state := policyStates(snapshot)["CircuitBreaker"]
			assert.True(t, state.Shared, "breaker state should be marked as shared")
			mu.Lock()
			states = append(states, state.State)
			mu.Unlock()
		})

	// When a failure opens the breaker
	_, err := executor.Get(func() (any, error) { return nil, errors.New("test") })
	require.Error(t, err)

	// Then the snapshot reflects the open state
	mu.Lock()
	require.Equal(t, []string{"open"}, states)
	mu.Unlock()

	// When the breaker half-opens and a success closes it again
	time.Sleep(100 * time.Millisecond)
	_, err = executor.Get(func() (any, error) { return "ok", nil })
	require.NoError(t, err)

	// Then the snapshot reflects the closed state
	mu.Lock()
	require.Equal(t, []string{"open", "closed"}, states)
	mu.Unlock()
}

// Verifies that a rate limiter's state is included in snapshots as shared, snapshot-time state.
func TestSnapshotRateLimiterSharedState(t *testing.T) {
	// Given
	limiter := ratelimiter.NewSmooth[any](1, time.Second)
	var snapshot failsafe.ExecutionSnapshot
	executor := failsafe.With[any](limiter).
		WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			var ok bool
			snapshot, ok = failsafe.SnapshotOf(event.ExecutionInfo)
			require.True(t, ok)
		})

	// When
	_, err := executor.Get(func() (any, error) { return nil, nil })

	// Then
	require.NoError(t, err)
	state, ok := policyStates(snapshot)["RateLimiter"]
	require.True(t, ok)
	assert.True(t, state.Shared)
	assert.NotEmpty(t, state.State)
}

// Verifies that an external context cancellation is distinguishable from a timeout.
func TestSnapshotContextCancellation(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())
	retryPolicy := retrypolicy.NewBuilder[any]().
		WithDelay(time.Millisecond).
		WithMaxRetries(-1).
		Build()
	var snapshot failsafe.ExecutionSnapshot
	executor := failsafe.With[any](retryPolicy).
		WithSnapshots(true).
		WithContext(ctx).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			var ok bool
			snapshot, ok = failsafe.SnapshotOf(event.ExecutionInfo)
			require.True(t, ok)
		})

	// When
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := executor.Get(func() (any, error) { return nil, errors.New("test") })

	// Then
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, snapshot.Canceled)
	assert.Equal(t, failsafe.CancelContext, snapshot.CancelCause)
	assert.ErrorIs(t, snapshot.LastError, context.Canceled)
}

func policyStates(snapshot failsafe.ExecutionSnapshot) map[string]failsafe.PolicySnapshot {
	states := make(map[string]failsafe.PolicySnapshot, len(snapshot.Policies))
	for _, policy := range snapshot.Policies {
		states[policy.Policy] = policy
	}
	return states
}

// ExampleSnapshotOf demonstrates reading an execution snapshot from a listener.
func ExampleSnapshotOf() {
	retryPolicy := retrypolicy.NewBuilder[any]().WithMaxRetries(2).Build()
	executor := failsafe.With[any](retryPolicy).
		WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			snapshot, ok := failsafe.SnapshotOf(event.ExecutionInfo)
			if !ok {
				return
			}
			fmt.Println("attempts:", snapshot.Attempts)
			fmt.Println("completed attempts:", len(snapshot.History))
		})

	attempts := 0
	_, _ = executor.Get(func() (any, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("connection refused")
		}
		return "ok", nil
	})

	// Output:
	// attempts: 3
	// completed attempts: 3
}

// snapshotMetrics is an example metrics sink that execution snapshots can be exported to. It is intentionally
// decoupled from any specific monitoring library.
type snapshotMetrics struct {
	recordAttempts     func(executionID uint64, attempts int)
	recordPlannedDelay func(executionID uint64, delay time.Duration)
	recordLastError    func(executionID uint64, err error)
}

func (m snapshotMetrics) export(snapshot failsafe.ExecutionSnapshot) {
	m.recordAttempts(snapshot.ID, snapshot.Attempts)
	m.recordPlannedDelay(snapshot.ID, snapshot.PlannedDelay)
	m.recordLastError(snapshot.ID, snapshot.LastError)
}

// ExampleSnapshotOf_metrics demonstrates exporting an execution snapshot to a metrics sink, without depending on any
// specific monitoring library.
func ExampleSnapshotOf_metrics() {
	metrics := snapshotMetrics{
		recordAttempts:     func(_ uint64, attempts int) { fmt.Println("attempts:", attempts) },
		recordPlannedDelay: func(_ uint64, delay time.Duration) { fmt.Println("planned delay:", delay) },
		recordLastError: func(_ uint64, err error) {
			if err != nil {
				fmt.Println("failed with:", err)
			}
		},
	}

	retryPolicy := retrypolicy.NewBuilder[any]().
		WithDelay(time.Second).
		WithMaxRetries(1).
		Build()
	executor := failsafe.With[any](retryPolicy).
		WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			if snapshot, ok := failsafe.SnapshotOf(event.ExecutionInfo); ok {
				metrics.export(snapshot)
			}
		})

	_, _ = executor.Get(func() (any, error) {
		return nil, errors.New("connection refused")
	})

	// Output:
	// attempts: 2
	// planned delay: 1s
	// failed with: retries exceeded. last result: <nil>, last error: connection refused
}
