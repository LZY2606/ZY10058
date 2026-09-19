package test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/bulkhead"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/failsafe-go/failsafe-go/hedgepolicy"
	"github.com/failsafe-go/failsafe-go/ratelimiter"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/failsafe-go/failsafe-go/timeout"
)

var errSnapshotTest = errors.New("snapshot test error")

// Asserts that snapshots are not recorded unless enabled.
func TestSnapshotDisabledByDefault(t *testing.T) {
	executor := failsafe.With[any](retrypolicy.NewWithDefaults[any]())
	var enabled atomic.Bool
	done := make(chan struct{})
	executor.OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
		_, ok := event.Snapshot()
		enabled.Store(ok)
		close(done)
	})

	require.NoError(t, executor.Run(func() error { return nil }))
	<-done
	assert.False(t, enabled.Load())
}

// Asserts that a retry execution records a stable execution ID, attempts in completion order, the planned delay,
// the last error, and retry policy state.
func TestSnapshotRetry(t *testing.T) {
	rp := retrypolicy.NewBuilder[any]().
		HandleErrors(errSnapshotTest).
		WithDelay(10 * time.Millisecond).
		WithMaxAttempts(3).
		Build()

	var snapshot failsafe.ExecutionSnapshot
	var ok bool
	executor := failsafe.With[any](rp).WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			snapshot, ok = event.Snapshot()
		})

	attempts := 0
	err := executor.Run(func() error {
		attempts++
		if attempts < 3 {
			return errSnapshotTest
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)

	assert.NotZero(t, snapshot.ID)
	assert.Equal(t, 3, snapshot.AttemptCount)
	assert.Equal(t, 2, snapshot.RetryCount)
	assert.Equal(t, 3, snapshot.ExecutionCount)
	assert.False(t, snapshot.Canceled)
	assert.Equal(t, failsafe.CancelSourceNone, snapshot.CancelSource)
	assert.ErrorIs(t, snapshot.LastError, errSnapshotTest)
	assert.GreaterOrEqual(t, snapshot.PlannedDelay, 10*time.Millisecond)
	assert.False(t, snapshot.StartedAt.IsZero())
	assert.Greater(t, snapshot.Elapsed, time.Duration(0))

	// Attempts are recorded in start order and complete in the same order
	require.Len(t, snapshot.Attempts, 3)
	for i, attempt := range snapshot.Attempts {
		assert.Equal(t, uint64(i+1), attempt.ID)
		assert.Zero(t, attempt.ParentID)
		assert.False(t, attempt.Hedge)
		assert.True(t, attempt.Completed())
		assert.Equal(t, i+1, attempt.CompletionOrder)
		assert.False(t, attempt.StartedAt.IsZero())
		assert.False(t, attempt.CompletedAt.IsZero())
		assert.Equal(t, failsafe.CancelSourceNone, attempt.CancelSource)
	}
	assert.ErrorIs(t, snapshot.Attempts[0].Err, errSnapshotTest)
	assert.NoError(t, snapshot.Attempts[2].Err)

	// Retry policy state is execution private
	rpSnapshot, found := snapshot.PolicySnapshot("RetryPolicy")
	require.True(t, found)
	assert.False(t, rpSnapshot.Shared)
	assert.Equal(t, 2, rpSnapshot.State["retries"])
}

// Asserts that a retry composed with a timeout records a timeout cancellation, distinguished from a context
// cancellation.
func TestSnapshotRetryWithTimeout(t *testing.T) {
	rp := retrypolicy.NewBuilder[any]().WithMaxAttempts(3).Build()
	to := timeout.New[any](50 * time.Millisecond)

	var snapshot failsafe.ExecutionSnapshot
	var ok bool
	executor := failsafe.With[any](to, rp).WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			snapshot, ok = event.Snapshot()
		})

	err := executor.Run(func() error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	require.ErrorIs(t, err, timeout.ErrExceeded)
	require.True(t, ok)

	assert.True(t, snapshot.Canceled)
	assert.Equal(t, failsafe.CancelSourceTimeout, snapshot.CancelSource)
	assert.ErrorIs(t, snapshot.LastError, timeout.ErrExceeded)

	toSnapshot, found := snapshot.PolicySnapshot("Timeout")
	require.True(t, found)
	assert.False(t, toSnapshot.Shared)
	assert.Equal(t, 50*time.Millisecond, toSnapshot.State["timeLimit"])

	// The timed out attempt records the timeout as its cancel source
	require.NotEmpty(t, snapshot.Attempts)
	assert.Equal(t, failsafe.CancelSourceTimeout, snapshot.Attempts[0].CancelSource)
}

// Asserts that cancellation by an external context is distinguished from a timeout.
func TestSnapshotContextCancel(t *testing.T) {
	rp := retrypolicy.NewBuilder[any]().WithMaxAttempts(3).Build()
	ctx, cancel := context.WithCancel(context.Background())

	var snapshot failsafe.ExecutionSnapshot
	var ok bool
	executor := failsafe.With[any](rp).WithContext(ctx).WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			snapshot, ok = event.Snapshot()
		})

	err := executor.Run(func() error {
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, ok)

	assert.True(t, snapshot.Canceled)
	assert.Equal(t, failsafe.CancelSourceContext, snapshot.CancelSource)
	require.NotEmpty(t, snapshot.Attempts)
	assert.Equal(t, failsafe.CancelSourceContext, snapshot.Attempts[0].CancelSource)
}

// Asserts that hedged attempts get unique IDs with a common parent, that the losing attempt's cancellation is
// recorded, and that completion order reflects the actual order of completion.
func TestSnapshotHedgeWinnerAndLoser(t *testing.T) {
	hp := hedgepolicy.NewBuilderWithDelay[string](10 * time.Millisecond).
		WithMaxHedges(1).
		Build()

	var event failsafe.ExecutionDoneEvent[string]
	executor := failsafe.With(hp).WithSnapshots(true).
		OnDone(func(e failsafe.ExecutionDoneEvent[string]) {
			event = e
		})

	result, err := executor.GetWithExecution(func(exec failsafe.Execution[string]) (string, error) {
		if !exec.IsHedge() {
			time.Sleep(200 * time.Millisecond)
			return "original", nil
		}
		return "hedged", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "hedged", result)

	snapshot, ok := event.Snapshot()
	require.True(t, ok)
	assert.Equal(t, 2, snapshot.AttemptCount)
	assert.Equal(t, 1, snapshot.HedgeCount)
	require.Len(t, snapshot.Attempts, 2)

	original, hedged := snapshot.Attempts[0], snapshot.Attempts[1]
	assert.NotEqual(t, original.ID, hedged.ID)
	assert.Zero(t, original.ParentID)
	assert.Equal(t, original.ID, hedged.ParentID)
	assert.False(t, original.Hedge)
	assert.True(t, hedged.Hedge)

	// The hedge won and the loser was canceled by the hedge policy
	assert.True(t, hedged.Completed())
	assert.Equal(t, 1, hedged.CompletionOrder)
	assert.Equal(t, failsafe.CancelSourceNone, hedged.CancelSource)
	assert.Equal(t, failsafe.CancelSourceHedge, original.CancelSource)

	// Hedge policy state
	hpSnapshot, found := snapshot.PolicySnapshot("HedgePolicy")
	require.True(t, found)
	assert.False(t, hpSnapshot.Shared)
	assert.Equal(t, 1, hpSnapshot.State["hedges"])

	// The loser completes later, and a fresh snapshot reflects the actual completion order
	time.Sleep(300 * time.Millisecond)
	later, ok := event.Snapshot()
	require.True(t, ok)
	require.Len(t, later.Attempts, 2)
	assert.True(t, later.Attempts[0].Completed())
	assert.Equal(t, 2, later.Attempts[0].CompletionOrder)
	assert.Equal(t, failsafe.CancelSourceHedge, later.Attempts[0].CancelSource)
	assert.Equal(t, snapshot.ID, later.ID)
}

// Asserts that circuit breaker state transitions are reflected in snapshots as shared state at the moment the
// snapshot is taken.
func TestSnapshotCircuitBreakerStateTransitions(t *testing.T) {
	cb := circuitbreaker.NewBuilder[any]().
		WithFailureThreshold(1).
		WithDelay(time.Minute).
		Build()

	snapshots := make(chan failsafe.ExecutionSnapshot, 10)
	executor := failsafe.With[any](cb).WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			if snapshot, ok := event.Snapshot(); ok {
				snapshots <- snapshot
			}
		})

	breakerState := func(snapshot failsafe.ExecutionSnapshot) string {
		cbSnapshot, found := snapshot.PolicySnapshot("CircuitBreaker")
		require.True(t, found)
		assert.True(t, cbSnapshot.Shared)
		return cbSnapshot.State["state"].(string)
	}

	// A failure opens the breaker
	require.Error(t, executor.Run(func() error { return errSnapshotTest }))
	assert.Equal(t, "open", breakerState(<-snapshots))

	// An open breaker blocks the execution before the attempt is executed
	require.ErrorIs(t, executor.Run(func() error {
		t.Fatal("should not execute")
		return nil
	}), circuitbreaker.ErrOpen)
	blocked := <-snapshots
	assert.Equal(t, "open", breakerState(blocked))
	require.Len(t, blocked.Attempts, 1)
	assert.False(t, blocked.Attempts[0].Completed())

	// Closing the breaker allows executions again
	cb.Close()
	require.NoError(t, executor.Run(func() error { return nil }))
	assert.Equal(t, "closed", breakerState(<-snapshots))
}

// Asserts that the key state of each composed policy is present in a snapshot, with shared state noted as such.
func TestSnapshotPolicyStates(t *testing.T) {
	rp := retrypolicy.NewBuilder[any]().WithMaxAttempts(3).WithDelay(time.Millisecond).Build()
	to := timeout.New[any](time.Second)
	bh := bulkhead.New[any](2)
	rl := ratelimiter.NewBursty[any](10, time.Second)
	cb := circuitbreaker.NewWithDefaults[any]()

	var snapshot failsafe.ExecutionSnapshot
	var ok bool
	executor := failsafe.With[any](rp, to, bh, rl, cb).WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			snapshot, ok = event.Snapshot()
		})

	require.NoError(t, executor.Run(func() error { return nil }))
	require.True(t, ok)

	expectedShared := map[string]bool{
		"RetryPolicy":    false,
		"Timeout":        false,
		"Bulkhead":       true,
		"RateLimiter":    true,
		"CircuitBreaker": true,
	}
	for name, shared := range expectedShared {
		policySnapshot, found := snapshot.PolicySnapshot(name)
		require.True(t, found, "expected policy %s", name)
		assert.Equal(t, shared, policySnapshot.Shared, "shared flag for %s", name)
		assert.NotEmpty(t, policySnapshot.State, "state for %s", name)
	}

	bhSnapshot, _ := snapshot.PolicySnapshot("Bulkhead")
	assert.Equal(t, 2, bhSnapshot.State["maxConcurrency"])
	rlSnapshot, _ := snapshot.PolicySnapshot("RateLimiter")
	assert.Contains(t, rlSnapshot.State, "availablePermits")
}

// Asserts that snapshots can be read concurrently with in-flight executions without data races.
func TestSnapshotConcurrentReads(t *testing.T) {
	rp := retrypolicy.NewBuilder[any]().WithDelay(time.Millisecond).WithMaxAttempts(3).Build()
	hp := hedgepolicy.NewWithDelay[any](time.Millisecond)
	cb := circuitbreaker.NewWithDefaults[any]()

	latest := &atomic.Pointer[failsafe.ExecutionDoneEvent[any]]{}
	executor := failsafe.With[any](rp, hp, cb).WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			latest.Store(&event)
		})

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Concurrent executions
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = executor.Get(func() (any, error) {
						time.Sleep(time.Millisecond)
						return nil, nil
					})
				}
			}
		}()
	}

	// Concurrent snapshot reads
	reads := &atomic.Int64{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if event := latest.Load(); event != nil {
						if snapshot, ok := event.Snapshot(); ok {
							reads.Add(1)
							_ = snapshot.Attempts
							_ = snapshot.Policies
						}
					}
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	assert.Greater(t, reads.Load(), int64(0))
}

// Asserts that reading a snapshot and triggering another execution from within a listener callback does not
// deadlock.
func TestSnapshotCallbackReentrancy(t *testing.T) {
	rp := retrypolicy.NewBuilder[any]().WithMaxAttempts(2).WithDelay(time.Millisecond).Build()

	var outerID, innerID uint64
	executor := failsafe.With[any](rp).WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			// Re-read the snapshot from within the callback
			snapshot, ok := event.Snapshot()
			require.True(t, ok)
			outerID = snapshot.ID

			// Trigger another execution from within the callback
			inner := failsafe.With[any](rp).WithSnapshots(true).
				OnDone(func(innerEvent failsafe.ExecutionDoneEvent[any]) {
					innerSnapshot, innerOK := innerEvent.Snapshot()
					require.True(t, innerOK)
					innerID = innerSnapshot.ID
				})
			require.NoError(t, inner.Run(func() error { return nil }))
		})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = executor.Run(func() error { return nil })
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: execution did not complete")
	}
	assert.NotZero(t, outerID)
	assert.NotZero(t, innerID)
	assert.NotEqual(t, outerID, innerID)
}

// Asserts that each execution gets a unique, stable snapshot ID.
func TestSnapshotStableExecutionID(t *testing.T) {
	ids := make(chan uint64, 2)
	executor := failsafe.With[any](retrypolicy.NewWithDefaults[any]()).WithSnapshots(true).
		OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
			snapshot, ok := event.Snapshot()
			require.True(t, ok)
			// The ID is stable across reads of the same execution
			again, ok := event.Snapshot()
			require.True(t, ok)
			assert.Equal(t, snapshot.ID, again.ID)
			ids <- snapshot.ID
		})

	require.NoError(t, executor.Run(func() error { return nil }))
	require.NoError(t, executor.Run(func() error { return nil }))
	assert.NotEqual(t, <-ids, <-ids)
}
