package failsafe_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
)

// This example enables snapshot recording for an executor and reads a snapshot of each execution from a listener.
func ExampleExecutor_WithSnapshots() {
	retryPolicy := retrypolicy.NewBuilder[any]().
		WithMaxAttempts(3).
		WithDelay(time.Millisecond).
		Build()
	executor := failsafe.With[any](retryPolicy).WithSnapshots(true)
	executor.OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
		if snapshot, ok := event.Snapshot(); ok {
			fmt.Println("attempts:", snapshot.AttemptCount)
			fmt.Println("retries:", snapshot.RetryCount)
			fmt.Println("cancel source:", snapshot.CancelSource)
		}
	})

	attempts := 0
	_ = executor.Run(func() error {
		attempts++
		if attempts < 3 {
			return errors.New("connection refused")
		}
		return nil
	})

	// Output:
	// attempts: 3
	// retries: 2
	// cancel source: none
}

// snapshotMetrics is a minimal metrics sink that execution snapshots are exported to. It can be adapted to any
// monitoring library, such as Prometheus or OpenTelemetry, by mapping the exported fields to that library's counter
// and gauge types.
type snapshotMetrics struct {
	executions    int
	attempts      int
	retries       int
	cancellations map[string]int
}

// export records an ExecutionSnapshot into the metrics sink.
func (m *snapshotMetrics) export(snapshot failsafe.ExecutionSnapshot) {
	m.executions++
	m.attempts += snapshot.AttemptCount
	m.retries += snapshot.RetryCount
	if snapshot.Canceled {
		m.cancellations[snapshot.CancelSource.String()]++
	}
}

// This example exports execution snapshots to a metrics sink from a listener, without depending on any particular
// monitoring library.
func ExampleExecutionSnapshot_metrics() {
	metrics := &snapshotMetrics{cancellations: map[string]int{}}
	retryPolicy := retrypolicy.NewBuilder[any]().
		WithMaxAttempts(2).
		WithDelay(time.Millisecond).
		Build()
	executor := failsafe.With[any](retryPolicy).WithSnapshots(true)
	executor.OnDone(func(event failsafe.ExecutionDoneEvent[any]) {
		if snapshot, ok := event.Snapshot(); ok {
			metrics.export(snapshot)
		}
	})

	// An execution that always fails and is retried once
	_ = executor.Run(func() error {
		return errors.New("connection refused")
	})

	fmt.Println("executions:", metrics.executions)
	fmt.Println("attempts:", metrics.attempts)
	fmt.Println("retries:", metrics.retries)

	// Output:
	// executions: 1
	// attempts: 2
	// retries: 1
}
