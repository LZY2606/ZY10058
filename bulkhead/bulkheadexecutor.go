package bulkhead

import (
	"errors"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/common"
	"github.com/failsafe-go/failsafe-go/internal"
	"github.com/failsafe-go/failsafe-go/policy"
)

// executor is a policy.Executor that handles failures according to a Bulkhead.
type executor[R any] struct {
	policy.BaseExecutor[R]
	*bulkhead[R]
}

var _ policy.Executor[any] = &executor[any]{}

func (e *executor[R]) PreExecute(exec policy.ExecutionInternal[R]) *common.PolicyResult[R] {
	if rec, ok := exec.(failsafe.SnapshotRecorder); ok {
		if tracker := rec.SnapshotTracker(); tracker != nil {
			// Bulkhead state is shared with other concurrent executions, and reflects the state at the moment a
			// snapshot is taken
			tracker.RegisterPolicy("Bulkhead", true, func() map[string]any {
				return map[string]any{
					"maxConcurrency": cap(e.semaphore),
					"inUse":          len(e.semaphore),
				}
			})
		}
	}
	if err := e.AcquirePermitWithMaxWait(exec.Context(), e.maxWaitTime); err != nil {
		// Check for cancellation while waiting for a permit
		if canceled, cancelResult := exec.IsCanceledWithResult(); canceled {
			return cancelResult
		}
		if e.onFull != nil && errors.Is(err, ErrFull) {
			e.onFull(failsafe.ExecutionEvent[R]{
				ExecutionAttempt: exec,
			})
		}
		return internal.FailureResult[R](err)
	}
	return nil
}

func (e *executor[R]) PostExecute(_ policy.ExecutionInternal[R], result *common.PolicyResult[R]) *common.PolicyResult[R] {
	e.bulkhead.ReleasePermit()
	return result
}
