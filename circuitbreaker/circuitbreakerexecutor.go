package circuitbreaker

import (
	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/common"
	"github.com/failsafe-go/failsafe-go/internal"
	"github.com/failsafe-go/failsafe-go/policy"
)

// executor is a policy.Executor that handles failures according to a CircuitBreaker.
type executor[R any] struct {
	policy.BaseExecutor[R]
	*circuitBreaker[R]
}

var _ policy.Executor[any] = &executor[any]{}

func (e *executor[R]) PreExecute(exec policy.ExecutionInternal[R]) *common.PolicyResult[R] {
	if rec, ok := exec.(failsafe.SnapshotRecorder); ok {
		if tracker := rec.SnapshotTracker(); tracker != nil {
			// Circuit breaker state is shared with other concurrent executions, and reflects the state at the moment a
			// snapshot is taken
			tracker.RegisterPolicy("CircuitBreaker", true, func() map[string]any {
				return map[string]any{"state": e.State().String()}
			})
		}
	}
	if !e.TryAcquirePermit() {
		return internal.FailureResult[R](ErrOpen)
	}
	return nil
}

func (e *executor[R]) OnSuccess(exec policy.ExecutionInternal[R], result *common.PolicyResult[R]) {
	e.BaseExecutor.OnSuccess(exec, result)
	e.RecordSuccess()
}

func (e *executor[R]) OnFailure(exec policy.ExecutionInternal[R], result *common.PolicyResult[R]) *common.PolicyResult[R] {
	e.BaseExecutor.OnFailure(exec, result)
	e.mu.Lock()
	defer e.mu.Unlock()

	// Wrap the result in the execution, so it's available when computing a delay
	e.recordFailure(exec.CopyWithResult(result))
	return result
}
