package failsafe

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// CancelCause identifies the source of an execution or attempt cancellation.
type CancelCause int

const (
	// CancelNone indicates the execution or attempt was not canceled.
	CancelNone CancelCause = iota
	// CancelContext indicates cancellation by an external context.Context.
	CancelContext
	// CancelTimeout indicates cancellation by a timeout.Timeout.
	CancelTimeout
	// CancelHedge indicates cancellation of a hedge attempt because a concurrent attempt completed.
	CancelHedge
)

// String returns a human-readable representation of the CancelCause.
func (c CancelCause) String() string {
	switch c {
	case CancelContext:
		return "Context"
	case CancelTimeout:
		return "Timeout"
	case CancelHedge:
		return "Hedge"
	default:
		return "None"
	}
}

// AttemptSnapshot is a read-only, point-in-time record of a single execution attempt, meaning a single invocation of
// the user-provided func. Attempts that were blocked before executing, such as by an open CircuitBreaker or an
// exhausted RateLimiter, are counted in ExecutionSnapshot.Attempts but do not appear in the history.
type AttemptSnapshot struct {
	// ID uniquely identifies the attempt.
	ID uint64
	// ParentID is the ID of the outer execution that the attempt belongs to.
	ParentID uint64
	// Hedge indicates the attempt was a hedged attempt.
	Hedge bool
	// StartTime is the time the attempt started at.
	StartTime time.Time
	// Completed indicates whether the attempt finished before the snapshot was taken.
	Completed bool
	// CompletionOrder is the 1-based order in which the attempt completed relative to the other attempts of the
	// execution, reflecting the actual completion order. It is 0 while the attempt is still in progress.
	CompletionOrder int
	// Duration is how long the attempt took. For attempts still in progress, it is the elapsed time at the moment the
	// snapshot was taken.
	Duration time.Duration
	// Err is the error the attempt completed with, else nil.
	Err error
	// Canceled indicates whether the attempt was canceled.
	Canceled bool
	// CancelCause identifies the source of the cancellation, allowing a timeout.Timeout to be distinguished from an
	// external context cancellation and from a losing hedge attempt.
	CancelCause CancelCause
}

// PolicySnapshot is a read-only, point-in-time view of a policy's key state.
type PolicySnapshot struct {
	// Policy is the name of the policy type, such as "RetryPolicy" or "CircuitBreaker".
	Policy string
	// Shared indicates the state is shared with other concurrent executions of the same policy instance, such as a
	// CircuitBreaker or RateLimiter, and was captured at the moment the snapshot was taken. It is not private to the
	// execution the snapshot belongs to.
	Shared bool
	// State is a short, human-readable description of the policy's key state at the moment the snapshot was taken.
	State string
}

// ExecutionSnapshot is a read-only, point-in-time view of a single outer execution, including its retries and hedges.
// Snapshots are only available when enabled via Executor.WithSnapshots, and can be read from event listeners or the
// user-provided func via SnapshotOf. Reading a snapshot never holds a lock that policy progress waits on, so it is
// safe to read a snapshot from within a listener, including reentrantly, or while triggering another execution.
type ExecutionSnapshot struct {
	// ID uniquely identifies the outer execution.
	ID uint64
	// Attempts is the number of execution attempts so far, including attempts in progress and attempts that were
	// blocked before being executed.
	Attempts int
	// Retries is the number of retries so far, including retries in progress.
	Retries int
	// Hedges is the number of hedges so far, including hedges in progress.
	Hedges int
	// Executions is the number of completed executions.
	Executions int
	// StartTime is the time the initial execution attempt started at.
	StartTime time.Time
	// ElapsedTime is the elapsed time since the initial execution attempt began, at the moment the snapshot was taken.
	ElapsedTime time.Duration
	// PlannedDelay is the most recently planned delay before a following attempt, such as a retry or hedge delay, else
	// 0 if no delay was planned.
	PlannedDelay time.Duration
	// LastError is the last error that occurred, else nil.
	LastError error
	// Canceled indicates whether the execution was canceled, either by an external context.Context or a
	// timeout.Timeout.
	Canceled bool
	// CancelCause identifies the source of the cancellation.
	CancelCause CancelCause
	// History contains a record for each attempt of the execution, in the order the attempts started. Concurrent
	// attempts, such as hedges, each have their own ID and share the same ParentID. Their CompletionOrder reflects
	// the order in which they actually completed.
	History []AttemptSnapshot
	// Policies contains the key state of each composed policy at the moment the snapshot was taken. Entries marked
	// Shared describe state that is shared across executions of the same policy instance.
	Policies []PolicySnapshot
}

// PolicySnapshotter is implemented by policies that contribute their key state to an ExecutionSnapshot.
type PolicySnapshotter interface {
	// SnapshotState returns a point-in-time view of the policy's key state. It must be safe to call concurrently with
	// executions and must not block on locks that policy progress waits on for a meaningful amount of time.
	SnapshotState() PolicySnapshot
}

// SnapshotOf returns a read-only, point-in-time snapshot of the execution that info belongs to. The second return
// value is false when snapshots are not enabled for the execution, which is the default. See Executor.WithSnapshots.
//
// The returned snapshot is a copy and is safe to use without synchronization. Taking a snapshot never holds a lock
// that policy progress waits on, so it cannot deadlock with policy execution, including when called reentrantly from
// within event listeners or when another execution is triggered from the same callback.
func SnapshotOf(info ExecutionInfo) (ExecutionSnapshot, bool) {
	if info == nil {
		return ExecutionSnapshot{}, false
	}
	if s, ok := info.(interface {
		snapshot() (ExecutionSnapshot, bool)
	}); ok {
		return s.snapshot()
	}
	return ExecutionSnapshot{}, false
}

// snapshotIDs allocates IDs for executions and their attempts from a single sequence so that both are unique.
var snapshotIDs atomic.Uint64

// errorBox allows an error to be stored atomically.
type errorBox struct{ err error }

// snapshotRecorder records the state that ExecutionSnapshots are built from. A recorder is only created when
// snapshots are enabled for an execution, and is shared by all the copies of that execution, including hedge
// attempts. All mutation methods are safe for concurrent use and never call user or policy code while holding mu,
// making mu a leaf lock that cannot participate in lock cycles.
type snapshotRecorder struct {
	id        uint64
	outerCtx  context.Context
	startTime time.Time

	// mu guards attempts and completed. It is a leaf lock: it is only ever held briefly to read or mutate the
	// attempt history, and is never held while acquiring another lock or calling user code.
	mu        sync.Mutex
	attempts  []AttemptSnapshot
	completed int

	plannedDelayNanos atomic.Int64
	lastErr           atomic.Pointer[errorBox]
	cause             atomic.Int32

	// providers is set once before the execution starts and is read-only afterwards.
	providers []PolicySnapshotter
}

func newSnapshotRecorder(outerCtx context.Context) *snapshotRecorder {
	return &snapshotRecorder{
		id:        snapshotIDs.Add(1),
		outerCtx:  outerCtx,
		startTime: time.Now(),
	}
}

// recordAttemptStart records the start of an attempt and returns its ID.
func (r *snapshotRecorder) recordAttemptStart(hedge bool) uint64 {
	id := snapshotIDs.Add(1)
	r.mu.Lock()
	r.attempts = append(r.attempts, AttemptSnapshot{
		ID:        id,
		ParentID:  r.id,
		Hedge:     hedge,
		StartTime: time.Now(),
	})
	r.mu.Unlock()
	return id
}

// recordAttemptComplete marks the attempt as completed, assigning its completion order. It is a no-op if the attempt
// was already completed.
func (r *snapshotRecorder) recordAttemptComplete(id uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.attempts) - 1; i >= 0; i-- {
		a := &r.attempts[i]
		if a.ID == id {
			if !a.Completed {
				a.Completed = true
				r.completed++
				a.CompletionOrder = r.completed
				a.Duration = time.Since(a.StartTime)
				if a.Err == nil {
					a.Err = err
				}
			}
			return
		}
	}
}

// cancelAttempt marks the attempt as canceled, without completing it.
func (r *snapshotRecorder) cancelAttempt(id uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.attempts) - 1; i >= 0; i-- {
		a := &r.attempts[i]
		if a.ID == id {
			a.Canceled = true
			if a.Err == nil {
				a.Err = err
			}
			return
		}
	}
}

// setAttemptCause records the cancellation cause of the attempt, if none was recorded yet.
func (r *snapshotRecorder) setAttemptCause(id uint64, cause CancelCause) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.attempts) - 1; i >= 0; i-- {
		a := &r.attempts[i]
		if a.ID == id {
			if a.CancelCause == CancelNone {
				a.CancelCause = cause
			}
			return
		}
	}
}

// setCause records the cancellation cause of the overall execution, if none was recorded yet.
func (r *snapshotRecorder) setCause(cause CancelCause) {
	r.cause.CompareAndSwap(int32(CancelNone), int32(cause))
}

func (r *snapshotRecorder) setLastError(err error) {
	if err != nil {
		r.lastErr.Store(&errorBox{err: err})
	}
}

// build returns a snapshot of the execution. It only briefly holds the recorder's leaf lock and reads atomics, so it
// never blocks on a lock that policy progress waits on.
func (r *snapshotRecorder) build(attempts, retries, hedges, executions int) ExecutionSnapshot {
	now := time.Now()

	r.mu.Lock()
	history := make([]AttemptSnapshot, len(r.attempts))
	copy(history, r.attempts)
	r.mu.Unlock()

	for i := range history {
		a := &history[i]
		if !a.Completed {
			a.Duration = now.Sub(a.StartTime)
		}
		if a.CancelCause == CancelNone && a.Canceled && isContextError(a.Err) {
			a.CancelCause = CancelContext
		}
	}

	var lastErr error
	if box := r.lastErr.Load(); box != nil {
		lastErr = box.err
	}
	canceled := r.outerCtx.Err() != nil
	if lastErr == nil && canceled {
		lastErr = r.outerCtx.Err()
	}
	cause := CancelCause(r.cause.Load())
	if cause == CancelNone && canceled {
		cause = CancelContext
	}

	snapshot := ExecutionSnapshot{
		ID:           r.id,
		Attempts:     attempts,
		Retries:      retries,
		Hedges:       hedges,
		Executions:   executions,
		StartTime:    r.startTime,
		ElapsedTime:  now.Sub(r.startTime),
		PlannedDelay: time.Duration(r.plannedDelayNanos.Load()),
		LastError:    lastErr,
		Canceled:     canceled,
		CancelCause:  cause,
		History:      history,
	}
	for _, provider := range r.providers {
		snapshot.Policies = append(snapshot.Policies, provider.SnapshotState())
	}
	return snapshot
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
