package failsafe

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// CancelSource identifies what canceled an execution or an individual execution attempt.
type CancelSource int

const (
	// CancelSourceNone indicates that no cancellation was recorded.
	CancelSourceNone CancelSource = iota

	// CancelSourceContext indicates that the execution was canceled by its context, such as when an external
	// context.Context that the executor was configured with is canceled or exceeds its deadline.
	CancelSourceContext

	// CancelSourceTimeout indicates that the execution attempt was canceled by a timeout.Timeout policy.
	CancelSourceTimeout

	// CancelSourceHedge indicates that the execution attempt was canceled by a hedgepolicy.HedgePolicy because a
	// sibling hedged attempt completed first.
	CancelSourceHedge
)

// String returns a human readable representation of the CancelSource.
func (s CancelSource) String() string {
	switch s {
	case CancelSourceContext:
		return "context"
	case CancelSourceTimeout:
		return "timeout"
	case CancelSourceHedge:
		return "hedge"
	default:
		return "none"
	}
}

// AttemptSnapshot is a read-only snapshot of a single execution attempt, captured as part of an ExecutionSnapshot.
// Attempts include the initial execution along with retries and hedges. Attempts that were blocked before being
// executed, such as by an open CircuitBreaker, are started but never completed.
type AttemptSnapshot struct {
	// ID uniquely identifies the attempt within the execution.
	ID uint64

	// ParentID identifies the attempt that this attempt was derived from. It is 0 for the initial attempt and for
	// retries, and is the ID of the originating attempt for hedged attempts.
	ParentID uint64

	// Hedge indicates whether the attempt was started by a hedgepolicy.HedgePolicy.
	Hedge bool

	// StartedAt is the time that the attempt started at.
	StartedAt time.Time

	// CompletedAt is the time that the attempt completed at, else the zero value if the attempt is still in progress.
	CompletedAt time.Time

	// CompletionOrder is the 1-based order in which the attempt completed relative to other attempts in the same
	// execution, reflecting the actual completion order. It is 0 if the attempt has not completed yet.
	CompletionOrder int

	// Err is the error that the attempt completed with, else nil.
	Err error

	// CancelSource indicates what canceled the attempt, else CancelSourceNone.
	CancelSource CancelSource
}

// Completed returns whether the attempt has completed.
func (a AttemptSnapshot) Completed() bool {
	return a.CompletionOrder != 0
}

// PolicySnapshot is a read-only snapshot of a policy's key state, captured at the moment an ExecutionSnapshot is
// taken.
type PolicySnapshot struct {
	// Name is the name of the policy, such as "RetryPolicy" or "CircuitBreaker".
	Name string

	// Shared indicates whether the policy state is shared with other concurrent executions, such as with a
	// CircuitBreaker, RateLimiter, or Bulkhead. When Shared is true, the State reflects the policy's shared state at
	// the moment the snapshot was taken, and is not private to the execution the snapshot belongs to.
	Shared bool

	// State contains the policy's key state at the moment the snapshot was taken.
	State map[string]any
}

// ExecutionSnapshot is a read-only snapshot of a single outer execution, including its attempts, planned delays,
// cancellation, and the key state of each composed policy. Snapshots are only recorded when enabled via
// Executor.WithSnapshots, and can be read from event listeners via ExecutionEvent.Snapshot,
// ExecutionScheduledEvent.Snapshot, and ExecutionDoneEvent.Snapshot, or via SnapshotOf.
//
// Taking a snapshot never blocks policy progress: snapshots are assembled from dedicated tracker state and atomics
// rather than the locks that policies use to coordinate execution. It is safe to take a snapshot from within a
// listener callback, including reentrantly or while triggering another execution.
type ExecutionSnapshot struct {
	// ID is the stable identifier of the outer execution. It is unique across executions and shared by all attempts
	// and snapshots that belong to the same execution.
	ID uint64

	// StartedAt is the time that the initial execution attempt started at.
	StartedAt time.Time

	// Elapsed is the elapsed time between when the initial execution attempt began and when the snapshot was taken.
	Elapsed time.Duration

	// AttemptCount is the number of execution attempts so far, including attempts in progress and attempts that were
	// blocked before being executed.
	AttemptCount int

	// RetryCount is the number of retries so far, including retries in progress.
	RetryCount int

	// HedgeCount is the number of hedges so far, including hedges in progress.
	HedgeCount int

	// ExecutionCount is the number of completed executions.
	ExecutionCount int

	// PlannedDelay is the delay that was most recently planned before the next execution attempt, such as by a
	// retrypolicy.RetryPolicy, else 0 if no delay was planned.
	PlannedDelay time.Duration

	// LastError is the last error that was recorded for the execution, else nil.
	LastError error

	// Canceled indicates whether the execution was canceled, either by an external context.Context or by a
	// timeout.Timeout.
	Canceled bool

	// CancelSource indicates what canceled the execution, else CancelSourceNone. Cancellation by a timeout.Timeout is
	// distinguished from cancellation by an external context.Context.
	CancelSource CancelSource

	// Attempts contains a snapshot of each attempt, in the order the attempts were started. See AttemptSnapshot for
	// the actual completion order.
	Attempts []AttemptSnapshot

	// Policies contains a snapshot of each composed policy's key state at the moment the snapshot was taken.
	Policies []PolicySnapshot
}

// PolicySnapshot returns the snapshot for the named policy, else false if the execution was not composed with a
// policy by that name.
func (s ExecutionSnapshot) PolicySnapshot(name string) (PolicySnapshot, bool) {
	for _, p := range s.Policies {
		if p.Name == name {
			return p, true
		}
	}
	return PolicySnapshot{}, false
}

// SnapshotOf returns a snapshot of the execution associated with the info, such as from an ExecutionEvent or
// ExecutionDoneEvent, and whether snapshot recording was enabled for the execution. Snapshot recording is disabled
// by default and can be enabled via Executor.WithSnapshots.
func SnapshotOf(info ExecutionInfo) (ExecutionSnapshot, bool) {
	if s, ok := info.(interface {
		Snapshot() (ExecutionSnapshot, bool)
	}); ok {
		return s.Snapshot()
	}
	return ExecutionSnapshot{}, false
}

// SnapshotRecorder is implemented by executions that record snapshot events. It allows policies to record
// cancellation sources and to access the SnapshotTracker for an execution. SnapshotRecorder is only implemented
// when snapshot recording is enabled via Executor.WithSnapshots, and SnapshotTracker returns nil when it is not.
type SnapshotRecorder interface {
	// SnapshotTracker returns the SnapshotTracker for the execution, else nil if snapshot recording is not enabled.
	SnapshotTracker() *SnapshotTracker

	// RecordSnapshotCancel records that the execution's current attempt was canceled by the source. It should be
	// called by a policy before it cancels an execution, and is a no-op if snapshot recording is not enabled.
	RecordSnapshotCancel(source CancelSource)
}

// SnapshotTracker records snapshot events for a single outer execution. A tracker is created for each execution when
// snapshot recording is enabled via Executor.WithSnapshots, and is shared by all the execution's attempts, including
// retries and hedges. Policies can register state providers via RegisterPolicy, which are read each time a snapshot
// is taken.
//
// This type is concurrency safe.
type SnapshotTracker struct {
	id        uint64
	startTime time.Time

	plannedDelay     atomic.Int64 // time.Duration
	execCancelSource atomic.Int32 // CancelSource

	// Guarded by mu. This lock is only ever held for short, internal critical sections, and is never held while
	// calling policy state providers or user listeners, so snapshot reads cannot block policy progress.
	mu             sync.Mutex
	nextAttemptID  uint64
	attempts       []attemptRecord
	attemptIndex   map[uint64]int // attempt ID -> index in attempts
	completedCount int
	lastErr        error // The final execution error, if any

	providersMu sync.RWMutex
	providers   []policyStateProvider
}

type attemptRecord struct {
	id              uint64
	parentID        uint64
	hedge           bool
	startedAt       time.Time
	completedAt     time.Time
	completionOrder int
	err             error
	cancelSource    CancelSource
}

type policyStateProvider struct {
	name   string
	shared bool
	state  func() map[string]any
}

var snapshotExecutionID atomic.Uint64

func newSnapshotTracker() *SnapshotTracker {
	return &SnapshotTracker{
		id:           snapshotExecutionID.Add(1),
		startTime:    time.Now(),
		attemptIndex: make(map[uint64]int),
	}
}

// ID returns the stable identifier of the execution that the tracker belongs to.
func (t *SnapshotTracker) ID() uint64 {
	return t.id
}

// RegisterPolicy registers a provider for the policy's key state, which is read each time a snapshot of the
// execution is taken. The shared parameter indicates whether the policy's state is shared with other concurrent
// executions, such as with a CircuitBreaker or RateLimiter, in which case snapshots will note that the state
// reflects the policy's shared state at the moment the snapshot was taken. The state func must be safe to call
// concurrently and must not block on user callbacks. Registering a policy with the same name twice is a no-op.
func (t *SnapshotTracker) RegisterPolicy(name string, shared bool, state func() map[string]any) {
	t.providersMu.Lock()
	defer t.providersMu.Unlock()
	for _, p := range t.providers {
		if p.name == name {
			return
		}
	}
	t.providers = append(t.providers, policyStateProvider{name: name, shared: shared, state: state})
}

// RecordPlannedDelay records the delay that a policy, such as a retrypolicy.RetryPolicy, plans to wait before the
// next execution attempt.
func (t *SnapshotTracker) RecordPlannedDelay(delay time.Duration) {
	t.plannedDelay.Store(int64(delay))
}

// startAttempt records the start of an attempt and returns its ID, which is unique within the execution.
func (t *SnapshotTracker) startAttempt(parentID uint64, hedge bool) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextAttemptID++
	id := t.nextAttemptID
	t.attempts = append(t.attempts, attemptRecord{
		id:        id,
		parentID:  parentID,
		hedge:     hedge,
		startedAt: time.Now(),
	})
	t.attemptIndex[id] = len(t.attempts) - 1
	return id
}

// completeAttempt records the completion of an attempt, in the order that completions actually occur.
func (t *SnapshotTracker) completeAttempt(attemptID uint64, err error, ctxErr error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	idx, ok := t.attemptIndex[attemptID]
	if !ok {
		return
	}
	rec := &t.attempts[idx]
	if rec.completionOrder == 0 {
		t.completedCount++
		rec.completionOrder = t.completedCount
		rec.completedAt = time.Now()
		rec.err = err
		if rec.cancelSource == CancelSourceNone && ctxErr != nil {
			rec.cancelSource = CancelSourceContext
		}
	}
}

// recordCancel records that an attempt was canceled by the source. Cancellation by a timeout.Timeout is also
// recorded for the overall execution, since it cancels the execution as a whole.
func (t *SnapshotTracker) recordCancel(attemptID uint64, source CancelSource) {
	t.mu.Lock()
	if idx, ok := t.attemptIndex[attemptID]; ok {
		t.attempts[idx].cancelSource = source
	}
	t.mu.Unlock()
	if source == CancelSourceTimeout {
		t.execCancelSource.Store(int32(source))
	}
}

// recordLastError records the final error of the execution, such as when a policy cancels an execution with an
// error that the execution's own last error would not reflect.
func (t *SnapshotTracker) recordLastError(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastErr = err
}

// executionState provides the live execution state that a snapshot is assembled from.
type executionState interface {
	Attempts() int
	Retries() int
	Hedges() int
	Executions() int
	LastError() error
	IsCanceled() bool
	Context() context.Context
}

// snapshot assembles an ExecutionSnapshot from the tracker and the live execution state. It does not acquire any
// locks that policies use to coordinate execution progress.
func (t *SnapshotTracker) snapshot(exec executionState) ExecutionSnapshot {
	now := time.Now()

	t.mu.Lock()
	attempts := make([]AttemptSnapshot, len(t.attempts))
	for i, rec := range t.attempts {
		attempts[i] = AttemptSnapshot{
			ID:              rec.id,
			ParentID:        rec.parentID,
			Hedge:           rec.hedge,
			StartedAt:       rec.startedAt,
			CompletedAt:     rec.completedAt,
			CompletionOrder: rec.completionOrder,
			Err:             rec.err,
			CancelSource:    rec.cancelSource,
		}
	}
	lastErr := t.lastErr
	t.mu.Unlock()

	t.providersMu.RLock()
	providers := make([]policyStateProvider, len(t.providers))
	copy(providers, t.providers)
	t.providersMu.RUnlock()

	policies := make([]PolicySnapshot, 0, len(providers))
	for _, p := range providers {
		policies = append(policies, PolicySnapshot{
			Name:   p.name,
			Shared: p.shared,
			State:  p.state(),
		})
	}

	cancelSource := CancelSource(t.execCancelSource.Load())
	canceled := exec.IsCanceled() || cancelSource != CancelSourceNone
	if cancelSource == CancelSourceNone && exec.Context().Err() != nil {
		cancelSource = CancelSourceContext
	}
	if lastErr == nil {
		lastErr = exec.LastError()
	}

	return ExecutionSnapshot{
		ID:             t.id,
		StartedAt:      t.startTime,
		Elapsed:        now.Sub(t.startTime),
		AttemptCount:   exec.Attempts(),
		RetryCount:     exec.Retries(),
		HedgeCount:     exec.Hedges(),
		ExecutionCount: exec.Executions(),
		PlannedDelay:   time.Duration(t.plannedDelay.Load()),
		LastError:      lastErr,
		Canceled:       canceled,
		CancelSource:   cancelSource,
		Attempts:       attempts,
		Policies:       policies,
	}
}
