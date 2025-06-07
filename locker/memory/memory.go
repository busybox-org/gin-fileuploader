package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/busybox-org/gin-fileuploader/locker"
)

// MemoryLocker persists locks using memory and therefore allowing a simple and
// cheap mechanism. Locks will only exist as long as this object is kept in
// reference and will be erased if the program exits.
type MemoryLocker struct {
	locks map[string]lockEntry
	mutex sync.RWMutex
}

type lockEntry struct {
	lockReleased chan struct{}
}

// New creates a new in-memory locker.
func New() *MemoryLocker {
	return &MemoryLocker{
		locks: make(map[string]lockEntry),
	}
}

func (locker *MemoryLocker) NewLock(id string) (locker.ILock, error) {
	return memoryLock{locker, id}, nil
}

type memoryLock struct {
	locker *MemoryLocker
	id     string
}

// Lock tries to obtain the exclusive lock.
func (lock memoryLock) Lock(ctx context.Context) error {
	lock.locker.mutex.RLock()
	entry, ok := lock.locker.locks[lock.id]
	lock.locker.mutex.RUnlock()

requestRelease:
	if ok {
		select {
		case <-ctx.Done():
			return errors.New("lock request timed out")
		case <-entry.lockReleased:
		}
	}

	lock.locker.mutex.Lock()
	// Check that the lock has not already been created in the meantime
	entry, ok = lock.locker.locks[lock.id]
	if ok {
		// Lock has been created in the meantime, so we must wait again until it is free
		lock.locker.mutex.Unlock()
		goto requestRelease
	}

	// No lock exists, so we can create it
	entry = lockEntry{
		lockReleased: make(chan struct{}),
	}

	lock.locker.locks[lock.id] = entry
	lock.locker.mutex.Unlock()

	return nil
}

// Unlock releases a lock. If no such lock exists, no error will be returned.
func (lock memoryLock) Unlock() {
	lock.locker.mutex.Lock()
	lockReleased := lock.locker.locks[lock.id].lockReleased
	// Delete the lock entry entirely
	delete(lock.locker.locks, lock.id)
	lock.locker.mutex.Unlock()
	close(lockReleased)
	return
}
