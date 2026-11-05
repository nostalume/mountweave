package core

import (
	"context"
	"errors"
	"io"
	"sync"
)

// Mount is the identity of one installed generation. Retiring an old handle
// never retires a later replacement installed at the same virtual path.
type Mount struct {
	core                 *Core
	path                 string
	driver               Driver
	keys                 bool
	projection           KeyProjection
	revision             uint64
	uses                 map[*use]struct{}
	retiring, closing    bool
	done                 chan struct{}
	closeErr, cleanupErr error
}

type use struct {
	*location
	ctx            context.Context
	cancel         context.CancelFunc
	once           sync.Once
	failedResource io.Closer
}

func (m *Mount) borrowLocked(parent context.Context, at *location) *use {
	ctx, cancel := context.WithCancel(parent)
	u := &use{location: at, ctx: ctx, cancel: cancel}
	m.uses[u] = struct{}{}
	return u
}

func (u *use) release() {
	u.once.Do(func() {
		u.cancel()
		c := u.mount.core
		c.mu.Lock()
		delete(u.mount.uses, u)
		u.mount.startCloseLocked()
		c.mu.Unlock()
	})
}

func (u *use) failed(err error, resource io.Closer) {
	c := u.mount.core
	c.mu.Lock()
	u.failedResource = resource
	u.mount.cleanupErr = errors.Join(u.mount.cleanupErr, err)
	c.mu.Unlock()
}

func (m *Mount) startCloseLocked() {
	if !m.retiring || m.closing || len(m.uses) != 0 {
		return
	}
	m.closing = true
	go func() {
		err := m.driver.Close()
		c := m.core
		c.mu.Lock()
		m.closeErr = err
		if err == nil && c.mounts[m.path] == m {
			delete(c.mounts, m.path)
			for prefix, b := range c.bindings {
				if b.mount == m {
					b.active = false
					delete(c.bindings, prefix)
				}
			}
			c.version++
		}
		close(m.done)
		c.mu.Unlock()
	}()
}

func (m *Mount) beginRetire() {
	c := m.core
	c.mu.Lock()
	if m.retiring {
		c.mu.Unlock()
		return
	}
	m.retiring = true
	c.version++
	uses := make([]*use, 0, len(m.uses))
	for u := range m.uses {
		uses = append(uses, u)
	}
	cursors := make([]*session, 0)
	for s := range c.sessions {
		if s.mount == m {
			cursors = append(cursors, s)
		}
	}
	m.startCloseLocked()
	c.mu.Unlock()
	for _, u := range uses {
		u.cancel()
	}
	for _, s := range cursors {
		s.invalidate()
	}
}

// Retire stops admission and requests cancellation immediately. Its context
// bounds this wait, not the lifetime of unfinished work or Driver.Close.
func (m *Mount) Retire(ctx context.Context) error { m.beginRetire(); return m.wait(ctx) }

func (m *Mount) wait(ctx context.Context) error {
	select {
	case <-m.done:
		return m.result()
	default:
	}
	select {
	case <-m.done:
		return m.result()
	case <-ctx.Done():
		return errors.Join(ctx.Err(), m.result())
	}
}

func (m *Mount) result() error {
	c := m.core
	c.mu.Lock()
	defer c.mu.Unlock()
	return errors.Join(m.closeErr, m.cleanupErr)
}

type MountStatus struct {
	Active, Retiring, Done bool
	Err                    error
}

// Status observes this exact generation, even after its prefix is replaced.
func (m *Mount) Status() MountStatus {
	c := m.core
	c.mu.Lock()
	defer c.mu.Unlock()
	s := MountStatus{Active: !c.closed && c.mounts[m.path] == m && !m.retiring,
		Retiring: m.retiring, Err: errors.Join(m.closeErr, m.cleanupErr)}
	select {
	case <-m.done:
		s.Done = true
	default:
	}
	return s
}

func requestScope(ctx, lifetime context.Context) (context.Context, func()) {
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifetime, cancel)
	if lifetime.Err() != nil {
		cancel()
	}
	return child, func() { stop(); cancel() }
}
