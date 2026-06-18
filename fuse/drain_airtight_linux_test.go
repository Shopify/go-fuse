// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"bytes"
	"context"
	"sync"
	"syscall"
	"testing"
	"time"
)

type drainSignalFS struct {
	RawFileSystem
	called chan struct{}
	once   sync.Once
}

func newDrainSignalFS() *drainSignalFS {
	return &drainSignalFS{
		RawFileSystem: NewDefaultRawFileSystem(),
		called:        make(chan struct{}),
	}
}

func (fs *drainSignalFS) GetAttr(cancel <-chan struct{}, in *GetAttrIn, out *AttrOut) Status {
	fs.once.Do(func() { close(fs.called) })
	return OK
}

// TestDrainInflightAirtightWakesParkedReader verifies the airtight property of a
// graceful hand-off: a reader parked waiting on the FUSE connection (the steady
// state of a live mount) must be woken by DrainInflight so the read loop exits,
// and a request that arrives *after* DrainInflight returns must NOT be dequeued
// -- it stays queued in the kernel for the successor server. Before the poll +
// eventfd wakeup, the parked reader stayed in a bare blocking read and would
// consume that late request, orphaning it across the hand-off (D-state hang).
func TestDrainInflightAirtightWakesParkedReader(t *testing.T) {
	fs := newDrainSignalFS()
	srv, peerFd, serveDone := newDrainTestServerWithDone(t, fs)

	// No request written: the single reader is parked waiting for the connection
	// to become readable. Give Serve a moment to reach that parked state.
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.DrainInflight(ctx); err != nil {
		t.Fatalf("DrainInflight returned %v; parked reader was not woken out of the read", err)
	}
	// The read loop must have exited (and the fd must be preserved, not closed).
	waitForServeDone(t, serveDone)

	// A request arriving after drain returned must not be processed by the drained
	// server; it stays queued for the successor.
	writeGetAttrRequest(t, peerFd, 1)
	select {
	case <-fs.called:
		t.Fatalf("drained server dequeued a request that arrived after DrainInflight returned")
	case <-time.After(150 * time.Millisecond):
	}

	// And the queued request is still readable from the preserved connection, so
	// the successor server will pick it up after the fd hand-off.
	payload := marshalGetAttrRequest(t, 1)
	buf := make([]byte, len(payload))
	n, err := syscall.Read(srv.MountFd(), buf)
	if err != nil {
		t.Fatalf("read queued late request from preserved mount fd: %v", err)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Fatalf("queued late request mismatch: got %d bytes, want %d", n, len(payload))
	}
}
