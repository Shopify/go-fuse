// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

type drainSlowFS struct {
	RawFileSystem

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newDrainSlowFS() *drainSlowFS {
	return &drainSlowFS{
		RawFileSystem: NewDefaultRawFileSystem(),
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
}

func (fs *drainSlowFS) GetAttr(cancel <-chan struct{}, input *GetAttrIn, out *AttrOut) Status {
	fs.once.Do(func() { close(fs.entered) })
	select {
	case <-fs.release:
		return OK
	case <-cancel:
		return EINTR
	}
}

func TestDrainInflightWaitsForInflight(t *testing.T) {
	fs := newDrainSlowFS()
	srv, peerFd := newDrainTestServer(t, fs)

	writeGetAttrRequest(t, peerFd, 1)
	waitForDrainTestSignal(t, fs.entered, "GetAttr to enter")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	drainDone := make(chan error, 1)
	go func() {
		drainDone <- srv.DrainInflight(ctx)
	}()

	assertDrainDoesNotReturn(t, drainDone)
	if !srv.draining.Load() {
		t.Fatalf("DrainInflight did not set draining flag")
	}

	close(fs.release)
	if err := waitForDrainResult(t, drainDone); err != nil {
		t.Fatalf("DrainInflight returned %v, want nil", err)
	}

	if _, err := syscall.Write(srv.MountFd(), []byte{0}); err == syscall.EBADF {
		t.Fatalf("DrainInflight closed mount fd")
	} else if err != nil {
		t.Fatalf("write to mount fd after DrainInflight: %v", err)
	}
}

func TestDrainInflightReturnsContextError(t *testing.T) {
	fs := newDrainSlowFS()
	srv, peerFd := newDrainTestServer(t, fs)

	writeGetAttrRequest(t, peerFd, 1)
	waitForDrainTestSignal(t, fs.entered, "GetAttr to enter")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := srv.DrainInflight(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DrainInflight returned %v, want %v", err, context.DeadlineExceeded)
	}
	if !srv.draining.Load() {
		t.Fatalf("DrainInflight did not set draining flag")
	}

	close(fs.release)
}

func newDrainTestServer(t *testing.T, fs RawFileSystem) (*Server, int) {
	t.Helper()

	local, remote, err := unixgramSocketpair()
	if err != nil {
		t.Fatal(err)
	}
	fd := int(local.Fd())
	peerFd := int(remote.Fd())

	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})

	opts := &MountOptions{Logger: log.New(io.Discard, "", 0)}
	srv, err := NewServerOnInitedFd(fs, fd, opts, &InitIn{Minor: 13}, 0)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()

	return srv, peerFd
}

func writeGetAttrRequest(t *testing.T, fd int, unique uint64) {
	t.Helper()

	payload := marshalGetAttrRequest(t, unique)
	n, err := syscall.Write(fd, payload)
	if err != nil {
		t.Fatalf("write GETATTR request: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("write GETATTR request wrote %d bytes, want %d", n, len(payload))
	}
}

func marshalGetAttrRequest(t *testing.T, unique uint64) []byte {
	t.Helper()

	req := GetAttrIn{
		InHeader: InHeader{
			Length: uint32(unsafe.Sizeof(GetAttrIn{})),
			Opcode: _OP_GETATTR,
			Unique: unique,
			NodeId: FUSE_ROOT_ID,
		},
	}
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, req); err != nil {
		t.Fatalf("marshal GETATTR request: %v", err)
	}
	return buf.Bytes()
}

func assertDrainDoesNotReturn(t *testing.T, drainDone <-chan error) {
	t.Helper()

	select {
	case err := <-drainDone:
		t.Fatalf("DrainInflight returned before in-flight request completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitForDrainResult(t *testing.T, drainDone <-chan error) error {
	t.Helper()

	select {
	case err := <-drainDone:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DrainInflight")
		return nil
	}
}

func waitForDrainTestSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
