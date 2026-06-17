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

type drainForgetFS struct {
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

func newDrainForgetFS() *drainForgetFS {
	return &drainForgetFS{
		RawFileSystem: NewDefaultRawFileSystem(),
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
}

func (fs *drainForgetFS) Forget(nodeid, nlookup uint64) {
	fs.once.Do(func() { close(fs.entered) })
	<-fs.release
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

func TestDrainInflightStopsServeLoopAndPreservesMountFd(t *testing.T) {
	fs := newDrainForgetFS()
	srv, peerFd, serveDone := newDrainTestServerWithDoneConfig(t, fs, func(srv *Server) {
		// Force inline request handling so this test exercises the loop that just
		// dispatched the draining request rather than a platform default reader mode.
		srv.singleReader = false
	})

	writeForgetRequest(t, peerFd, 1)
	waitForDrainTestSignal(t, fs.entered, "Forget to enter")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	drainDone := make(chan error, 1)
	go func() {
		drainDone <- srv.DrainInflight(ctx)
	}()

	assertDrainDoesNotReturn(t, drainDone)

	close(fs.release)
	if err := waitForDrainResult(t, drainDone); err != nil {
		t.Fatalf("DrainInflight returned %v, want nil", err)
	}
	waitForServeDone(t, serveDone)

	var st syscall.Stat_t
	if err := syscall.Fstat(srv.MountFd(), &st); err != nil {
		t.Fatalf("mount fd was closed after drained Serve returned: %v", err)
	}

	payload := marshalForgetRequest(t, 2)
	writeRequest(t, peerFd, payload, "second FORGET request")
	buf := make([]byte, len(payload))
	n, err := syscall.Read(srv.MountFd(), buf)
	if err != nil {
		t.Fatalf("read queued request from preserved mount fd: %v", err)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Fatalf("queued request mismatch: got %v, want %v", buf[:n], payload)
	}
}

func newDrainTestServer(t *testing.T, fs RawFileSystem) (*Server, int) {
	t.Helper()

	srv, peerFd, _ := newDrainTestServerWithDone(t, fs)
	return srv, peerFd
}

func newDrainTestServerWithDone(t *testing.T, fs RawFileSystem) (*Server, int, <-chan struct{}) {
	t.Helper()

	return newDrainTestServerWithDoneConfig(t, fs, nil)
}

func newDrainTestServerWithDoneConfig(t *testing.T, fs RawFileSystem, configure func(*Server)) (*Server, int, <-chan struct{}) {
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
	if configure != nil {
		configure(srv)
	}
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		srv.Serve()
	}()

	return srv, peerFd, serveDone
}

func writeGetAttrRequest(t *testing.T, fd int, unique uint64) {
	t.Helper()

	writeRequest(t, fd, marshalGetAttrRequest(t, unique), "GETATTR request")
}

func writeForgetRequest(t *testing.T, fd int, unique uint64) {
	t.Helper()

	writeRequest(t, fd, marshalForgetRequest(t, unique), "FORGET request")
}

func writeRequest(t *testing.T, fd int, payload []byte, name string) {
	t.Helper()

	n, err := syscall.Write(fd, payload)
	if err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	if n != len(payload) {
		t.Fatalf("write %s wrote %d bytes, want %d", name, n, len(payload))
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
	return marshalDrainRequest(t, req, "GETATTR request")
}

func marshalForgetRequest(t *testing.T, unique uint64) []byte {
	t.Helper()

	req := ForgetIn{
		InHeader: InHeader{
			Length: uint32(unsafe.Sizeof(ForgetIn{})),
			Opcode: _OP_FORGET,
			Unique: unique,
			NodeId: FUSE_ROOT_ID,
		},
		Nlookup: 1,
	}
	return marshalDrainRequest(t, req, "FORGET request")
}

func marshalDrainRequest(t *testing.T, req interface{}, name string) []byte {
	t.Helper()

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, req); err != nil {
		t.Fatalf("marshal %s: %v", name, err)
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

func waitForServeDone(t *testing.T, serveDone <-chan struct{}) {
	t.Helper()

	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Serve to return after drain")
	}
}
