// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"golang.org/x/sys/unix"
)

// Airtight drain depends on never being parked in an uninterruptible blocking
// read on the FUSE connection. Instead of reading the device directly, each
// reader first waits with poll(2) on both the FUSE fd and an eventfd. To drain,
// DrainInflight makes the eventfd readable; every reader parked in poll wakes,
// observes draining, and exits without consuming a request, so any request the
// kernel still has queued is preserved for the successor server after hand-off.

// setupDrainWake creates the eventfd used to wake parked readers. It must be
// called before any reader runs (i.e. before handleInit / Serve).
func (ms *Server) setupDrainWake() error {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		ms.drainWakeFd = -1
		return err
	}
	ms.drainWakeFd = fd
	return nil
}

// closeDrainWake releases the eventfd. The eventfd is server-internal and is
// never part of the fd hand-off. It is called exactly once, from Serve's defer
// (Serve panics on re-entry), and does not mutate drainWakeFd: the field is
// written once in setupDrainWake (before any reader/Serve goroutine starts) and
// is read-only thereafter, so concurrent readers (waitForRequest, DrainInflight)
// never race a write.
func (ms *Server) closeDrainWake() {
	if ms.drainWakeFd >= 0 {
		unix.Close(ms.drainWakeFd)
	}
}

// signalDrainWake makes the eventfd readable. The counter is never read back,
// so the eventfd stays readable (level-triggered): every current and future
// poll observes it and exits. Safe to call more than once.
func (ms *Server) signalDrainWake() {
	if ms.drainWakeFd < 0 {
		return
	}
	val := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	for {
		_, err := unix.Write(ms.drainWakeFd, val)
		if err == unix.EINTR {
			continue
		}
		return
	}
}

// waitForRequest blocks until the FUSE connection has a request to read or the
// drain wake is signalled. It returns woken=true only for the drain wake; the
// caller must then NOT read, leaving any queued request in the kernel for the
// successor server.
func (ms *Server) waitForRequest() (woken bool, err error) {
	if ms.drainWakeFd < 0 {
		return false, nil
	}
	if ms.draining.Load() {
		return true, nil
	}
	fds := []unix.PollFd{
		{Fd: int32(ms.mountFd), Events: unix.POLLIN},
		{Fd: int32(ms.drainWakeFd), Events: unix.POLLIN},
	}
	for {
		fds[0].Revents = 0
		fds[1].Revents = 0
		_, perr := unix.Poll(fds, -1)
		if perr == unix.EINTR {
			continue
		}
		if perr != nil {
			return false, perr
		}
		if fds[1].Revents != 0 {
			return true, nil
		}
		// FUSE fd readable, hung up, or errored: proceed to read so the existing
		// read path delivers the request or surfaces the error.
		return false, nil
	}
}
