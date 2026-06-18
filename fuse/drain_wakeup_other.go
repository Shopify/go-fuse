// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !linux

package fuse

// Non-Linux platforms do not support the FUSE fd hand-off / takeover path, so
// the airtight drain wakeup is a no-op. Readers fall through to the existing
// blocking read; these platforms already use a single reader (see useSingleReader)
// to avoid the unmount-hang hazard that the wakeup addresses on Linux.

func (ms *Server) setupDrainWake() error {
	ms.drainWakeFd = -1
	return nil
}

func (ms *Server) closeDrainWake() {}

func (ms *Server) signalDrainWake() {}

func (ms *Server) waitForRequest() (woken bool, err error) {
	return false, nil
}
