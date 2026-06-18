// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import "context"

// DrainInflight quiesces the server for a graceful FUSE fd hand-off. It is
// airtight: when it returns nil, no request will be read from the connection
// again and every request already read has been fully replied. Any request the
// kernel still has queued is left untouched so the successor server picks it up
// after the fd is handed off.
//
// It works by (1) setting the draining flag so readers stop looping back to
// read, (2) waking any reader parked in poll (see waitForRequest) so the read
// loop can exit instead of staying blocked on the device, (3) waiting for all
// read loops to exit — after which no further request can be dequeued — and
// (4) waiting for the requests that were already dispatched to finish.
func (ms *Server) DrainInflight(ctx context.Context) error {
	ms.draining.Store(true)
	ms.signalDrainWake()

	done := make(chan struct{})
	go func() {
		// With a drain wake (Linux), readers parked in poll have been woken, so
		// once every read loop exits the in-flight count can no longer grow
		// (requests are counted before dispatch, inside the loop) and the
		// subsequent Wait drains a settled count -- airtight. Where no wake
		// exists (non-Linux, which has no fd hand-off) a reader may be parked in
		// a bare device read, so skip the loop join to avoid blocking on it.
		if ms.drainWakeFd >= 0 {
			ms.loops.Wait()
		}
		ms.inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
